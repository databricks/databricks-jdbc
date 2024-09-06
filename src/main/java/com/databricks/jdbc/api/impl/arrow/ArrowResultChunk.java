package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.IS_FAKE_SERVICE_TEST_PROP;
import static com.databricks.jdbc.common.util.DatabricksThriftUtil.createExternalLink;
import static com.databricks.jdbc.common.util.ValidationUtil.checkHTTPError;

import com.databricks.jdbc.common.CompressionType;
import com.databricks.jdbc.common.LogLevel;
import com.databricks.jdbc.common.util.DecompressionUtil;
import com.databricks.jdbc.common.util.LoggingUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.client.thrift.generated.TSparkArrowResultLink;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.sdk.service.sql.BaseChunkInfo;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.channels.ClosedByInterruptException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;

public class ArrowResultChunk {

  /**
   * The status of a chunk would proceed in following path:
   *
   * <ul>
   *   <li>Create placeholder for chunk, along with the chunk cardinal
   *   <li>Fetch chunk url
   *   <li>Submit task for data download
   *       <ul>
   *         <li>Download has completed
   *         <li>Download has failed and we will retry
   *         <li>Download has failed and we gave up
   *       </ul>
   *   <li>Data has been consumed and chunk is free to be released from memory
   * </ul>
   */
  enum ChunkStatus {
    /** Default status, though for the ArrowChunk, it should be initialized with Pending state */
    UNKNOWN,
    /** This is a placeholder for chunk, we don't even have the Url */
    PENDING,
    /** We have the Url for the chunk, and it is ready for download */
    URL_FETCHED,
    /** Download task has been submitted */
    DOWNLOAD_IN_PROGRESS,
    /** Data has been downloaded and ready for consumption */
    DOWNLOAD_SUCCEEDED,
    /** Result Chunk was of type inline arrow and extract is successful */
    EXTRACT_SUCCEEDED,
    /** Download has failed and it would be retried */
    DOWNLOAD_FAILED,
    /** Result Chunk was of type inline arrow and extract has failed */
    EXTRACT_FAILED,
    /** Download has failed and we have given up */
    DOWNLOAD_FAILED_ABORTED,
    /** Download has been cancelled */
    CANCELLED,
    /** Chunk memory has been consumed and released */
    CHUNK_RELEASED
  }

  private static final Integer SECONDS_BUFFER_FOR_EXPIRY = 60;
  final long numRows;
  long rowOffset;
  List<List<ValueVector>> recordBatchList;
  private final long chunkIndex;
  private ExternalLink chunkLink;
  private final String statementId;
  private Instant expiryTime;
  private ChunkStatus status;
  private final BufferAllocator rootAllocator;
  private String errorMessage;
  private boolean isDataInitialized;
  private final CompressionType compressionType;

  private ArrowResultChunk(Builder builder) throws DatabricksParsingException {
    this.chunkIndex = builder.chunkIndex;
    this.numRows = builder.numRows;
    this.rowOffset = builder.rowOffset;
    this.chunkLink = builder.chunkLink;
    this.statementId = builder.statementId;
    this.expiryTime = builder.expiryTime;
    this.status = builder.status;
    this.compressionType = builder.compressionType;
    this.rootAllocator = new RootAllocator(/* limit= */ Integer.MAX_VALUE);
    if (builder.inputStream != null) {
      // Data is already available
      try {
        initializeData(builder.inputStream);
        this.status = ChunkStatus.EXTRACT_SUCCEEDED;
      } catch (DatabricksSQLException | IOException e) {
        handleFailure(e, ChunkStatus.EXTRACT_FAILED);
      }
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class ArrowResultChunkIterator {
    private final ArrowResultChunk resultChunk;

    // total number of record batches in the chunk
    private final int recordBatchesInChunk;

    // index of record batch in chunk
    private int recordBatchCursorInChunk;

    // total number of rows in record batch under consideration
    private int rowsInRecordBatch;

    // current row index in current record batch
    private int rowCursorInRecordBatch;

    // total number of rows read
    private int rowsReadByIterator;

    ArrowResultChunkIterator(ArrowResultChunk resultChunk) {
      this.resultChunk = resultChunk;
      this.recordBatchesInChunk = resultChunk.getRecordBatchCountInChunk();
      // start before first batch
      this.recordBatchCursorInChunk = -1;
      // initialize to -1
      this.rowsInRecordBatch = -1;
      // start before first row
      this.rowCursorInRecordBatch = -1;
      // initialize rows read to 0
      this.rowsReadByIterator = 0;
    }

    /**
     * Moves iterator to the next row of the chunk. Returns false if it is at the last row in the
     * chunk.
     */
    boolean nextRow() {
      if (!hasNextRow()) {
        return false;
      }
      // Either not initialized or crossed record batch boundary
      if (rowsInRecordBatch < 0 || ++rowCursorInRecordBatch == rowsInRecordBatch) {
        // reset rowCursor to 0
        rowCursorInRecordBatch = 0;
        // Fetches number of rows in the record batch using the number of values in the first column
        // vector
        recordBatchCursorInChunk++;
        while (recordBatchCursorInChunk < recordBatchesInChunk
            && resultChunk.recordBatchList.get(recordBatchCursorInChunk).get(0).getValueCount()
                == 0) {
          recordBatchCursorInChunk++;
        }
        rowsInRecordBatch =
            resultChunk.recordBatchList.get(recordBatchCursorInChunk).get(0).getValueCount();
      }
      rowsReadByIterator++;
      return true;
    }

    /** Returns whether the next row in the chunk exists. */
    boolean hasNextRow() {
      if (rowsReadByIterator >= resultChunk.numRows) return false;
      // If there are more rows in record batch
      return (rowCursorInRecordBatch < rowsInRecordBatch - 1)
          // or there are more record batches to be processed
          || (recordBatchCursorInChunk < recordBatchesInChunk - 1);
    }

    /** Returns object in the current row at the specified columnIndex. */
    Object getColumnObjectAtCurrentRow(int columnIndex) {
      return this.resultChunk
          .getColumnVector(this.recordBatchCursorInChunk, columnIndex)
          .getObject(this.rowCursorInRecordBatch);
    }
  }

  @VisibleForTesting
  void setIsDataInitialized(boolean isDataInitialized) {
    this.isDataInitialized = isDataInitialized;
  }

  /** Sets link details for the given chunk. */
  void setChunkLink(ExternalLink chunk) {
    this.chunkLink = chunk;
    this.expiryTime = Instant.parse(chunk.getExpiration());
    this.status = ChunkStatus.URL_FETCHED;
  }

  /** Updates status for the chunk */
  void setStatus(ChunkStatus status) {
    this.status = status;
  }

  /** Checks if the link is valid */
  boolean isChunkLinkInvalid() {
    return status == ChunkStatus.PENDING
        || (!Boolean.parseBoolean(System.getProperty(IS_FAKE_SERVICE_TEST_PROP))
            && expiryTime.minusSeconds(SECONDS_BUFFER_FOR_EXPIRY).isBefore(Instant.now()));
  }

  /** Returns the status for the chunk */
  ChunkStatus getStatus() {
    return this.status;
  }

  void addHeaders(HttpGet getRequest, Map<String, String> headers) {
    if (headers != null) {
      headers.forEach(getRequest::addHeader);
    } else {
      LoggingUtil.log(
          LogLevel.DEBUG,
          String.format(
              "No encryption headers present for chunk index [%s] and statement [%s]",
              chunkIndex, statementId));
    }
  }

  String getErrorMessage() {
    return this.errorMessage;
  }

  void downloadData(IDatabricksHttpClient httpClient)
      throws DatabricksParsingException, IOException {
    CloseableHttpResponse response = null;
    try {
      URIBuilder uriBuilder = new URIBuilder(chunkLink.getExternalLink());
      HttpGet getRequest = new HttpGet(uriBuilder.build());
      addHeaders(getRequest, chunkLink.getHttpHeaders());
      // Retry would be done in http client, we should not bother about that here
      response = httpClient.execute(getRequest);
      checkHTTPError(response);
      HttpEntity entity = response.getEntity();
      initializeData(entity.getContent());
      setStatus(ChunkStatus.DOWNLOAD_SUCCEEDED);
    } catch (IOException | DatabricksSQLException | URISyntaxException e) {
      handleFailure(e, ChunkStatus.DOWNLOAD_FAILED);
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }

  /**
   * Decompresses the given {@link InputStream} and initializes {@link #recordBatchList} from
   * decompressed stream.
   *
   * @param inputStream the input stream to decompress
   * @throws DatabricksSQLException if decompression fails
   * @throws IOException if reading from the stream fails
   */
  void initializeData(InputStream inputStream) throws DatabricksSQLException, IOException {
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format(
            "Parsing data for chunk index [%s] and statement [%s]",
            this.chunkIndex, this.statementId));
    InputStream decompressedStream =
        DecompressionUtil.decompress(
            inputStream,
            this.compressionType,
            String.format(
                "Data fetch for chunk index [%d] and statement [%s] with decompression algorithm : [%s]",
                this.chunkIndex, this.statementId, this.compressionType));
    this.recordBatchList =
        getRecordBatchList(
            decompressedStream, this.rootAllocator, this.statementId, this.chunkIndex);
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format(
            "Data parsed for chunk index [%s] and statement [%s]",
            this.chunkIndex, this.statementId));
    this.isDataInitialized = true;
  }

  void handleFailure(Exception exception, ChunkStatus failedStatus)
      throws DatabricksParsingException {
    this.errorMessage =
        String.format(
            "Data parsing failed for chunk index [%d] and statement [%s]. Exception [%s]",
            this.chunkIndex, this.statementId, exception);
    LoggingUtil.log(LogLevel.ERROR, this.errorMessage);
    setStatus(failedStatus);
    throw new DatabricksParsingException(this.errorMessage, exception);
  }

  /**
   * Releases chunk from memory
   *
   * @return true if chunk is released, false if it was already released
   */
  synchronized boolean releaseChunk() {
    if (status == ChunkStatus.CHUNK_RELEASED) {
      return false;
    }
    if (isDataInitialized) {
      purgeArrowData(this.recordBatchList);
      rootAllocator.close();
    }
    setStatus(ChunkStatus.CHUNK_RELEASED);
    return true;
  }

  /** Returns number of recordBatches in the chunk. */
  int getRecordBatchCountInChunk() {
    return this.isDataInitialized ? this.recordBatchList.size() : 0;
  }

  ArrowResultChunkIterator getChunkIterator() {
    return new ArrowResultChunkIterator(this);
  }

  /** Returns the chunk download link */
  String getChunkUrl() {
    return chunkLink.getExternalLink();
  }

  /** Returns index for current chunk */
  Long getChunkIndex() {
    return this.chunkIndex;
  }

  private ValueVector getColumnVector(int recordBatchIndex, int columnIndex) {
    return this.recordBatchList.get(recordBatchIndex).get(columnIndex);
  }

  private static List<List<ValueVector>> getRecordBatchList(
      InputStream inputStream, BufferAllocator rootAllocator, String statementId, long chunkIndex)
      throws IOException {
    List<List<ValueVector>> recordBatchList = new ArrayList<>();
    try (ArrowStreamReader arrowStreamReader = new ArrowStreamReader(inputStream, rootAllocator)) {
      VectorSchemaRoot vectorSchemaRoot = arrowStreamReader.getVectorSchemaRoot();
      while (arrowStreamReader.loadNextBatch()) {
        recordBatchList.add(getVectorsFromSchemaRoot(vectorSchemaRoot, rootAllocator));
        vectorSchemaRoot.clear();
      }
    } catch (ClosedByInterruptException e) {
      // release resources if thread is interrupted when reading arrow data
      LoggingUtil.log(
          LogLevel.ERROR,
          String.format(
              "Data parsing interrupted for chunk index [%s] and statement [%s]. Error [%s]",
              chunkIndex, statementId, e));
      purgeArrowData(recordBatchList);
    }

    return recordBatchList;
  }

  private static List<ValueVector> getVectorsFromSchemaRoot(
      VectorSchemaRoot vectorSchemaRoot, BufferAllocator rootAllocator) {
    return vectorSchemaRoot.getFieldVectors().stream()
        .map(
            fieldVector -> {
              TransferPair transferPair = fieldVector.getTransferPair(rootAllocator);
              transferPair.transfer();
              return transferPair.getTo();
            })
        .collect(Collectors.toList());
  }

  private static void purgeArrowData(List<List<ValueVector>> recordBatchList) {
    recordBatchList.forEach(vectors -> vectors.forEach(ValueVector::close));
    recordBatchList.clear();
  }

  public static class Builder {
    private long chunkIndex;
    private long numRows;
    private long rowOffset;
    private ExternalLink chunkLink;
    private String statementId;
    private Instant expiryTime;
    private ChunkStatus status;
    private CompressionType compressionType;
    private InputStream inputStream;

    public Builder statementId(String statementId) {
      this.statementId = statementId;
      return this;
    }

    public Builder compressionType(CompressionType compressionType) {
      this.compressionType = compressionType;
      return this;
    }

    public Builder withChunkInfo(BaseChunkInfo baseChunkInfo) {
      this.chunkIndex = baseChunkInfo.getChunkIndex();
      this.numRows = baseChunkInfo.getRowCount();
      this.rowOffset = baseChunkInfo.getRowOffset();
      this.status = ChunkStatus.PENDING;
      return this;
    }

    public Builder withInputStream(InputStream stream, long rowCount) {
      this.numRows = rowCount;
      this.inputStream = stream;
      this.status = ChunkStatus.PENDING;
      return this;
    }

    public Builder withThriftChunkInfo(long chunkIndex, TSparkArrowResultLink chunkInfo) {
      this.chunkIndex = chunkIndex;
      this.numRows = chunkInfo.getRowCount();
      this.rowOffset = chunkInfo.getStartRowOffset();
      this.expiryTime = Instant.ofEpochMilli(chunkInfo.getExpiryTime());
      this.status = ChunkStatus.URL_FETCHED; // URL has always been fetched in case of thrift
      this.chunkLink = createExternalLink(chunkInfo, chunkIndex);
      return this;
    }

    public ArrowResultChunk build() throws DatabricksParsingException {
      return new ArrowResultChunk(this);
    }
  }
}
