package com.databricks.jdbc.core;

import static com.databricks.jdbc.commons.util.ValidationUtil.checkHTTPError;

import com.databricks.jdbc.client.DatabricksHttpException;
import com.databricks.jdbc.client.IDatabricksHttpClient;
import com.databricks.jdbc.client.sqlexec.ExternalLink;
import com.databricks.jdbc.commons.util.DecompressionUtil;
import com.databricks.jdbc.core.types.CompressionType;
import com.databricks.sdk.service.sql.BaseChunkInfo;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.ClosedByInterruptException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
   *
   * ->
   */
  enum ChunkStatus {
    // Default status, though for the ArrowChunk, it should be initialized with Pending state
    UNKNOWN,
    // This is a placeholder for chunk, we don't even have the Url
    PENDING,
    // We have the Url for the chunk, and it is ready for download
    URL_FETCHED,
    // Download task has been submitted
    DOWNLOAD_IN_PROGRESS,
    // Data has been downloaded and ready for consumption
    DOWNLOAD_SUCCEEDED,
    // Result Chunk was of type inline arrow and extract is successful
    EXTRACT_SUCCEEDED,
    // Download has failed and it would be retried
    DOWNLOAD_FAILED,
    // Result Chunk was of type inline arrow and extract has failed
    EXTRACT_FAILED,
    // Download has failed and we have given up
    DOWNLOAD_FAILED_ABORTED,
    // Download has been cancelled
    CANCELLED,
    // Chunk memory has been consumed and released
    CHUNK_RELEASED;
  }

  private static final Integer SECONDS_BUFFER_FOR_EXPIRY = 60;

  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowResultChunk.class);

  private final long chunkIndex;
  final long numRows;
  final long rowOffset;
  final Long byteCount;

  private ExternalLink chunkLink;
  private final String statementId;
  private Long nextChunkIndex;
  private Instant expiryTime;
  private ChunkStatus status;
  private Long downloadStartTime;
  private Long downloadFinishTime;

  public List<List<ValueVector>> recordBatchList;

  private RootAllocator rootAllocator;
  private String errorMessage;

  private boolean isDataInitialized;

  private VectorSchemaRoot vectorSchemaRoot;

  private CompressionType compressionType;

  ArrowResultChunk(BaseChunkInfo chunkInfo, String statementId, CompressionType compressionType) {
    this.chunkIndex = chunkInfo.getChunkIndex();
    this.numRows = chunkInfo.getRowCount();
    this.rowOffset = chunkInfo.getRowOffset();
    this.byteCount = chunkInfo.getByteCount();
    this.status = ChunkStatus.PENDING;
    this.rootAllocator = new RootAllocator(/* limit= */ Integer.MAX_VALUE);
    this.chunkLink = null;
    this.downloadStartTime = null;
    this.downloadFinishTime = null;
    this.statementId = statementId;
    isDataInitialized = false;
    this.errorMessage = null;
    this.vectorSchemaRoot = null;
    this.compressionType = compressionType;
  }

  ArrowResultChunk(
      long rowCount, String statementId, CompressionType compressionType, InputStream stream)
      throws DatabricksParsingException {
    this.chunkIndex = 0L;
    this.numRows = rowCount;
    this.rowOffset = 0L;
    this.byteCount = null; // Inline results don't have byteCount attached to its chunk
    this.status = ChunkStatus.PENDING;
    this.rootAllocator = new RootAllocator(/* limit= */ Integer.MAX_VALUE);
    this.chunkLink = null;
    this.statementId = statementId;
    isDataInitialized = true;
    this.errorMessage = null;
    this.vectorSchemaRoot = null;
    try {
      getArrowDataFromInputStream(stream);
      this.status = ChunkStatus.EXTRACT_SUCCEEDED;
    } catch (Exception e) {
      handleFailure(e, ChunkStatus.EXTRACT_FAILED);
    }
    this.compressionType = compressionType;
  }

  public static class ArrowResultChunkIterator {
    private final ArrowResultChunk resultChunk;

    // total number of record batches in the chunk
    private int recordBatchesInChunk;

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
    public boolean nextRow() {
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
    public boolean hasNextRow() {
      if (rowsReadByIterator >= resultChunk.numRows) return false;
      // If there are more rows in record batch
      return (rowCursorInRecordBatch < rowsInRecordBatch - 1)
          // or there are more record batches to be processed
          || (recordBatchCursorInChunk < recordBatchesInChunk - 1);
    }

    /** Returns object in the current row at the specified columnIndex. */
    public Object getColumnObjectAtCurrentRow(int columnIndex) {
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
    this.nextChunkIndex = chunk.getNextChunkIndex();
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
        || expiryTime.minusSeconds(SECONDS_BUFFER_FOR_EXPIRY).isBefore(Instant.now());
  }

  /** Returns the status for the chunk */
  ChunkStatus getStatus() {
    return this.status;
  }

  void addHeaders(HttpGet getRequest, Map<String, String> headers) {
    if (headers != null) {
      headers.forEach(getRequest::addHeader);
    } else {
      LOGGER.debug(
          "No encryption headers present for chunk index [{}] and statement [{}]",
          chunkIndex,
          statementId);
    }
  }

  public String getErrorMessage() {
    return this.errorMessage;
  }

  void downloadData(IDatabricksHttpClient httpClient)
      throws DatabricksHttpException, DatabricksParsingException, IOException {
    CloseableHttpResponse response = null;
    try {
      this.downloadStartTime = Instant.now().toEpochMilli();
      URIBuilder uriBuilder = new URIBuilder(chunkLink.getExternalLink());
      HttpGet getRequest = new HttpGet(uriBuilder.build());
      addHeaders(getRequest, chunkLink.getHttpHeaders());
      // Retry would be done in http client, we should not bother about that here
      response = httpClient.execute(getRequest);
      checkHTTPError(response);
      HttpEntity entity = response.getEntity();
      getArrowDataFromInputStream(entity.getContent());
      this.downloadFinishTime = Instant.now().toEpochMilli();
      this.setStatus(ChunkStatus.DOWNLOAD_SUCCEEDED);
    } catch (Exception e) {
      handleFailure(e, ChunkStatus.DOWNLOAD_FAILED);
      throw new DatabricksHttpException(errorMessage, e);
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }

  public void getArrowDataFromInputStream(InputStream inputStream) throws DatabricksSQLException {
    LOGGER.debug(
        "Parsing data for chunk index [{}] and statement [{}]", this.chunkIndex, this.statementId);
    InputStream decompressedStream =
        DecompressionUtil.decompress(
            inputStream,
            this.compressionType,
            String.format(
                "Data fetch failed for chunk index [%d] and statement [%s] as decompression was unsuccessful. Algorithm : [%s]",
                this.chunkIndex, this.statementId, this.compressionType));
    this.isDataInitialized = true;
    // add check to see if input stream has been populated
    initializeRecordBatch(decompressedStream);
  }

  private void initializeRecordBatch(InputStream decompressedStream)
      throws DatabricksParsingException {
    this.recordBatchList = new ArrayList<>();
    ArrowStreamReader arrowStreamReader =
        new ArrowStreamReader(decompressedStream, this.rootAllocator);
    List<ValueVector> vectors = new ArrayList<>();
    try {
      this.vectorSchemaRoot = arrowStreamReader.getVectorSchemaRoot();
      while (arrowStreamReader.loadNextBatch()) {
        this.recordBatchList.add(getVectorsFromSchemaRoot());
        vectorSchemaRoot.clear();
      }
      LOGGER.debug(
          "Data parsed for chunk index [{}] and statement [{}]", this.chunkIndex, this.statementId);
    } catch (ClosedByInterruptException e) {
      LOGGER.debug("Data parsing interrupted when loading Arrow Result", e);
      vectors.forEach(ValueVector::close);
      purgeArrowData();
      // no need to throw an exception here, this is expected if statement is closed when loading
      // data
    } catch (Exception e) {
      vectors.forEach(ValueVector::close);
      handleFailure(e, ChunkStatus.DOWNLOAD_FAILED);
    }
  }

  private List<ValueVector> getVectorsFromSchemaRoot() {
    return vectorSchemaRoot.getFieldVectors().stream()
        .map(
            fieldVector -> {
              TransferPair transferPair = fieldVector.getTransferPair(rootAllocator);
              transferPair.transfer();
              return transferPair.getTo();
            })
        .collect(Collectors.toList());
  }

  void handleFailure(Exception exception, ChunkStatus failedStatus)
      throws DatabricksParsingException {
    String errMsg =
        String.format(
            "Data parsing failed for chunk index [%d] and statement [%s]",
            this.chunkIndex, this.statementId);
    LOGGER.error(errMsg, exception);
    this.setStatus(failedStatus);
    purgeArrowData();
    throw new DatabricksParsingException(errMsg, exception);
  }

  void purgeArrowData() {
    this.recordBatchList.forEach(vectors -> vectors.forEach(ValueVector::close));
    this.recordBatchList.clear();
    if (this.vectorSchemaRoot != null) {
      this.vectorSchemaRoot.clear();
      this.vectorSchemaRoot = null;
    }
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
    if (isDataInitialized) this.recordBatchList.clear();
    this.setStatus(ChunkStatus.CHUNK_RELEASED);
    return true;
  }

  /**
   * Returns number of recordBatches in the chunk.
   *
   * @return
   */
  int getRecordBatchCountInChunk() {
    return this.isDataInitialized ? this.recordBatchList.size() : 0;
  }

  public ArrowResultChunkIterator getChunkIterator() {
    return new ArrowResultChunkIterator(this);
  }

  private ValueVector getColumnVector(int recordBatchIndex, int columnIndex) {
    return this.recordBatchList.get(recordBatchIndex).get(columnIndex);
  }

  /** Returns the chunk download link */
  String getChunkUrl() {
    return chunkLink.getExternalLink();
  }

  /** Returns index for current chunk */
  Long getChunkIndex() {
    return this.chunkIndex;
  }

  Long getDownloadFinishTime() {
    return this.downloadFinishTime;
  }
}
