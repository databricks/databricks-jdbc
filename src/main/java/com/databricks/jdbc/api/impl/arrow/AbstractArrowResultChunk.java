package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.ARROW_METADATA_KEY;

import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.common.util.DriverUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.core.ExternalLink;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.ClosedByInterruptException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.commons.lang3.exception.ExceptionUtils;

/**
 * An abstract class that represents a chunk of query result.
 *
 * <p>This class provides methods for downloading, processing, and releasing the data in the chunk.
 * It also manages the state of the chunk and provides access to the data as Arrow record batches.
 */
public abstract class AbstractArrowResultChunk {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(AbstractArrowResultChunk.class);

  protected static final Integer SECONDS_BUFFER_FOR_EXPIRY = 60;
  protected final long numRows;
  protected final long rowOffset;
  protected final long chunkIndex;
  protected final StatementId statementId;
  protected final BufferAllocator rootAllocator;

  /**
   * Future to track when the chunk becomes ready for consumption. This includes both the download
   * and processing phases. The state of the Future is updated by the {@link ChunkDownloadTask} and
   * indicates when the chunk's data is fully processed and available for use.
   */
  protected final CompletableFuture<Void> chunkReadyFuture;

  protected final ArrowResultChunkStateMachine stateMachine;
  protected List<List<ValueVector>> recordBatchList;
  protected ExternalLink chunkLink;
  protected Instant expiryTime;
  protected String errorMessage;
  protected List<String> arrowMetadata;
  protected int chunkReadyTimeoutSeconds;

  static final class ArrowData {
    private final List<List<ValueVector>> valueVectors;
    private final List<String> metadata;
    private final long rowCount;

    public ArrowData(List<List<ValueVector>> valueVectors, List<String> metadata, long rowCount) {
      this.valueVectors = valueVectors;
      this.metadata = metadata;
      this.rowCount = rowCount;
    }

    public List<List<ValueVector>> getValueVectors() {
      return valueVectors;
    }

    public List<String> getMetadata() {
      return metadata;
    }

    public long getRowCount() {
      return rowCount;
    }
  }

  protected AbstractArrowResultChunk(
      long numRows,
      long rowOffset,
      long chunkIndex,
      StatementId statementId,
      ChunkStatus initialStatus,
      ExternalLink chunkLink,
      Instant expiryTime,
      int chunkReadyTimeoutSeconds) {
    this.numRows = numRows;
    this.rowOffset = rowOffset;
    this.chunkIndex = chunkIndex;
    this.statementId = statementId;
    this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
    this.chunkReadyFuture = new CompletableFuture<>();
    this.chunkLink = chunkLink;
    this.expiryTime = expiryTime;
    this.stateMachine = new ArrowResultChunkStateMachine(initialStatus, chunkIndex, statementId);
    this.chunkReadyTimeoutSeconds = chunkReadyTimeoutSeconds;
  }

  /**
   * Returns the index of this chunk.
   *
   * @return chunk index
   */
  public Long getChunkIndex() {
    return chunkIndex;
  }

  /**
   * Checks if the chunk link is invalid or expired.
   *
   * @return true if link is invalid, false otherwise
   */
  public boolean isChunkLinkInvalid() {
    return getStatus() == ChunkStatus.PENDING
        || (!DriverUtil.isRunningAgainstFake()
            && expiryTime.minusSeconds(SECONDS_BUFFER_FOR_EXPIRY).isBefore(Instant.now()));
  }

  /**
   * Releases all resources associated with this chunk.
   *
   * @return true if chunk was released, false if it was already released
   */
  public boolean releaseChunk() {
    if (getStatus() == ChunkStatus.CHUNK_RELEASED) {
      return false;
    }

    if (getStatus() == ChunkStatus.PROCESSING_SUCCEEDED) {
      logAllocatorStats("BeforeRelease");
      purgeArrowData(this.recordBatchList);
      rootAllocator.close();
    }
    setStatus(ChunkStatus.CHUNK_RELEASED);

    return true;
  }

  /**
   * Sets the external link details for this chunk.
   *
   * @param chunk the external link information
   */
  public void setChunkLink(ExternalLink chunk) {
    chunkLink = chunk;
    expiryTime = Instant.parse(chunk.getExpiration());
    setStatus(ChunkStatus.URL_FETCHED);
  }

  /**
   * Returns the current status of the chunk.
   *
   * @return current ChunkStatus
   */
  public ChunkStatus getStatus() {
    return stateMachine.getCurrentStatus();
  }

  /**
   * Downloads and initializes data for this chunk using the provided HTTP client and compression
   * codec.
   *
   * @param httpClient the HTTP client to use for downloading
   * @param compressionCodec the compression codec to use for decompression
   * @param speedThreshold the minimum expected download speed in MB/s for logging warnings
   * @throws DatabricksParsingException if there is an error parsing the data
   * @throws IOException if there is an error downloading or reading the data
   */
  protected abstract void downloadData(
      IDatabricksHttpClient httpClient, CompressionCodec compressionCodec, double speedThreshold)
      throws DatabricksParsingException, IOException;

  /** Handles a failure during the download or processing of this chunk. */
  protected abstract void handleFailure(Exception exception, ChunkStatus failedStatus)
      throws DatabricksParsingException;

  /**
   * Returns the number of record batches in the chunk.
   *
   * @return number of record batches
   */
  protected int getRecordBatchCountInChunk() {
    return getStatus() == ChunkStatus.PROCESSING_SUCCEEDED ? recordBatchList.size() : 0;
  }

  /**
   * Returns the list of record batches, where each record batch is a list of value vectors.
   *
   * @return List of record batches
   */
  protected List<List<ValueVector>> getRecordBatchList() {
    return recordBatchList;
  }

  /**
   * Returns the total number of rows in the chunk.
   *
   * @return number of rows
   */
  protected long getNumRows() {
    return numRows;
  }

  /**
   * Returns the value vector for a specific record batch and column.
   *
   * @param recordBatchIndex index of the record batch
   * @param columnIndex index of the column
   * @return ValueVector for the specified position
   */
  protected ValueVector getColumnVector(int recordBatchIndex, int columnIndex) {
    return recordBatchList.get(recordBatchIndex).get(columnIndex);
  }

  /**
   * Updates the status of the chunk.
   *
   * @param targetStatus new status to set
   */
  protected void setStatus(ChunkStatus targetStatus) {
    try {
      stateMachine.transition(targetStatus);
    } catch (DatabricksParsingException e) {
      LOGGER.warn(
          "Failed to transition to state [{}] from state [{}] for chunk [{}] and statement [{}]. Stack trace: {}",
          targetStatus,
          getStatus(),
          chunkIndex,
          statementId,
          ExceptionUtils.getStackTrace(e));
    }
  }

  /**
   * Returns an iterator for traversing the rows in this chunk.
   *
   * @return ArrowResultChunkIterator for this chunk
   */
  protected ArrowResultChunkIterator getChunkIterator() {
    return new ArrowResultChunkIterator(this);
  }

  protected CompletableFuture<Void> getChunkReadyFuture() {
    return chunkReadyFuture;
  }

  /**
   * Waits for the chunk to be ready for consumption.
   *
   * @throws ExecutionException if the chunk download or processing throws an exception
   * @throws InterruptedException if the thread is interrupted while waiting
   * @throws TimeoutException if the chunk is not ready within the timeout
   */
  protected void waitForChunkReady()
      throws ExecutionException, InterruptedException, TimeoutException {
    try {
      if (chunkReadyTimeoutSeconds <= 0) {
        // Wait indefinitely when timeout is 0 or negative
        chunkReadyFuture.get();
      } else {
        chunkReadyFuture.get(chunkReadyTimeoutSeconds, TimeUnit.SECONDS);
      }

    } catch (InterruptedException e) {
      LOGGER.error(
          e,
          "Chunk download interrupted for chunk index {} and statement {}",
          chunkIndex,
          statementId);
      Thread.currentThread().interrupt();
      throw e;
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
  protected void initializeData(InputStream inputStream)
      throws DatabricksSQLException, IOException {
    LOGGER.debug("Parsing data for chunk index {} and statement {}", chunkIndex, statementId);
    ArrowData arrowData = getRecordBatchList(inputStream, rootAllocator, statementId, chunkIndex);
    recordBatchList = arrowData.getValueVectors();
    arrowMetadata = arrowData.getMetadata();
    LOGGER.debug(
        "Data parsed for chunk index {} and statement {}. Row count: {}",
        chunkIndex,
        statementId,
        arrowData.getRowCount());
    setStatus(ChunkStatus.PROCESSING_SUCCEEDED);
  }

  protected List<String> getArrowMetadata() {
    return arrowMetadata;
  }

  /**
   * Reads Arrow format data from an input stream and converts it into a list of record batches.
   * Each record batch is represented as a list of {@link ValueVector}s.
   */
  private ArrowData getRecordBatchList(
      InputStream inputStream,
      BufferAllocator rootAllocator,
      StatementId statementId,
      long chunkIndex)
      throws IOException {
    List<List<ValueVector>> recordBatchList = new ArrayList<>();
    List<String> metadata = new ArrayList<>();
    long rowCount = 0L;
    try (ArrowStreamReader arrowStreamReader = new ArrowStreamReader(inputStream, rootAllocator)) {
      VectorSchemaRoot vectorSchemaRoot = arrowStreamReader.getVectorSchemaRoot();
      boolean fetchedMetadata = false;
      while (arrowStreamReader.loadNextBatch()) {
        rowCount += vectorSchemaRoot.getRowCount();
        if (!fetchedMetadata) {
          metadata = getMetadataInformationFromSchemaRoot(vectorSchemaRoot);
          fetchedMetadata = true;
        }
        recordBatchList.add(getVectorsFromSchemaRoot(vectorSchemaRoot, rootAllocator));
        vectorSchemaRoot.clear();
      }
    } catch (ClosedByInterruptException e) {
      // release resources if thread is interrupted when reading arrow data
      LOGGER.error(
          e,
          "Data parsing interrupted for chunk index [{}] and statement [{}]. Error [{}]",
          chunkIndex,
          statementId,
          e.getMessage());
      purgeArrowData(recordBatchList);
    } catch (IOException e) {
      LOGGER.error(
          "Error while reading arrow data, purging the local list and rethrowing the exception.");
      purgeArrowData(recordBatchList);
      throw e;
    }

    return new ArrowData(recordBatchList, metadata, rowCount);
  }

  private List<String> getMetadataInformationFromSchemaRoot(VectorSchemaRoot vectorSchemaRoot) {
    return vectorSchemaRoot.getFieldVectors().stream()
        .map(fieldVector -> fieldVector.getField().getMetadata().get(ARROW_METADATA_KEY))
        .collect(Collectors.toList());
  }

  /**
   * Transfers the data from the given {@link VectorSchemaRoot} to a list of {@link ValueVector}s.
   */
  private List<ValueVector> getVectorsFromSchemaRoot(
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

  private void logAllocatorStats(String event) {
    long allocatedMemory = rootAllocator.getAllocatedMemory();
    long peakMemory = rootAllocator.getPeakMemoryAllocation();
    long headRoom = rootAllocator.getHeadroom();
    long initReservation = rootAllocator.getInitReservation();

    LOGGER.debug(
        "Chunk allocator stats Log - Event: {}, Chunk Index: {}, Allocated Memory: {}, Peak Memory: {}, Headroom: {}, Init Reservation: {}",
        event,
        chunkIndex,
        allocatedMemory,
        peakMemory,
        headRoom,
        initReservation);
  }

  /** Releases all Arrow-related resources and clears the record batch list. */
  private void purgeArrowData(List<List<ValueVector>> recordBatchList) {
    recordBatchList.forEach(vectors -> vectors.forEach(ValueVector::close));
    recordBatchList.clear();
  }
}
