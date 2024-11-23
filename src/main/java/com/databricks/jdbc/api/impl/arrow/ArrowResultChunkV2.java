package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.IS_FAKE_SERVICE_TEST_PROP;
import static com.databricks.jdbc.common.util.DatabricksThriftUtil.createExternalLink;

import com.databricks.jdbc.common.util.DecompressionUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.TSparkArrowResultLink;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.sdk.service.sql.BaseChunkInfo;
import com.google.common.annotations.VisibleForTesting;
import java.io.*;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.hc.client5.http.async.methods.*;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.nio.AsyncRequestProducer;
import org.apache.hc.core5.http.nio.support.AsyncRequestBuilder;

public class ArrowResultChunkV2 {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ArrowResultChunkV2.class);

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
    CHUNK_RELEASED,
    DOWNLOAD_RETRY
  }

  enum DownloadPhase {
    LINK_REFRESH("link refresh"),
    DATA_DOWNLOAD("data download"),
    DOWNLOAD_SETUP("download setup");

    private final String description;

    DownloadPhase(String description) {
      this.description = description;
    }

    public String getDescription() {
      return description;
    }
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
  private final CompletableFuture<Void> downloadFuture = new CompletableFuture<>();

  // Add executor service as a static field since it can be shared across chunks
  private static final ExecutorService processingExecutor =
      Executors.newFixedThreadPool(
          Runtime.getRuntime().availableProcessors(),
          new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
              Thread thread =
                  new Thread(r, "Arrow-Processing-Thread-" + threadNumber.getAndIncrement());
              thread.setDaemon(true); // Make threads daemon so they don't prevent JVM shutdown
              return thread;
            }
          });

  private static final ScheduledExecutorService retryScheduler =
      Executors.newScheduledThreadPool(
          Runtime.getRuntime().availableProcessors(),
          new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
              Thread thread =
                  new Thread(r, "Arrow-Retry-Scheduler-" + threadNumber.getAndIncrement());
              thread.setDaemon(true);
              return thread;
            }
          });

  private ArrowResultChunkV2(Builder builder) throws DatabricksParsingException {
    this.chunkIndex = builder.chunkIndex;
    this.numRows = builder.numRows;
    this.rowOffset = builder.rowOffset;
    this.chunkLink = builder.chunkLink;
    this.statementId = builder.statementId;
    this.expiryTime = builder.expiryTime;
    this.status = builder.status;
    this.rootAllocator = new RootAllocator(/* limit= */ Integer.MAX_VALUE);
    if (builder.inputStream != null) {
      // Data is already available
      try {
        initializeData(builder.inputStream);
        this.status = ChunkStatus.EXTRACT_SUCCEEDED;
      } catch (DatabricksSQLException | IOException e) {
        this.errorMessage =
            String.format(
                "Data parsing failed for chunk index [%d] and statement [%s]. Exception [%s]",
                this.chunkIndex, this.statementId, e);
        LOGGER.error(this.errorMessage);
        setStatus(ChunkStatus.EXTRACT_FAILED);
        throw new DatabricksParsingException(this.errorMessage, e);
      }
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  private static class RetryConfig {
    final int maxAttempts;
    final long baseDelayMs;
    final long maxDelayMs;

    private RetryConfig(Builder builder) {
      this.maxAttempts = builder.maxAttempts;
      this.baseDelayMs = builder.baseDelayMs;
      this.maxDelayMs = builder.maxDelayMs;
    }

    static class Builder {
      private int maxAttempts = 3;
      private long baseDelayMs = 1000;
      private long maxDelayMs = 5000;

      Builder maxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
        return this;
      }

      Builder baseDelayMs(long baseDelayMs) {
        this.baseDelayMs = baseDelayMs;
        return this;
      }

      Builder maxDelayMs(long maxDelayMs) {
        this.maxDelayMs = maxDelayMs;
        return this;
      }

      RetryConfig build() {
        return new RetryConfig(this);
      }
    }
  }

  private static class StreamingResponseConsumer extends AbstractBinResponseConsumer<Void> {
    private final PipedOutputStream outputStream;
    private final CompletableFuture<Void> future;

    public StreamingResponseConsumer(
        PipedOutputStream outputStream, CompletableFuture<Void> future) {
      this.outputStream = outputStream;
      this.future = future;
    }

    @Override
    protected void start(org.apache.hc.core5.http.HttpResponse response, ContentType contentType)
        throws HttpException {
      // Verify response status code here if needed
      if (response.getCode() != 200) {
        throw new HttpException("Unexpected response status: " + response.getCode());
      }
    }

    @Override
    protected int capacityIncrement() {
      // Define the size of chunks to process, e.g., 8KB
      return 32 * 1024;
    }

    @Override
    protected void data(ByteBuffer data, boolean endOfStream) throws IOException {
      // Write data as it comes in
      try {
        // Write data as it comes in
        while (data.hasRemaining()) {
          byte[] bytes = new byte[Math.min(data.remaining(), 32 * 1024)];
          data.get(bytes);
          outputStream.write(bytes);
        }

        if (endOfStream) {
          outputStream.close();
        }
      } catch (IOException e) {
        failed(e);
        throw e;
      }
    }

    @Override
    protected Void buildResult() {
      return null;
    }

    @Override
    public void failed(Exception cause) {
      try {
        outputStream.close();
      } catch (IOException e) {
        // Log error
      }
      future.completeExceptionally(cause);
    }

    @Override
    public void releaseResources() {
      try {
        outputStream.close();
      } catch (IOException ignored) {
      }
    }
  }

  public static class ArrowResultChunkIterator {
    private final ArrowResultChunkV2 resultChunk;

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

    ArrowResultChunkIterator(ArrowResultChunkV2 resultChunk) {
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
  synchronized void setStatus(ChunkStatus status) {
    this.status = status;
  }

  /** Checks if the link is valid */
  boolean isChunkLinkInvalid() {
    return status == ChunkStatus.PENDING
        || (!Boolean.parseBoolean(System.getProperty(IS_FAKE_SERVICE_TEST_PROP))
            && expiryTime.minusSeconds(SECONDS_BUFFER_FOR_EXPIRY).isBefore(Instant.now()));
  }

  /** Returns the status for the chunk */
  synchronized ChunkStatus getStatus() {
    return this.status;
  }

  String getErrorMessage() {
    return this.errorMessage;
  }

  void downloadDataAsync(IDatabricksHttpClient httpClient, ChunkDownloadCallback callback) {
    CloseableHttpAsyncClient asyncClient = httpClient.getAsyncClient();
    asyncClient.start();
    RetryConfig retryConfig =
        new RetryConfig.Builder().maxAttempts(3).baseDelayMs(1000).maxDelayMs(5000).build();
    retryDownload(asyncClient, callback, retryConfig, 1);
  }

  private void retryDownload(
      CloseableHttpAsyncClient asyncClient,
      ChunkDownloadCallback callback,
      RetryConfig retryConfig,
      int currentAttempt) {
    try {
      PipedOutputStream outputStream = new PipedOutputStream();
      PipedInputStream inputStream = new PipedInputStream(outputStream, 4 * 1024 * 1024);
      StreamingResponseConsumer consumer =
          new StreamingResponseConsumer(outputStream, downloadFuture);
      processingExecutor.execute(
          () -> {
            // blocking and executed in processing executor
            try {
              String context =
                  String.format(
                      "Data decompression for chunk index %d and statement %s",
                      chunkIndex, statementId);

              InputStream uncompressedStream =
                  DecompressionUtil.decompress(
                      inputStream, callback.getCompressionCodec(), context);

              initializeData(uncompressedStream);
              setStatus(ChunkStatus.DOWNLOAD_SUCCEEDED);
              downloadFuture.complete(null);
            } catch (Exception e) {
              handleStreamingFailure(e);
            }
          });
      CompletableFuture<Void> linkRefreshFuture =
          CompletableFuture.runAsync(
              () -> {
                // blocking and executed in processing executor
                if (isChunkLinkInvalid()) {
                  try {
                    callback.downloadLinks(getChunkIndex());
                  } catch (Exception e) {
                    throw new CompletionException(e);
                  }
                }
              },
              processingExecutor);
      linkRefreshFuture
          .thenRun(
              () -> {
                AsyncRequestBuilder requestBuilder =
                    AsyncRequestBuilder.get(chunkLink.getExternalLink());
                if (chunkLink.getHttpHeaders() != null) {
                  chunkLink.getHttpHeaders().forEach(requestBuilder::addHeader);
                }
                AsyncRequestProducer requestProducer = requestBuilder.build();
                long requestStartTime = System.currentTimeMillis();
                // non-blocking
                asyncClient.execute(
                    requestProducer,
                    consumer,
                    new FutureCallback<>() {
                      @Override
                      public void completed(Void result) {
                        long responseTime = System.currentTimeMillis() - requestStartTime;
                        LOGGER.debug("Response time for chunk {}: {} ms", chunkIndex, responseTime);
                      }

                      @Override
                      public void failed(Exception e) {
                        handleRetryableError(
                            asyncClient,
                            callback,
                            retryConfig,
                            currentAttempt,
                            e,
                            DownloadPhase.DATA_DOWNLOAD);
                      }

                      @Override
                      public void cancelled() {
                        setStatus(ChunkStatus.CANCELLED);
                        downloadFuture.cancel(true);
                      }
                    });
              })
          .exceptionally(
              throwable -> {
                handleRetryableError(
                    asyncClient,
                    callback,
                    retryConfig,
                    currentAttempt,
                    throwable instanceof CompletionException
                        ? (Exception) throwable.getCause()
                        : (Exception) throwable,
                    DownloadPhase.LINK_REFRESH);
                return null;
              });
    } catch (Exception e) {
      handleRetryableError(
          asyncClient, callback, retryConfig, currentAttempt, e, DownloadPhase.DOWNLOAD_SETUP);
    }
  }

  private void handleRetryableError(
      CloseableHttpAsyncClient asyncClient,
      ChunkDownloadCallback callback,
      RetryConfig retryConfig,
      int currentAttempt,
      Exception e,
      DownloadPhase phase) {
    if (currentAttempt < retryConfig.maxAttempts && isRetryableError(e)) {
      long delayMs = calculateBackoffDelay(currentAttempt, retryConfig);
      LOGGER.warn(
          "Retryable error during %s for chunk %s (attempt %s/%s), retrying in %s ms. Error: %s",
          phase.getDescription(),
          chunkIndex,
          currentAttempt,
          retryConfig.maxAttempts,
          delayMs,
          e.getMessage());
      setStatus(ChunkStatus.DOWNLOAD_RETRY);
      retryScheduler.schedule(
          () -> retryDownload(asyncClient, callback, retryConfig, currentAttempt + 1),
          delayMs,
          TimeUnit.MILLISECONDS);
    } else {
      handleStreamingFailure(e);
    }
  }

  private boolean isRetryableError(Exception e) {
    return e instanceof SocketException
        || e instanceof SocketTimeoutException
        || e instanceof DatabricksHttpException
        || (e instanceof IOException && e.getMessage().contains("Connection reset"))
        || (e instanceof DatabricksParsingException && e.getCause() instanceof SocketException);
  }

  private long calculateBackoffDelay(int attempt, RetryConfig retryConfig) {
    // Exponential backoff with jitter
    long delay =
        Math.min(retryConfig.maxDelayMs, retryConfig.baseDelayMs * (long) Math.pow(2, attempt - 1));

    // Add random jitter between 0-100ms
    return delay + ThreadLocalRandom.current().nextLong(100);
  }

  private void handleStreamingFailure(Exception e) {
    String errorMessage =
        String.format(
            "Data parsing failed for chunk index %d and statement %s. Exception %s",
            chunkIndex, statementId, e.getMessage());
    LOGGER.error(errorMessage);
    setStatus(ChunkStatus.DOWNLOAD_FAILED);
    downloadFuture.completeExceptionally(new DatabricksParsingException(errorMessage, e));
  }

  void waitForDownload() throws InterruptedException {
    try {
      downloadFuture.get();
    } catch (Exception e) {
      Thread.currentThread().interrupt();
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
    LOGGER.debug(
        String.format(
            "Parsing data for chunk index [%s] and statement [%s]",
            this.chunkIndex, this.statementId));
    this.recordBatchList =
        getRecordBatchList(inputStream, this.rootAllocator, this.statementId, this.chunkIndex);
    LOGGER.debug(
        String.format(
            "Data parsed for chunk index [%s] and statement [%s]",
            this.chunkIndex, this.statementId));
    this.isDataInitialized = true;
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
      logAllocatorStats("BeforeRelease");
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
      LOGGER.error(
          e,
          "Data parsing interrupted for chunk index [%s] and statement [%s]. Error [%s]",
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

  private void logAllocatorStats(String event) {
    long allocatedMemory = rootAllocator.getAllocatedMemory();
    long peakMemory = rootAllocator.getPeakMemoryAllocation();
    long headRoom = rootAllocator.getHeadroom();
    long initReservation = rootAllocator.getInitReservation();

    String allocatorStatsLog =
        String.format(
            "Chunk allocator stats Log - Event: %s, Chunk Index: %s, Allocated Memory: %s, Peak Memory: %s, Headroom: %s, Init Reservation: %s",
            event, chunkIndex, allocatedMemory, peakMemory, headRoom, initReservation);
    LOGGER.debug(allocatorStatsLog);
  }

  public static class Builder {
    private long chunkIndex;
    private long numRows;
    private long rowOffset;
    private ExternalLink chunkLink;
    private String statementId;
    private Instant expiryTime;
    private ChunkStatus status;
    private InputStream inputStream;

    public Builder statementId(String statementId) {
      this.statementId = statementId;
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

    public ArrowResultChunkV2 build() throws DatabricksParsingException {
      return new ArrowResultChunkV2(this);
    }
  }
}
