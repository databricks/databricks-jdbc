package com.databricks.jdbc.api.impl.arrow.incubator;

import static com.databricks.jdbc.common.util.DatabricksThriftUtil.createExternalLink;

import com.databricks.jdbc.api.impl.arrow.AbstractArrowResultChunk;
import com.databricks.jdbc.api.impl.arrow.ChunkStatus;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.common.DatabricksJdbcUrlParams;
import com.databricks.jdbc.common.util.DecompressionUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.TSparkArrowResultLink;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.service.sql.BaseChunkInfo;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.nio.AsyncRequestProducer;
import org.apache.hc.core5.http.nio.support.AsyncRequestBuilder;

public class ArrowResultChunkV2 extends AbstractArrowResultChunk {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ArrowResultChunkV2.class);

  /**
   * The number of threads in the executor for processing downloaded Arrow data chunks.
   *
   * <p>TODO: Make this configurable
   */
  private static final int N_THREADS_PROCESSING = 150;

  /**
   * Scheduler dedicated to retry operations for failed chunk downloads. Uses a small thread pool
   * since chunk downloads operate asynchronously and retry operations are lightweight scheduling
   * tasks.
   *
   * <p>Thread pool size is set to the number of available processors to balance resource usage with
   * responsiveness. Each thread is configured as a daemon thread to prevent blocking JVM shutdown.
   *
   * <p>This scheduler handles: - Exponential backoff delays between retry attempts - Scheduling
   * retry operations when network errors or timeouts occur - Managing retry attempts across
   * multiple concurrent chunk downloads
   *
   * <p>The scheduler operates independently of the main arrow data processing executor to ensure
   * retry scheduling is not blocked by data processing operations.
   */
  private static final ScheduledExecutorService retryScheduler =
      Executors.newScheduledThreadPool(
          Runtime.getRuntime().availableProcessors(),
          new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);

            @Override
            public Thread newThread(@Nonnull Runnable r) {
              Thread thread =
                  new Thread(r, "Arrow-Retry-Scheduler-" + threadNumber.getAndIncrement());
              thread.setDaemon(true);
              return thread;
            }
          });

  /**
   * A thread pool executor for processing Arrow data chunks after successful download.
   *
   * <p>Processing operations include: - Decompressing downloaded byte arrays using the specified
   * compression codec - Initializing Arrow data structures from the decompressed input stream
   *
   * <p>Uses a fixed thread pool of 150 threads to handle concurrent chunk processing across
   * multiple active result sets. The large thread pool size accommodates the potentially blocking
   * nature of decompression and Arrow data initialization operations.
   *
   * <p>Each thread is configured as a daemon thread to prevent blocking JVM shutdown. Processing
   * tasks are submitted here after chunks are successfully downloaded to separate I/O operations
   * from CPU-intensive data transformation work.
   */
  private static final ExecutorService arrowDataProcessingExecutor =
      Executors.newFixedThreadPool(
          N_THREADS_PROCESSING,
          new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);

            @Override
            public Thread newThread(@Nonnull Runnable r) {
              Thread thread =
                  new Thread(r, "Arrow-Processing-Thread-" + threadNumber.getAndIncrement());
              thread.setDaemon(true); // Make threads daemon so they don't prevent JVM shutdown
              return thread;
            }
          });

  protected volatile long downloadStartTime;
  protected volatile long downloadEndTime;
  protected volatile long bytesDownloaded;
  protected byte[] downloadedBytes;

  private ArrowResultChunkV2(Builder builder) {
    super(
        builder.numRows,
        builder.rowOffset,
        builder.chunkIndex,
        builder.statementId,
        builder.status,
        builder.chunkLink,
        builder.expiryTime,
        builder.chunkReadyTimeoutSeconds);
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  protected void downloadData(
      IDatabricksHttpClient httpClient, CompressionCodec compressionCodec, double speedThreshold) {
    // TODO: Make this configurable
    RetryConfig retryConfig =
        new RetryConfig.Builder().maxAttempts(3).baseDelayMs(1000).maxDelayMs(5000).build();
    retryDownload(httpClient, compressionCodec, retryConfig, 1);
  }

  @Override
  protected void handleFailure(Exception exception, ChunkStatus failedStatus) {
    errorMessage =
        String.format(
            "Data parsing failed for chunk index [%d] and statement [%s]. Exception [%s]",
            chunkIndex, statementId, exception);
    LOGGER.error(errorMessage);
    setStatus(failedStatus);
    // TODO: set correct error code
    chunkReadyFuture.completeExceptionally(
        new DatabricksParsingException(
            errorMessage, exception, DatabricksDriverErrorCode.CHUNK_DOWNLOAD_ERROR));
  }

  /**
   * Attempts to download a chunk with retry capabilities based on the provided configuration.
   * Implements exponential backoff and handles various types of network and HTTP errors.
   *
   * @param httpClient the HTTP client to use for the download
   * @param compressionCodec the compression codec for decompressing the data
   * @param retryConfig configuration parameters for retry behavior
   * @param currentAttempt the current retry attempt number
   */
  private void retryDownload(
      IDatabricksHttpClient httpClient,
      CompressionCodec compressionCodec,
      RetryConfig retryConfig,
      int currentAttempt) {
    try {
      // Initialize consumer to handle streaming response
      StreamingResponseConsumer consumer = new StreamingResponseConsumer(this);

      // Build HTTP GET request with optional headers
      AsyncRequestBuilder requestBuilder = AsyncRequestBuilder.get(chunkLink.getExternalLink());
      if (chunkLink.getHttpHeaders() != null) {
        chunkLink.getHttpHeaders().forEach(requestBuilder::addHeader);
      }
      AsyncRequestProducer requestProducer = requestBuilder.build();

      // Execute async HTTP request with callback handlers
      httpClient.executeAsync(
          requestProducer,
          consumer,
          new ChunkDownloadCallback(httpClient, compressionCodec, retryConfig, currentAttempt));
    } catch (Exception e) {
      // Handle exceptions during request setup with retry logic
      handleRetryableError(
          httpClient,
          compressionCodec,
          retryConfig,
          currentAttempt,
          e,
          DownloadPhase.DOWNLOAD_SETUP);
    }
  }

  /**
   * Processes the downloaded Arrow data by decompressing and initializing it. After successful
   * processing, clears the downloaded bytes and updates the chunk status.
   *
   * @param compressionCodec the codec to use for decompression
   * @param context descriptive context string for error reporting
   */
  private void processArrowData(CompressionCodec compressionCodec, String context) {
    try (ByteArrayInputStream compressedStream = new ByteArrayInputStream(downloadedBytes);
        InputStream uncompressedStream =
            DecompressionUtil.decompress(compressedStream, compressionCodec, context)) {
      initializeData(uncompressedStream);
      // Clear the downloaded bytes after successful processing
      downloadedBytes = null;
      chunkReadyFuture.complete(null);
    } catch (IOException | DatabricksSQLException e) {
      handleFailure(e, ChunkStatus.PROCESSING_FAILED);
    }
  }

  /**
   * Handles retryable errors during download operations by implementing exponential backoff and
   * scheduling retry attempts when appropriate.
   *
   * @param httpClient the HTTP client to use for retries
   * @param compressionCodec the compression codec for data decompression
   * @param retryConfig configuration parameters for retry behavior
   * @param currentAttempt the current retry attempt number
   * @param e the exception that triggered the retry
   * @param phase the download phase during which the error occurred
   */
  private void handleRetryableError(
      IDatabricksHttpClient httpClient,
      CompressionCodec compressionCodec,
      RetryConfig retryConfig,
      int currentAttempt,
      Exception e,
      DownloadPhase phase) {
    setStatus(ChunkStatus.DOWNLOAD_FAILED);
    LOGGER.info(
        "Retrying, current attempt: "
            + currentAttempt
            + " for chunk "
            + chunkIndex
            + " for download phase "
            + phase.getDescription()
            + " with error: "
            + e);

    // Check if we should retry based on max attempts and error type
    if (currentAttempt < retryConfig.maxAttempts) {
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

      // Schedule retry attempt after calculated delay
      retryScheduler.schedule(
          () -> retryDownload(httpClient, compressionCodec, retryConfig, currentAttempt + 1),
          delayMs,
          TimeUnit.MILLISECONDS);
    } else {
      // If max attempts reached or non-retryable error, mark as failed
      handleFailure(e, ChunkStatus.DOWNLOAD_FAILED);
    }
  }

  /** Calculates the backoff delay for retry attempts using exponential backoff with jitter. */
  private long calculateBackoffDelay(int attempt, RetryConfig retryConfig) {
    // Exponential backoff with jitter
    long delay =
        Math.min(retryConfig.maxDelayMs, retryConfig.baseDelayMs * (long) Math.pow(2, attempt - 1));

    // Add random jitter between 0-100 ms
    return delay + ThreadLocalRandom.current().nextLong(100);
  }

  public static class Builder {
    private long chunkIndex;
    private long numRows;
    private long rowOffset;
    private ExternalLink chunkLink;
    private StatementId statementId;
    private Instant expiryTime;
    private ChunkStatus status;
    private int chunkReadyTimeoutSeconds =
        Integer.parseInt(DatabricksJdbcUrlParams.CHUNK_READY_TIMEOUT_SECONDS.getDefaultValue());

    public Builder withStatementId(StatementId statementId) {
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

    public Builder withThriftChunkInfo(long chunkIndex, TSparkArrowResultLink chunkInfo) {
      this.chunkIndex = chunkIndex;
      this.numRows = chunkInfo.getRowCount();
      this.rowOffset = chunkInfo.getStartRowOffset();
      this.expiryTime = Instant.ofEpochMilli(chunkInfo.getExpiryTime());
      this.status = ChunkStatus.URL_FETCHED; // URL has always been fetched in case of thrift
      this.chunkLink = createExternalLink(chunkInfo, chunkIndex);
      return this;
    }

    public Builder withChunkReadyTimeoutSeconds(int chunkReadyTimeoutSeconds) {
      this.chunkReadyTimeoutSeconds = chunkReadyTimeoutSeconds;
      return this;
    }

    public ArrowResultChunkV2 build() {
      return new ArrowResultChunkV2(this);
    }
  }

  private class ChunkDownloadCallback implements FutureCallback<byte[]> {
    private final IDatabricksHttpClient httpClient;
    private final CompressionCodec compressionCodec;
    private final RetryConfig retryConfig;
    private final int currentAttempt;

    public ChunkDownloadCallback(
        IDatabricksHttpClient httpClient,
        CompressionCodec compressionCodec,
        RetryConfig retryConfig,
        int currentAttempt) {
      this.httpClient = httpClient;
      this.compressionCodec = compressionCodec;
      this.retryConfig = retryConfig;
      this.currentAttempt = currentAttempt;
    }

    @Override
    public void completed(byte[] result) {
      // Store downloaded data and update status on successful download
      downloadedBytes = result;
      setStatus(ChunkStatus.DOWNLOAD_SUCCEEDED);
      String context =
          String.format(
              "Data decompression for chunk index [%d] and statement [%s]",
              getChunkIndex(), statementId);
      // Submit arrow data processing task to executor
      arrowDataProcessingExecutor.submit(() -> processArrowData(compressionCodec, context));
    }

    @Override
    public void failed(Exception e) {
      // Handle download failures with retry logic
      handleRetryableError(
          httpClient,
          compressionCodec,
          retryConfig,
          currentAttempt,
          e,
          DownloadPhase.DATA_DOWNLOAD);
    }

    @Override
    public void cancelled() {
      // Update status and cancel future on request cancellation
      setStatus(ChunkStatus.CANCELLED);
      chunkReadyFuture.cancel(true);
    }
  }
}
