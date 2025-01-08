package com.databricks.jdbc.api.impl.arrow.incubator;

import static com.databricks.jdbc.common.util.DatabricksThriftUtil.createExternalLink;

import com.databricks.jdbc.api.impl.arrow.AbstractArrowResultChunk;
import com.databricks.jdbc.api.impl.arrow.ChunkStatus;
import com.databricks.jdbc.common.CompressionCodec;
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
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.time.Instant;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.ConnectionClosedException;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.nio.AsyncRequestProducer;
import org.apache.hc.core5.http.nio.support.AsyncRequestBuilder;

public class ArrowResultChunkV2 extends AbstractArrowResultChunk {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ArrowResultChunkV2.class);

  /**
   * Scheduler dedicated to retry operations for failed chunk downloads. Uses a small thread pool
   * since chunk downloads operate asynchronously.
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

  /** A thread pool executor for processing Arrow data chunks. */
  private static final ExecutorService arrowDataProcessingExecutor =
      Executors.newFixedThreadPool(
          150,
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
        builder.expiryTime);
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * {@inheritDoc}
   *
   * <p>Downloads and processes the Arrow data chunk asynchronously using the provided HTTP client
   * and compression codec. Implements retry logic with exponential backoff for handling transient
   * failures. The download and processing are done in separate phases using async I/O and thread
   * pools.
   *
   * @param httpClient the HTTP client used to download the chunk data asynchronously
   * @param compressionCodec the codec used to decompress the downloaded data
   */
  @Override
  protected void downloadData(IDatabricksHttpClient httpClient, CompressionCodec compressionCodec) {
    RetryConfig retryConfig =
        new RetryConfig.Builder().maxAttempts(3).baseDelayMs(1000).maxDelayMs(5000).build();
    retryDownload(httpClient, compressionCodec, retryConfig, 1);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Handles failures that occur during chunk download or processing. Sets the error message,
   * logs the error, updates the chunk status, and completes the chunk's future exceptionally.
   *
   * @param exception the exception that caused the failure
   * @param failedStatus the status to set for the chunk after failure (e.g. {@link
   *     ChunkStatus#DOWNLOAD_FAILED} or {@link ChunkStatus#PROCESSING_FAILED})
   */
  @Override
  protected void handleFailure(Exception exception, ChunkStatus failedStatus) {
    errorMessage =
        String.format(
            "Data parsing failed for chunk index [%d] with status [%s] and statement [%s]. Exception [%s]",
            chunkIndex, getStatus(), statementId, exception);
    LOGGER.error(errorMessage);
    setStatus(failedStatus);
    chunkReadyFuture.completeExceptionally(new DatabricksParsingException(errorMessage, exception));
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
          new FutureCallback<>() {
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
              setStatus(ChunkStatus.DOWNLOAD_FAILED);
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
          });
    } catch (Exception e) {
      setStatus(ChunkStatus.DOWNLOAD_FAILED);
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
      setStatus(ChunkStatus.PROCESSING_SUCCEEDED);
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

  /**
   * Determines if an error is retryable based on its type and characteristics.
   *
   * @param e the exception to evaluate
   * @return true if the error is retryable, false otherwise
   */
  private boolean isRetryableError(Exception e) {
    return e instanceof SocketException
        || e instanceof SocketTimeoutException
        || e instanceof ConnectionClosedException
        || e instanceof DatabricksHttpException
        || (e instanceof IOException && e.getMessage().contains("Connection reset"))
        || (e instanceof HttpException
            && e.getMessage().contains("Unexpected response status: 500"))
        || (e instanceof DatabricksParsingException && e.getCause() instanceof SocketException);
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
    private String statementId;
    private Instant expiryTime;
    private ChunkStatus status;

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

    public Builder withThriftChunkInfo(long chunkIndex, TSparkArrowResultLink chunkInfo) {
      this.chunkIndex = chunkIndex;
      this.numRows = chunkInfo.getRowCount();
      this.rowOffset = chunkInfo.getStartRowOffset();
      this.expiryTime = Instant.ofEpochMilli(chunkInfo.getExpiryTime());
      this.status = ChunkStatus.URL_FETCHED; // URL has always been fetched in case of thrift
      this.chunkLink = createExternalLink(chunkInfo, chunkIndex);
      return this;
    }

    public ArrowResultChunkV2 build() {
      return new ArrowResultChunkV2(this);
    }
  }
}
