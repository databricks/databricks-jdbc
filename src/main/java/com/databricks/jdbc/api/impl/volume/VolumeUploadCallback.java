package com.databricks.jdbc.api.impl.volume;

import com.databricks.jdbc.api.impl.VolumeOperationStatus;
import com.databricks.jdbc.api.impl.volume.DBFSVolumeClient.UploadRequest;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.util.VolumeRetryUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.filesystem.CreateUploadUrlResponse;
import com.databricks.jdbc.model.client.filesystem.VolumePutResult;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.async.methods.SimpleResponseConsumer;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.nio.AsyncEntityProducer;
import org.apache.hc.core5.http.nio.AsyncRequestProducer;
import org.apache.hc.core5.http.nio.AsyncResponseConsumer;
import org.apache.hc.core5.http.nio.entity.AsyncEntityProducers;
import org.apache.hc.core5.http.nio.support.AsyncRequestBuilder;

/**
 * Unified callback for both file and stream uploads to DBFS volumes. Handles retry logic for failed
 * uploads with exponential backoff.
 */
public class VolumeUploadCallback implements FutureCallback<SimpleHttpResponse> {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(VolumeUploadCallback.class);

  private final IDatabricksHttpClient httpClient;
  private final CompletableFuture<VolumePutResult> uploadFuture;
  private final UploadRequest request;
  private final Semaphore semaphore;
  private final int attempt;
  private final UrlGenerator urlGenerator;
  private final Function<Integer, Long> retryDelayCalculator;
  private final IDatabricksConnectionContext connectionContext;
  private final long retryStartTime;

  /** Interface for generating presigned URLs. */
  @FunctionalInterface
  public interface UrlGenerator {
    CompletableFuture<CreateUploadUrlResponse> apply(
        String ucVolumePath, String objectPath, int attempt);
  }

  /**
   * Constructor for the callback.
   *
   * @param httpClient The HTTP client for making requests
   * @param uploadFuture Future to complete when upload is done
   * @param request The upload request with file or stream details
   * @param semaphore Semaphore for controlling concurrency
   * @param urlGenerator Function to generate presigned URLs
   * @param retryDelayCalculator Function to calculate retry delays
   */
  public VolumeUploadCallback(
      IDatabricksHttpClient httpClient,
      CompletableFuture<VolumePutResult> uploadFuture,
      UploadRequest request,
      Semaphore semaphore,
      UrlGenerator urlGenerator,
      Function<Integer, Long> retryDelayCalculator,
      IDatabricksConnectionContext connectionContext) {
    this(
        httpClient,
        uploadFuture,
        request,
        semaphore,
        urlGenerator,
        retryDelayCalculator,
        connectionContext,
        1,
        System.currentTimeMillis());
  }

  /** Constructor with attempt number for retries. */
  private VolumeUploadCallback(
      IDatabricksHttpClient httpClient,
      CompletableFuture<VolumePutResult> uploadFuture,
      UploadRequest request,
      Semaphore semaphore,
      UrlGenerator urlGenerator,
      Function<Integer, Long> retryDelayCalculator,
      IDatabricksConnectionContext connectionContext,
      int attempt,
      long retryStartTime) {
    this.httpClient = httpClient;
    this.uploadFuture = uploadFuture;
    this.request = request;
    this.semaphore = semaphore;
    this.urlGenerator = urlGenerator;
    this.retryDelayCalculator = retryDelayCalculator;
    this.connectionContext = connectionContext;
    this.attempt = attempt;
    this.retryStartTime = retryStartTime;
  }

  @Override
  public void completed(SimpleHttpResponse uploadResult) {
    if (uploadResult.getCode() >= 200 && uploadResult.getCode() < 300) {
      // Success case
      VolumeOperationStatus status = VolumeOperationStatus.SUCCEEDED;
      uploadFuture.complete(new VolumePutResult(uploadResult.getCode(), status, null));
    } else if (VolumeRetryUtil.isRetryableHttpCode(uploadResult.getCode(), connectionContext)
        && VolumeRetryUtil.shouldRetry(attempt, retryStartTime, connectionContext)) {
      // Server error - retry with backoff
      long retryDelayMs = retryDelayCalculator.apply(Integer.valueOf(attempt));
      long elapsedSeconds = (System.currentTimeMillis() - retryStartTime) / 1000;
      int timeoutSeconds = VolumeRetryUtil.getRetryTimeoutSeconds(connectionContext);
      LOGGER.warn(
          "Upload failed for {}: HTTP {} - {}. Retrying in {} ms (elapsed: {}s, timeout: {}s)",
          request.objectPath,
          uploadResult.getCode(),
          uploadResult.getReasonPhrase(),
          retryDelayMs,
          elapsedSeconds,
          timeoutSeconds);

      // Retry the entire upload process
      retry(retryDelayMs);
    } else {
      // Permanent failure or max retries exceeded
      VolumeOperationStatus status = VolumeOperationStatus.FAILED;
      String message = uploadResult.getReasonPhrase();

      LOGGER.error(
          "Upload failed for {}: HTTP {} - {}",
          request.objectPath,
          uploadResult.getCode(),
          message);

      uploadFuture.complete(new VolumePutResult(uploadResult.getCode(), status, message));
    }
  }

  @Override
  public void failed(Exception ex) {
    if (VolumeRetryUtil.shouldRetry(attempt, retryStartTime, connectionContext)) {
      long retryDelayMs = retryDelayCalculator.apply(attempt);
      long elapsedSeconds = (System.currentTimeMillis() - retryStartTime) / 1000;
      int timeoutSeconds = VolumeRetryUtil.getRetryTimeoutSeconds(connectionContext);
      LOGGER.warn(
          "Upload failed for {}: {}. Retrying in {} ms (elapsed: {}s, timeout: {}s)",
          request.objectPath,
          ex.getMessage(),
          retryDelayMs,
          elapsedSeconds,
          timeoutSeconds);

      // Retry the entire upload process
      retry(retryDelayMs);
    } else {
      LOGGER.error(ex, "Upload failed for {}: {}", request.objectPath, ex.getMessage());
      uploadFuture.complete(
          new VolumePutResult(500, VolumeOperationStatus.FAILED, ex.getMessage()));
    }
  }

  @Override
  public void cancelled() {
    LOGGER.warn("Upload cancelled for {}", request.objectPath);
    uploadFuture.complete(
        new VolumePutResult(499, VolumeOperationStatus.ABORTED, "Upload cancelled"));
  }

  /**
   * Retry the upload after a delay.
   *
   * @param delayMs Delay before retry in milliseconds
   */
  private void retry(long delayMs) {
    java.util.concurrent.Executors.newSingleThreadScheduledExecutor()
        .schedule(
            new Runnable() {
              @Override
              public void run() {
                // Handle stream reset if needed
                if (!request.isFile() && request.inputStream.markSupported()) {
                  try {
                    request.inputStream.reset();
                  } catch (IOException e) {
                    LOGGER.warn("Could not reset input stream for retry: " + e.getMessage());
                  }
                }

                // Get a new presigned URL and retry the upload
                urlGenerator
                    .apply(request.ucVolumePath, request.objectPath, 1)
                    .thenAccept(
                        response -> {
                          String presignedUrl = response.getUrl();
                          LOGGER.debug(
                              "Got new presigned URL for retry of {} (attempt {})",
                              request.objectPath,
                              attempt + 1);

                          try {
                            // Create upload producer
                            AsyncRequestProducer uploadProducer;
                            if (request.isFile()) {
                              // File upload
                              uploadProducer =
                                  AsyncRequestBuilder.put()
                                      .setUri(URI.create(presignedUrl))
                                      .setEntity(
                                          AsyncEntityProducers.create(
                                              request.file.toFile(), ContentType.DEFAULT_BINARY))
                                      .build();
                            } else {
                              // Stream upload
                              AsyncEntityProducer entity =
                                  new InputStreamFixedLenProducer(
                                      request.inputStream, request.contentLength);
                              uploadProducer =
                                  AsyncRequestBuilder.put()
                                      .setUri(URI.create(presignedUrl))
                                      .setEntity(entity)
                                      .build();
                            }

                            AsyncResponseConsumer<SimpleHttpResponse> uploadConsumer =
                                SimpleResponseConsumer.create();

                            // Create callback with incremented attempt count
                            VolumeUploadCallback uploadCallback =
                                new VolumeUploadCallback(
                                    httpClient,
                                    uploadFuture,
                                    request,
                                    semaphore,
                                    urlGenerator,
                                    retryDelayCalculator,
                                    connectionContext,
                                    attempt + 1,
                                    retryStartTime);

                            httpClient.executeAsync(uploadProducer, uploadConsumer, uploadCallback);
                          } catch (Exception e) {
                            String errorMessage =
                                "Error setting up retry for "
                                    + request.objectPath
                                    + ": "
                                    + e.getMessage();
                            LOGGER.error(e, errorMessage);
                            uploadFuture.complete(
                                new VolumePutResult(
                                    500, VolumeOperationStatus.FAILED, errorMessage));
                          }
                        })
                    .exceptionally(
                        e -> {
                          String errorMessage =
                              "Failed to get presigned URL for retry of "
                                  + request.objectPath
                                  + ": "
                                  + e.getMessage();
                          LOGGER.error(e, errorMessage);
                          uploadFuture.complete(
                              new VolumePutResult(500, VolumeOperationStatus.FAILED, errorMessage));
                          return null;
                        });
              }
            },
            delayMs,
            TimeUnit.MILLISECONDS);
  }
}
