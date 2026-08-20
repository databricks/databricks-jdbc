package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpRetryHandler;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

/** Task class to manage download for a single chunk. */
class ChunkDownloadTask implements DatabricksCallableTask {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ChunkDownloadTask.class);
  public static final int MAX_RETRIES = 5;
  private final ArrowResultChunk chunk;
  private final IDatabricksHttpClient httpClient;
  private final ChunkDownloadManager chunkDownloader;
  private final IDatabricksConnectionContext connectionContext;
  private final String statementId;
  private final ChunkLinkDownloadService<ArrowResultChunk> linkDownloadService;
  Throwable uncaughtException = null;

  ChunkDownloadTask(
      ArrowResultChunk chunk,
      IDatabricksHttpClient httpClient,
      ChunkDownloadManager chunkDownloader,
      ChunkLinkDownloadService<ArrowResultChunk> linkDownloadService) {
    this.chunk = chunk;
    this.httpClient = httpClient;
    this.chunkDownloader = chunkDownloader;
    this.connectionContext = DatabricksThreadContextHolder.getConnectionContext();
    this.statementId = DatabricksThreadContextHolder.getStatementId();
    this.linkDownloadService = linkDownloadService;
  }

  @Override
  public Void call() throws DatabricksSQLException, ExecutionException, InterruptedException {
    int retries = 0;
    boolean downloadSuccessful = false;

    // Sets context in the newly spawned thread
    DatabricksThreadContextHolder.setConnectionContext(this.connectionContext);
    DatabricksThreadContextHolder.setStatementId(this.statementId);

    long taskStartTime = System.nanoTime();
    try {
      DatabricksThreadContextHolder.setRetryCount(retries);
      while (!downloadSuccessful) {
        try {
          if (chunk.isChunkLinkInvalid()) {
            long linkWaitStart = System.nanoTime();
            ExternalLink link =
                linkDownloadService
                    .getLinkForChunk(chunk.getChunkIndex())
                    .get(); // Block until link is available
            long linkWaitMs = (System.nanoTime() - linkWaitStart) / 1_000_000;
            LOGGER.debug(
                "Link wait: statementId={}, chunkIndex={}, linkWaitMs={}",
                statementId,
                chunk.getChunkIndex(),
                linkWaitMs);
            chunk.setChunkLink(link);
          }

          chunk.downloadData(
              httpClient,
              chunkDownloader.getCompressionCodec(),
              connectionContext != null ? connectionContext.getCloudFetchSpeedThreshold() : 0.1);
          downloadSuccessful = true;
          long taskTotalMs = (System.nanoTime() - taskStartTime) / 1_000_000;
          LOGGER.debug(
              "ChunkDownloadTask complete: statementId={}, chunkIndex={}, totalTaskMs={}, retries={}",
              statementId,
              chunk.getChunkIndex(),
              taskTotalMs,
              retries);
        } catch (IOException | DatabricksSQLException e) {
          int httpStatus = extractHttpStatus(e);
          retries++;
          if (retries >= MAX_RETRIES) {
            LOGGER.error(
                e,
                "Failed to download chunk after %d attempts. Chunk index: %d, HTTP status: %d, Error: %s",
                MAX_RETRIES,
                chunk.getChunkIndex(),
                httpStatus,
                e.getMessage());
            chunk.setStatus(ChunkStatus.DOWNLOAD_FAILED);
            throw new DatabricksSQLException(
                String.format(
                    "Failed to download chunk after multiple attempts (HTTP status: %d)",
                    httpStatus),
                e,
                statementId,
                chunk.getChunkIndex(),
                DatabricksDriverErrorCode.CHUNK_DOWNLOAD_ERROR.name());
          } else if (isPermanentHttpFailure(httpStatus)) {
            LOGGER.error(
                e,
                "Permanent HTTP %d error for chunk index: %d, will not retry. Error: %s",
                httpStatus,
                chunk.getChunkIndex(),
                e.getMessage());
            chunk.setStatus(ChunkStatus.DOWNLOAD_FAILED);
            throw new DatabricksSQLException(
                String.format(
                    "Permanent HTTP %d error downloading chunk %d",
                    httpStatus, chunk.getChunkIndex()),
                e,
                statementId,
                chunk.getChunkIndex(),
                DatabricksDriverErrorCode.CHUNK_DOWNLOAD_ERROR.name());
          } else {
            long delayMs = DatabricksHttpRetryHandler.calculateExponentialBackoff(retries);
            LOGGER.warn(
                String.format(
                    "Retry attempt %d for chunk index: %d, HTTP status: %d, retryDelayMs: %d, Error: %s",
                    retries, chunk.getChunkIndex(), httpStatus, delayMs, e.getMessage()));
            chunk.setStatus(ChunkStatus.DOWNLOAD_RETRY);
            try {
              Thread.sleep(delayMs);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              throw new DatabricksSQLException(
                  "Chunk download was interrupted",
                  ie,
                  DatabricksDriverErrorCode.THREAD_INTERRUPTED_ERROR);
            }
          }
        }
      }
    } catch (Throwable t) {
      uncaughtException = t;
      throw t;
    } finally {
      if (downloadSuccessful) {
        chunk.getChunkReadyFuture().complete(null); // complete the void future successfully
      } else {
        LOGGER.info(
            "Uncaught exception during chunk download. Chunk index: {}, Error: {}",
            chunk.getChunkIndex(),
            Arrays.toString(uncaughtException.getStackTrace()));
        // Status is set to DOWNLOAD_SUCCEEDED in the happy path. For any failure case,
        // explicitly set status to DOWNLOAD_FAILED here to ensure consistent error handling
        chunk.setStatus(ChunkStatus.DOWNLOAD_FAILED);
        chunk
            .getChunkReadyFuture()
            .completeExceptionally(
                new DatabricksSQLException(
                    "Download failed for chunk index " + chunk.getChunkIndex(),
                    uncaughtException,
                    DatabricksDriverErrorCode.CHUNK_DOWNLOAD_ERROR));
      }

      DatabricksThreadContextHolder.clearAllContext();
    }

    return null;
  }

  /**
   * Extracts the HTTP status code from the exception or its direct cause when either is a {@link
   * DatabricksHttpException}. Returns 0 when the failure is a network error with no HTTP response.
   */
  private static int extractHttpStatus(Exception e) {
    if (e instanceof DatabricksHttpException) {
      return ((DatabricksHttpException) e).getHttpStatusCode();
    }
    Throwable cause = e.getCause();
    if (cause instanceof DatabricksHttpException) {
      return ((DatabricksHttpException) cause).getHttpStatusCode();
    }
    return 0;
  }

  /**
   * Returns {@code true} for 4xx HTTP status codes that represent permanent client errors not worth
   * retrying. Excludes codes that are transient or recoverable:
   *
   * <ul>
   *   <li>403: pre-signed URL may have expired; {@code isChunkLinkInvalid()} will detect and
   *       refresh the link on the next iteration.
   *   <li>408: request timeout — transient.
   *   <li>429: rate limit — transient.
   * </ul>
   */
  private static boolean isPermanentHttpFailure(int httpStatus) {
    return httpStatus >= 400
        && httpStatus < 500
        && httpStatus != 403
        && httpStatus != 408
        && httpStatus != 429;
  }
}
