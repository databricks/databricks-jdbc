package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Callable;

/**
 * A download task for streaming chunk provider. Uses a {@link LinkRefresher} callback for link
 * refresh, which enables the {@link StreamingChunkProvider} to coalesce concurrent expired-link
 * refreshes into a single batch RPC.
 */
public class StreamingChunkDownloadTask implements Callable<Void> {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(StreamingChunkDownloadTask.class);

  private static final int MAX_RETRIES = 5;

  private final ArrowResultChunk chunk;
  private final IDatabricksHttpClient httpClient;
  private final CompressionCodec compressionCodec;
  private final LinkRefresher linkRefresher;
  private final double cloudFetchSpeedThreshold;
  private final ChunkRetryPolicy retryPolicy;

  // Capture caller's thread context for telemetry/logging on the download thread
  private final IDatabricksConnectionContext connectionContext;
  private final String statementId;

  public StreamingChunkDownloadTask(
      ArrowResultChunk chunk,
      IDatabricksHttpClient httpClient,
      CompressionCodec compressionCodec,
      LinkRefresher linkRefresher,
      double cloudFetchSpeedThreshold) {
    this(
        chunk,
        httpClient,
        compressionCodec,
        linkRefresher,
        cloudFetchSpeedThreshold,
        new ChunkRetryPolicy());
  }

  StreamingChunkDownloadTask(
      ArrowResultChunk chunk,
      IDatabricksHttpClient httpClient,
      CompressionCodec compressionCodec,
      LinkRefresher linkRefresher,
      double cloudFetchSpeedThreshold,
      ChunkRetryPolicy retryPolicy) {
    this.chunk = chunk;
    this.httpClient = httpClient;
    this.compressionCodec = compressionCodec;
    this.linkRefresher = linkRefresher;
    this.cloudFetchSpeedThreshold = cloudFetchSpeedThreshold;
    this.retryPolicy = retryPolicy;
    this.connectionContext = DatabricksThreadContextHolder.getConnectionContext();
    this.statementId = DatabricksThreadContextHolder.getStatementId();
  }

  @Override
  public Void call() throws DatabricksSQLException {
    int retries = 0;
    boolean downloadSuccessful = false;
    Throwable uncaughtException = null;

    // Propagate caller's thread context for telemetry/logging
    DatabricksThreadContextHolder.setConnectionContext(this.connectionContext);
    DatabricksThreadContextHolder.setStatementId(this.statementId);

    long taskStartTime = System.nanoTime();
    try {
      while (!downloadSuccessful) {
        try {
          // Check if link is expired and refresh if needed.
          // The LinkRefresher (StreamingChunkProvider.getRefreshedLink) updates the chunk's
          // link directly under the refetchLock, so we don't need to set it here.
          if (chunk.isChunkLinkInvalid()) {
            LOGGER.debug("Link invalid for chunk {}, refetching", chunk.getChunkIndex());
            linkRefresher.refreshLink(chunk.getChunkIndex(), chunk.getStartRowOffset());
          }

          // Perform the download
          chunk.downloadData(httpClient, compressionCodec, cloudFetchSpeedThreshold);
          downloadSuccessful = true;

          long taskTotalMs = (System.nanoTime() - taskStartTime) / 1_000_000;
          LOGGER.debug(
              "Chunk download complete: chunkIndex={}, totalMs={}, retries={}",
              chunk.getChunkIndex(),
              taskTotalMs,
              retries);

        } catch (IOException | SQLException e) {
          retries++;
          if (retries >= MAX_RETRIES) {
            LOGGER.error(
                "Failed to download chunk {} after {} attempts: {}",
                chunk.getChunkIndex(),
                MAX_RETRIES,
                e.getMessage());
            // Status set to DOWNLOAD_FAILED in the finally block
            throw new DatabricksSQLException(
                String.format(
                    "Failed to download chunk %d after %d attempts",
                    chunk.getChunkIndex(), MAX_RETRIES),
                e,
                DatabricksDriverErrorCode.CHUNK_DOWNLOAD_ERROR);
          } else {
            long retryDelayMs = retryPolicy.getRetryDelayMs();
            LOGGER.warn(
                "Retry {} for chunk {} in {} ms: {}",
                retries,
                chunk.getChunkIndex(),
                retryDelayMs,
                e.getMessage());
            chunk.setStatus(ChunkStatus.DOWNLOAD_RETRY);
            try {
              retryPolicy.sleep(retryDelayMs);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              throw new DatabricksSQLException(
                  "Chunk download interrupted",
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
        chunk.getChunkReadyFuture().complete(null);
      } else {
        LOGGER.error(
            "Download failed for chunk {}: {}",
            chunk.getChunkIndex(),
            uncaughtException != null ? uncaughtException.getMessage() : "unknown");
        chunk.setStatus(ChunkStatus.DOWNLOAD_FAILED);
        chunk
            .getChunkReadyFuture()
            .completeExceptionally(
                new DatabricksSQLException(
                    "Download failed for chunk " + chunk.getChunkIndex(),
                    uncaughtException,
                    DatabricksDriverErrorCode.CHUNK_DOWNLOAD_ERROR));
      }

      DatabricksThreadContextHolder.clearAllContext();
    }

    return null;
  }
}
