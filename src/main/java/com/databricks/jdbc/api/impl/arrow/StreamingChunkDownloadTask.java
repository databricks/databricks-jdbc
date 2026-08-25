package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksParsingException;
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
  private static final long RETRY_DELAY_MS = 1500;

  private final ArrowResultChunk chunk;
  private final IDatabricksHttpClient httpClient;
  private final CompressionCodec compressionCodec;
  private final LinkRefresher linkRefresher;
  private final double cloudFetchSpeedThreshold;

  // Capture caller's thread context for telemetry/logging on the download thread
  private final IDatabricksConnectionContext connectionContext;
  private final String statementId;

  public StreamingChunkDownloadTask(
      ArrowResultChunk chunk,
      IDatabricksHttpClient httpClient,
      CompressionCodec compressionCodec,
      LinkRefresher linkRefresher,
      double cloudFetchSpeedThreshold) {
    this.chunk = chunk;
    this.httpClient = httpClient;
    this.compressionCodec = compressionCodec;
    this.linkRefresher = linkRefresher;
    this.cloudFetchSpeedThreshold = cloudFetchSpeedThreshold;
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

        } catch (DatabricksParsingException e) {
          throw e;
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
            LOGGER.warn(
                "Retry {} for chunk {}: {}", retries, chunk.getChunkIndex(), e.getMessage());
            chunk.setStatus(ChunkStatus.DOWNLOAD_RETRY);
            try {
              Thread.sleep(RETRY_DELAY_MS);
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
        if (chunk.getStatus() != ChunkStatus.DOWNLOAD_FAILED
            && chunk.getStatus() != ChunkStatus.PROCESSING_FAILED) {
          chunk.setStatus(ChunkStatus.DOWNLOAD_FAILED);
        }
        chunk.getChunkReadyFuture().completeExceptionally(uncaughtException);
      }

      DatabricksThreadContextHolder.clearAllContext();
    }

    return null;
  }
}
