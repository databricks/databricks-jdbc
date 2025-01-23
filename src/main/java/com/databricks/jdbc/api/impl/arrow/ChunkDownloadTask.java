package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.io.IOException;

/** Task class to manage download for a single chunk. */
class ChunkDownloadTask implements DatabricksCallableTask {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ChunkDownloadTask.class);
  public static final int MAX_RETRIES = 5;
  private static final long RETRY_DELAY_MS = 1500; // 1.5 seconds
  private final ArrowResultChunk chunk;
  private final IDatabricksHttpClient httpClient;
  private final ChunkDownloadCallback chunkDownloader;
  private final IDatabricksConnectionContext connectionContext;
  private final StatementId statementId;

  ChunkDownloadTask(
      ArrowResultChunk chunk,
      IDatabricksHttpClient httpClient,
      ChunkDownloadCallback chunkDownloader) {
    this.chunk = chunk;
    this.httpClient = httpClient;
    this.chunkDownloader = chunkDownloader;
    this.connectionContext = DatabricksThreadContextHolder.getConnectionContext();
    this.statementId = DatabricksThreadContextHolder.getStatementId();
  }

  @Override
  public Void call() throws DatabricksSQLException {
    int retries = 0;
    boolean downloadSuccessful = false;
    DatabricksThreadContextHolder.setChunkId(chunk.getChunkIndex());
    // Sets  context in the newly spawned thread
    DatabricksThreadContextHolder.setConnectionContext(this.connectionContext);
    DatabricksThreadContextHolder.setStatementId(this.statementId);
    try {
      while (retries < MAX_RETRIES && !downloadSuccessful) {
        try {
          if (chunk.isChunkLinkInvalid()) {
            chunkDownloader.downloadLinks(chunk.getChunkIndex());
          }
          chunk.downloadData(httpClient, chunkDownloader.getCompressionCodec());
          downloadSuccessful = true;
        } catch (DatabricksParsingException | IOException e) {
          retries++;
          if (retries >= MAX_RETRIES) {
            LOGGER.error(
                e,
                "Failed to download chunk after %d attempts. Chunk index: %d, Error: %s",
                MAX_RETRIES,
                chunk.getChunkIndex(),
                e.getMessage());
            chunk.setStatus(ArrowResultChunk.ChunkStatus.DOWNLOAD_FAILED);
            throw new DatabricksSQLException(
                "Failed to download chunk after multiple attempts",
                e,
                DatabricksDriverErrorCode.CHUNK_DOWNLOAD_ERROR);
          } else {
            LOGGER.warn(
                String.format(
                    "Retry attempt %d for chunk index: %d, Error: %s",
                    retries, chunk.getChunkIndex(), e.getMessage()));
            chunk.setStatus(ArrowResultChunk.ChunkStatus.DOWNLOAD_RETRY);
            try {
              Thread.sleep(RETRY_DELAY_MS);
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
    } finally {
      if (!downloadSuccessful) {
        chunk.setStatus(ArrowResultChunk.ChunkStatus.DOWNLOAD_FAILED);
      }
      chunkDownloader.downloadProcessed(chunk.getChunkIndex());
      DatabricksThreadContextHolder.clearAllContext();
    }
    return null;
  }
}
