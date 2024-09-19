package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.io.IOException;
import java.util.concurrent.Callable;

/** Task class to manage download for a single chunk. */
class SingleChunkDownloader implements Callable<Void> {

  public static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(SingleChunkDownloader.class);
  public static final int MAX_RETRIES = 5;
  private static final long RETRY_DELAY_MS = 1500; // 1.5 seconds
  private final ArrowResultChunk chunk;
  private final IDatabricksHttpClient httpClient;
  private final ChunkDownloadCallback chunkDownloader;

  SingleChunkDownloader(
      ArrowResultChunk chunk,
      IDatabricksHttpClient httpClient,
      ChunkDownloadCallback chunkDownloader) {
    this.chunk = chunk;
    this.httpClient = httpClient;
    this.chunkDownloader = chunkDownloader;
  }

  @Override
  public Void call() throws DatabricksSQLException {
    int retries = 0;
    boolean downloadSuccessful = false;

    try {
      while (retries < MAX_RETRIES && !downloadSuccessful) {
        try {
          if (chunk.isChunkLinkInvalid()) {
            chunkDownloader.downloadLinks(chunk.getChunkIndex());
          }
          chunk.downloadData(httpClient, chunkDownloader.getCompressionType());
          downloadSuccessful = true;
        } catch (DatabricksParsingException | IOException e) {
          retries++;
          if (retries >= MAX_RETRIES) {
            LOGGER.error(
                String.format(
                    "Failed to download chunk after %d attempts. Chunk index: %d, Error: %s",
                    MAX_RETRIES, chunk.getChunkIndex(), e.getMessage()));
            chunk.setStatus(ArrowResultChunk.ChunkStatus.DOWNLOAD_FAILED);
            throw new DatabricksSQLException("Failed to download chunk after multiple attempts", e);
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
              throw new DatabricksSQLException("Chunk download was interrupted", ie);
            }
          }
        }
      }
    } finally {
      chunkDownloader.downloadProcessed(chunk.getChunkIndex());
    }
    return null;
  }
}
