package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.io.IOException;
import java.util.concurrent.Callable;

/** Task class to manage download for a single chunk. */
class SingleChunkDownloader implements Callable<Void> {
  public static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(SingleChunkDownloader.class);
  private final ArrowResultChunk chunk;
  private final IDatabricksHttpClient httpClient;
  private final ChunkDownloader chunkDownloader;

  SingleChunkDownloader(
      ArrowResultChunk chunk, IDatabricksHttpClient httpClient, ChunkDownloader chunkDownloader) {
    this.chunk = chunk;
    this.httpClient = httpClient;
    this.chunkDownloader = chunkDownloader;
  }

  @Override
  public Void call() throws DatabricksSQLException {
    if (chunk.isChunkLinkInvalid()) {
      chunkDownloader.downloadLinks(chunk.getChunkIndex());
    }
    try {
      chunk.downloadData(httpClient);
    } catch (DatabricksHttpException | DatabricksParsingException e) {
      // TODO: handle retries
    } catch (IOException e) {
      LOGGER.error(
          String.format("Unable to close response, there might be a connection leak. %s", e.getMessage()), e);
    } finally {
      chunkDownloader.downloadProcessed(chunk.getChunkIndex());
    }
    return null;
  }
}
