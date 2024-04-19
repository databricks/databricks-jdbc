package com.databricks.jdbc.core;

import com.databricks.jdbc.client.DatabricksHttpException;
import com.databricks.jdbc.client.IDatabricksHttpClient;
import java.io.IOException;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Task class to manage download for a single chunk. */
class SingleChunkDownloader implements Callable<Void> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SingleChunkDownloader.class);
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
      LOGGER.warn("Unable to close response, there might be a connection leak", e);
    } finally {
      chunkDownloader.downloadProcessed(chunk.getChunkIndex());
    }
    return null;
  }
}
