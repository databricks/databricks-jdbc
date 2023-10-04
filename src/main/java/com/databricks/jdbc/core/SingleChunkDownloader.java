package com.databricks.jdbc.core;

import com.databricks.jdbc.client.DatabricksHttpException;
import com.databricks.jdbc.client.IDatabricksHttpClient;

import java.util.concurrent.Callable;

/**
 * Task class to manage download for a single chunk.
 */
class SingleChunkDownloader implements Callable<Void> {

  private final ArrowResultChunk chunk;
  private final IDatabricksHttpClient httpClient;
  private final ChunkDownloader chunkDownloader;

  SingleChunkDownloader(ArrowResultChunk chunk, IDatabricksHttpClient httpClient, ChunkDownloader chunkDownloader) {
    this.chunk = chunk;
    this.httpClient = httpClient;
    this.chunkDownloader = chunkDownloader;
  }

  @Override
  public Void call() throws Exception {
    if (chunk.isChunkLinkInvalid()) {
      chunkDownloader.downloadLinks(chunk.getChunkIndex());
    }
    try {
      chunk.downloadData(httpClient);
    } catch (DatabricksHttpException | DatabricksParsingException e) {
      // TODO: handle retries
    } finally {
      chunkDownloader.downloadProcessed(chunk.getChunkIndex());
    }
    return null;
  }
}
