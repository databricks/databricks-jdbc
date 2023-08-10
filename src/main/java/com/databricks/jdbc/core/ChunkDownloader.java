package com.databricks.jdbc.core;

import com.databricks.sdk.service.sql.ChunkInfo;
import com.databricks.sdk.service.sql.ExternalLink;
import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.ResultManifest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class ChunkDownloader {

  private static final int CHUNKS_DOWNLOADER_THREAD_POOL_SIZE = 4;
  private static final String CHUNKS_DOWNLOADER_THREAD_POOL_PREFIX = "databricks-jdbc-chunks-downloader-";
  private final IDatabricksSession session;
  private final String statementId;
  private final long totalChunks;
  private final ExecutorService chunkDownloaderExecutorService;
  private long currentChunk;
  private long nextChunkLinkToDownload;
  private long nextChunkToDownload;
  private long totalChunksInMemory;
  private long allowedChunksInMemory;
  private long totalBytesInUse;

  ConcurrentHashMap<Long, ArrowResultChunk> chunkIndexToChunksMap;

  ChunkDownloader(String statementId, ResultManifest resultManifest, ResultData resultData, IDatabricksSession session) {
    this.session = session;
    this.statementId = statementId;
    this.totalChunks = resultManifest.getTotalChunkCount();
    this.chunkIndexToChunksMap = initializeChunksMap(resultManifest, resultData);
    // We are assuming that all links provided are in sequence. Currently, it only returns the
    // first chunk link, and next links need to be fetched sequentially using a separate API
    this.nextChunkLinkToDownload = resultData.getExternalLinks().size();
    // No chunks are downloaded, we need to start from first one
    this.nextChunkToDownload = 0;
    // Initialize current chunk to -1, since we don't have anything to read
    this.currentChunk = -1;
    // We don't have any chunk in downloaded yet
    this.totalChunksInMemory = 0;
    // Number of worker threads are directly linked to allowed chunks in memory
    this.allowedChunksInMemory = Math.min(CHUNKS_DOWNLOADER_THREAD_POOL_SIZE, totalChunks);
    this.chunkDownloaderExecutorService = createChunksDownloaderExecutorService();
    // The first link has been
    this.initDownloader();
  }

  private static ConcurrentHashMap<Long, ArrowResultChunk> initializeChunksMap(ResultManifest resultManifest, ResultData resultData) {
    ConcurrentHashMap<Long, ArrowResultChunk> chunkIndexMap = new ConcurrentHashMap<>();
    for (ChunkInfo chunkInfo : resultManifest.getChunks()) {
      chunkIndexMap.put(chunkInfo.getChunkIndex(), new ArrowResultChunk(chunkInfo));
    }

    for (ExternalLink externalLink : resultData.getExternalLinks()) {
      chunkIndexMap.get(externalLink.getChunkIndex()).setChunkUrl(externalLink);
    }
    return chunkIndexMap;
  }

  private static ExecutorService createChunksDownloaderExecutorService() {
    ThreadFactory threadFactory =
        new ThreadFactory() {
          private int threadCount = 1;

          public Thread newThread(final Runnable r) {
            final Thread thread = new Thread(r);
            thread.setName(CHUNKS_DOWNLOADER_THREAD_POOL_PREFIX + threadCount++);
            // TODO: catch uncaught exceptions
            thread.setDaemon(true);

            return thread;
          }
        };
    return Executors.newFixedThreadPool(CHUNKS_DOWNLOADER_THREAD_POOL_SIZE, threadFactory);
  }

    /**
     * Fetches the chunk for the given index. If chunk is not already downloaded, will download the chunk first
     * @param chunkIndex index of chunk
     * @return the chunk at given index
     */
  public ArrowResultChunk getChunk(long chunkIndex) {
    // TODO: download the chunk if not already downloaded
    return chunkIndexToChunksMap.get(chunkIndex);
  }

  private void initDownloader() {
    // The first link is already downloaded, we can download chunk for the same
    initChunksDownload();
    // Download all links using session's executor service. The links are downloaded sequentially.
    initLinksDownload(this.session.getExecutorService());
  }

  private void initLinksDownload(ExecutorService executorService) {
    if (nextChunkLinkToDownload < totalChunks) {
      executorService.submit(
          () -> {
            while (nextChunkLinkToDownload < totalChunks) {
              // TODO: handle failures
              ExternalLink chunk = session.getDatabricksClient().getResultChunk(statementId, nextChunkLinkToDownload).get();
              chunkIndexToChunksMap.get(chunk.getChunkIndex()).setChunkUrl(chunk);
              nextChunkLinkToDownload++;
            }
            // Once all chunk links has been downloaded, trigger the chunks download.
            initChunksDownload();
          });
    }
  }

  /**
   * Starts the chunk download from given already downloaded position
   */
  private synchronized void initChunksDownload() {
    // TODO: Add impl, avoiding UnsupportedOperationException for test case
  }

  /**
   * Release the memory for given chunk since it is already consumed
   * @param chunkIndex index of consumed chunk
   */
  public synchronized void releaseChunk(int chunkIndex) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public void setChunkLink(int chunkIndex, ExternalLink chunkLink) {
    chunkIndexToChunksMap.get(chunkIndex).setChunkUrl(chunkLink);
  }
}
