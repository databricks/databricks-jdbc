package com.databricks.jdbc.core;

import com.databricks.jdbc.client.IDatabricksHttpClient;
import com.databricks.jdbc.client.http.DatabricksHttpClient;
import com.databricks.sdk.service.sql.ChunkInfo;
import com.databricks.sdk.service.sql.ExternalLink;
import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.ResultManifest;
import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.memory.RootAllocator;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Class to manage Arrow chunks and fetch them on proactive basis.
 */
public class ChunkDownloader {
  private static final int CHUNKS_DOWNLOADER_THREAD_POOL_SIZE = 4;
  private static final String CHUNKS_DOWNLOADER_THREAD_POOL_PREFIX = "databricks-jdbc-chunks-downloader-";
  private final IDatabricksSession session;
  private final String statementId;
  private final long totalChunks;
  private final ExecutorService chunkDownloaderExecutorService;
  private final IDatabricksHttpClient httpClient;
  private Long currentChunkIndex;
  private long nextChunkLinkToDownload;
  private long nextChunkToDownload;
  private Long totalChunksInMemory;
  private long allowedChunksInMemory;
  private long totalBytesInUse;
  private boolean isClosed;

  ConcurrentHashMap<Long, ArrowResultChunk> chunkIndexToChunksMap;

  ChunkDownloader(String statementId, ResultManifest resultManifest, ResultData resultData, IDatabricksSession session) {
    this(statementId, resultManifest, resultData, session, DatabricksHttpClient.getInstance());
  }

  @VisibleForTesting
  ChunkDownloader(String statementId, ResultManifest resultManifest, ResultData resultData, IDatabricksSession session,
                  IDatabricksHttpClient httpClient) {
    this.session = session;
    this.statementId = statementId;
    this.totalChunks = resultManifest.getTotalChunkCount();
    this.chunkIndexToChunksMap = initializeChunksMap(resultManifest, resultData, statementId);
    // We are assuming that all links provided are in sequence. Currently, it only returns the
    // first chunk link, and next links need to be fetched sequentially using a separate API
    this.nextChunkLinkToDownload = resultData.getExternalLinks().size();
    // No chunks are downloaded, we need to start from first one
    this.nextChunkToDownload = 0;
    // Initialize current chunk to -1, since we don't have anything to read
    this.currentChunkIndex = -1L;
    // We don't have any chunk in downloaded yet
    this.totalChunksInMemory = 0L;
    // Number of worker threads are directly linked to allowed chunks in memory
    this.allowedChunksInMemory = Math.min(CHUNKS_DOWNLOADER_THREAD_POOL_SIZE, totalChunks);
    this.chunkDownloaderExecutorService = createChunksDownloaderExecutorService();
    this.httpClient = httpClient;
    this.isClosed = false;
    // The first link is available
    this.downloadNextChunks();
  }

  private static ConcurrentHashMap<Long, ArrowResultChunk> initializeChunksMap(ResultManifest resultManifest, ResultData resultData, String statementId) {
    ConcurrentHashMap<Long, ArrowResultChunk> chunkIndexMap = new ConcurrentHashMap<>();
    for (ChunkInfo chunkInfo : resultManifest.getChunks()) {
      // TODO: Add logging to check data (in bytes) from server and in root allocator. If they are close, we can directly assign the number of bytes as the limit with a small buffer.
      chunkIndexMap.put(chunkInfo.getChunkIndex(), new ArrowResultChunk(chunkInfo, new RootAllocator(/*limit =*/ Integer.MAX_VALUE), statementId));
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
  public ArrowResultChunk getChunk() {
    if (currentChunkIndex < 0) {
      return null;
    }
    ArrowResultChunk chunk = chunkIndexToChunksMap.get(currentChunkIndex);
    synchronized (chunk) {
      try {
        while (!isDownloadComplete(chunk.getStatus())) {
          chunk.wait();
        }
      } catch (InterruptedException e) {
        // Handle interruption
      }
    }
    // TODO: check for errors
    return chunk;
  }

  boolean hasNextChunk() {
    return currentChunkIndex < totalChunks - 1;
  }

  boolean next() {
    if (!hasNextChunk()) {
      return false;
    }
    // go to next chunk
    currentChunkIndex++;
    if (currentChunkIndex > 0) {
      // release previous chunk
      releaseChunk();
    }
    return true;
  }

  private boolean isDownloadComplete(ArrowResultChunk.DownloadStatus status) {
    return status == ArrowResultChunk.DownloadStatus.DOWNLOAD_SUCCEEDED
        || status == ArrowResultChunk.DownloadStatus.DOWNLOAD_FAILED
        || status == ArrowResultChunk.DownloadStatus.DOWNLOAD_FAILED_ABORTED;
  }

  void downloadProcessed(long chunkIndex) {
    ArrowResultChunk chunk = chunkIndexToChunksMap.get(chunkIndex);
    synchronized (chunk) {
      chunk.notify();
    }
  }

  void downloadLinks(long chunkIndexToDownloadLink) {
    Collection<ExternalLink> chunks = session.getDatabricksClient().getResultChunks(
        statementId, chunkIndexToDownloadLink);
    for (ExternalLink chunkLink : chunks) {
      setChunkLink(chunkLink);
    }
  }

  /**
   * Release the memory for previous chunk since it is already consumed
   * @param chunkIndex index of consumed chunk
   */
  public void releaseChunk() {
    if (chunkIndexToChunksMap.get(currentChunkIndex - 1).releaseChunk()) {
      totalChunksInMemory--;
      downloadNextChunks();
    }
  }

  /**
   * Initialize chunk with external link details
   * @param chunkIndex index of chunk
   * @param chunkLink external link details for chunk
   */
  void setChunkLink(ExternalLink chunkLink) {
    chunkIndexToChunksMap.get(chunkLink.getChunkIndex()).setChunkUrl(chunkLink);
  }

  /**
   * Fetches total chunks that we have in memory
   */
  long getTotalChunksInMemory() {
    return totalChunksInMemory;
  }

  /**
   * Release all chunks from memory. This would be called when result-set has been closed.
   */
  void releaseAllChunks() {
    this.isClosed = true;
    this.chunkDownloaderExecutorService.shutdownNow();
    // TODO: release all chunks
  }

  void downloadNextChunks() {
    while (!this.isClosed && nextChunkToDownload < totalChunks && totalChunksInMemory < allowedChunksInMemory) {
      ArrowResultChunk chunk = chunkIndexToChunksMap.get(nextChunkToDownload);
      if (chunk.getStatus() != ArrowResultChunk.DownloadStatus.DOWNLOAD_SUCCEEDED) {
        this.chunkDownloaderExecutorService.submit(new SingleChunkDownloader(chunk, httpClient, this));
        totalChunksInMemory++;
      }
      nextChunkToDownload++;
    }
  }
}
