package com.databricks.jdbc.core;

import com.databricks.jdbc.client.IDatabricksHttpClient;
import com.databricks.jdbc.client.http.DatabricksHttpClient;
import com.databricks.jdbc.client.impl.thrift.generated.TRowSet;
import com.databricks.jdbc.client.impl.thrift.generated.TSparkArrowResultLink;
import com.databricks.jdbc.client.sqlexec.ExternalLink;
import com.databricks.jdbc.client.sqlexec.ResultData;
import com.databricks.jdbc.client.sqlexec.ResultManifest;
import com.databricks.jdbc.core.types.CompressionType;
import com.databricks.sdk.service.sql.BaseChunkInfo;
import com.google.common.annotations.VisibleForTesting;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Class to manage Arrow chunks and fetch them on proactive basis. */
public class ChunkDownloader {

  private static final Logger logger = LogManager.getLogger(ChunkDownloader.class);
  private static final String CHUNKS_DOWNLOADER_THREAD_POOL_PREFIX =
      "databricks-jdbc-chunks-downloader-";
  private final IDatabricksSession session;
  private final String statementId;
  private final long totalChunks;
  private final ExecutorService chunkDownloaderExecutorService;
  private final IDatabricksHttpClient httpClient;
  private static int chunksDownloaderThreadPoolSize;
  private Long currentChunkIndex;
  private long nextChunkToDownload;
  private Long totalChunksInMemory;
  private long allowedChunksInMemory;
  private boolean isClosed;

  ConcurrentHashMap<Long, ArrowResultChunk> chunkIndexToChunksMap;

  ChunkDownloader(
      String statementId,
      ResultManifest resultManifest,
      ResultData resultData,
      IDatabricksSession session,
      int chunksDownloaderThreadPoolSize) {
    this(
        statementId,
        resultManifest,
        resultData,
        session,
        DatabricksHttpClient.getInstance(session.getConnectionContext()),
        chunksDownloaderThreadPoolSize);
  }

  @VisibleForTesting
  ChunkDownloader(
      String statementId,
      ResultManifest resultManifest,
      ResultData resultData,
      IDatabricksSession session,
      IDatabricksHttpClient httpClient,
      int chunksDownloaderThreadPoolSize) {
    this.chunksDownloaderThreadPoolSize = chunksDownloaderThreadPoolSize;
    this.chunkDownloaderExecutorService = createChunksDownloaderExecutorService();
    this.httpClient = httpClient;
    this.session = session;
    this.statementId = statementId;
    this.totalChunks = resultManifest.getTotalChunkCount();
    this.chunkIndexToChunksMap = initializeChunksMap(resultManifest, resultData, statementId);
    initializeData();
  }

  ChunkDownloader(
      String statementId,
      TRowSet resultData,
      IDatabricksSession session,
      int chunksDownloaderThreadPoolSize) {
    this(
        statementId,
        resultData,
        session,
        DatabricksHttpClient.getInstance(session.getConnectionContext()),
        chunksDownloaderThreadPoolSize);
  }

  @VisibleForTesting
  ChunkDownloader(
      String statementId,
      TRowSet resultData,
      IDatabricksSession session,
      IDatabricksHttpClient httpClient,
      int chunksDownloaderThreadPoolSize) {
    this.chunksDownloaderThreadPoolSize = chunksDownloaderThreadPoolSize;
    this.chunkDownloaderExecutorService = createChunksDownloaderExecutorService();
    this.httpClient = httpClient;
    this.session = session;
    this.statementId = statementId;
    this.totalChunks = resultData.getResultLinksSize();
    this.chunkIndexToChunksMap = initializeChunksMap(resultData, statementId);
    initializeData();
  }

  private static ConcurrentHashMap<Long, ArrowResultChunk> initializeChunksMap(
      TRowSet resultData, String statementId) {
    ConcurrentHashMap<Long, ArrowResultChunk> chunkIndexMap = new ConcurrentHashMap<>();
    long chunkIndex = 0;
    if (resultData.getResultLinksSize() == 0) {
      return chunkIndexMap;
    }
    for (TSparkArrowResultLink resultLink : resultData.getResultLinks()) {
      // TODO : add compression
      chunkIndexMap.put(
          chunkIndex,
          new ArrowResultChunk(chunkIndex, resultLink, statementId, CompressionType.NONE));
      chunkIndex++;
    }
    return chunkIndexMap;
  }

  private static ExecutorService createChunksDownloaderExecutorService() {
    ThreadFactory threadFactory =
        new ThreadFactory() {
          private AtomicInteger threadCount = new AtomicInteger(1);

          public Thread newThread(final Runnable r) {
            final Thread thread = new Thread(r);
            thread.setName(CHUNKS_DOWNLOADER_THREAD_POOL_PREFIX + threadCount.getAndIncrement());
            thread.setDaemon(true);
            return thread;
          }
        };
    return Executors.newFixedThreadPool(chunksDownloaderThreadPoolSize, threadFactory);
  }

  /**
   * Fetches the chunk for the given index. If chunk is not already downloaded, will download the
   * chunk first
   *
   * @return the chunk at given index
   */
  public ArrowResultChunk getChunk() throws DatabricksSQLException {
    if (currentChunkIndex < 0) {
      return null;
    }
    ArrowResultChunk chunk = chunkIndexToChunksMap.get(currentChunkIndex);
    httpClient.closeExpiredAndIdleConnections();
    synchronized (chunk) {
      try {
        while (!isDownloadComplete(chunk.getStatus())) {
          chunk.wait();
        }
        if (chunk.getStatus() != ArrowResultChunk.ChunkStatus.DOWNLOAD_SUCCEEDED) {
          throw new DatabricksSQLException(chunk.getErrorMessage());
        }
      } catch (InterruptedException e) {
        logger.info(
            "Caught interrupted exception while waiting for chunk [%s] for statement [%s]. Exception [%s]",
            chunk.getChunkIndex(), statementId, e);
      }
    }

    return chunk;
  }

  boolean hasNextChunk() {
    return currentChunkIndex < totalChunks - 1;
  }

  boolean next() {
    if (currentChunkIndex >= 0) {
      // release current chunk
      releaseChunk();
    }
    if (!hasNextChunk()) {
      return false;
    }
    // go to next chunk
    currentChunkIndex++;
    return true;
  }

  private boolean isDownloadComplete(ArrowResultChunk.ChunkStatus status) {
    return status == ArrowResultChunk.ChunkStatus.DOWNLOAD_SUCCEEDED
        || status == ArrowResultChunk.ChunkStatus.DOWNLOAD_FAILED
        || status == ArrowResultChunk.ChunkStatus.DOWNLOAD_FAILED_ABORTED;
  }

  void downloadProcessed(long chunkIndex) {
    ArrowResultChunk chunk = chunkIndexToChunksMap.get(chunkIndex);
    synchronized (chunk) {
      chunk.notify();
    }
  }

  void downloadLinks(long chunkIndexToDownloadLink) throws DatabricksSQLException {
    Collection<ExternalLink> chunks =
        session.getDatabricksClient().getResultChunks(statementId, chunkIndexToDownloadLink);
    for (ExternalLink chunkLink : chunks) {
      setChunkLink(chunkLink);
    }
  }

  /** Release the memory for previous chunk since it is already consumed */
  public void releaseChunk() {
    if (chunkIndexToChunksMap.get(currentChunkIndex).releaseChunk()) {
      totalChunksInMemory--;
      downloadNextChunks();
    }
  }

  /**
   * Initialize chunk with external link details
   *
   * @param chunkLink external link details for chunk
   */
  void setChunkLink(ExternalLink chunkLink) {
    if (!isDownloadComplete(chunkIndexToChunksMap.get(chunkLink.getChunkIndex()).getStatus())) {
      chunkIndexToChunksMap.get(chunkLink.getChunkIndex()).setChunkLink(chunkLink);
    }
  }

  /** Fetches total chunks that we have in memory */
  long getTotalChunksInMemory() {
    return totalChunksInMemory;
  }

  /** Release all chunks from memory. This would be called when result-set has been closed. */
  void releaseAllChunks() {
    this.isClosed = true;
    this.chunkDownloaderExecutorService.shutdownNow();
    this.chunkIndexToChunksMap.values().forEach(chunk -> chunk.releaseChunk());
    httpClient.closeExpiredAndIdleConnections();
  }

  void downloadNextChunks() {
    while (!this.isClosed
        && nextChunkToDownload < totalChunks
        && totalChunksInMemory < allowedChunksInMemory) {
      ArrowResultChunk chunk = chunkIndexToChunksMap.get(nextChunkToDownload);
      if (chunk.getStatus() != ArrowResultChunk.ChunkStatus.DOWNLOAD_SUCCEEDED) {
        this.chunkDownloaderExecutorService.submit(
            new SingleChunkDownloader(chunk, httpClient, this));
        totalChunksInMemory++;
      }
      nextChunkToDownload++;
    }
  }

  void initializeData() {
    // No chunks are downloaded, we need to start from first one
    this.nextChunkToDownload = 0;
    // Initialize current chunk to -1, since we don't have anything to read
    this.currentChunkIndex = -1L;
    // We don't have any chunk in downloaded yet
    this.totalChunksInMemory = 0L;
    // Number of worker threads are directly linked to allowed chunks in memory
    this.allowedChunksInMemory = Math.min(chunksDownloaderThreadPoolSize, totalChunks);
    this.isClosed = false;
    // The first link is available
    this.downloadNextChunks();
  }

  private static ConcurrentHashMap<Long, ArrowResultChunk> initializeChunksMap(
      ResultManifest resultManifest, ResultData resultData, String statementId) {
    ConcurrentHashMap<Long, ArrowResultChunk> chunkIndexMap = new ConcurrentHashMap<>();
    if (resultManifest.getTotalChunkCount() == 0) {
      return chunkIndexMap;
    }
    for (BaseChunkInfo chunkInfo : resultManifest.getChunks()) {
      // TODO: Add logging to check data (in bytes) from server and in root allocator.
      //  If they are close, we can directly assign the number of bytes as the limit with a small
      // buffer.
      chunkIndexMap.put(
          chunkInfo.getChunkIndex(),
          new ArrowResultChunk(chunkInfo, statementId, resultManifest.getCompressionType()));
    }

    for (ExternalLink externalLink : resultData.getExternalLinks()) {
      chunkIndexMap.get(externalLink.getChunkIndex()).setChunkLink(externalLink);
    }
    return chunkIndexMap;
  }
}
