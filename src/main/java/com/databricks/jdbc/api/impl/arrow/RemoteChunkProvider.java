package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.common.util.DatabricksThriftUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.TFetchResultsResp;
import com.databricks.jdbc.model.client.thrift.generated.TRowSet;
import com.databricks.jdbc.model.client.thrift.generated.TSparkArrowResultLink;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.service.sql.BaseChunkInfo;
import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/** Class to manage Arrow chunks and fetch them on proactive basis. */
public class RemoteChunkProvider implements ChunkProvider, ChunkDownloadCallback {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(RemoteChunkProvider.class);
  private static final String CHUNKS_DOWNLOADER_THREAD_POOL_PREFIX =
      "databricks-jdbc-chunks-downloader-";
  private static int chunksDownloaderThreadPoolSize;
  private final StatementId statementId;
  private long chunkCount;
  private long rowCount;
  private final ExecutorService chunkDownloaderExecutorService;
  private final IDatabricksHttpClient httpClient;
  private Long currentChunkIndex;
  private long nextChunkToDownload;
  private Long totalChunksInMemory;
  private long allowedChunksInMemory;
  private boolean isClosed;
  private final CompressionCodec compressionCodec;
  private final ConcurrentHashMap<Long, ArrowResultChunk> chunkIndexToChunksMap;
  private final ChunkLinkDownloadService linkDownloadService;

  RemoteChunkProvider(
      StatementId statementId,
      ResultManifest resultManifest,
      ResultData resultData,
      IDatabricksSession session,
      IDatabricksHttpClient httpClient,
      int chunksDownloaderThreadPoolSize)
      throws DatabricksParsingException {
    RemoteChunkProvider.chunksDownloaderThreadPoolSize = chunksDownloaderThreadPoolSize;
    this.chunkDownloaderExecutorService = createChunksDownloaderExecutorService();
    this.httpClient = httpClient;
    this.statementId = statementId;
    this.chunkCount = resultManifest.getTotalChunkCount();
    this.rowCount = resultManifest.getTotalRowCount();
    this.chunkIndexToChunksMap = initializeChunksMap(resultManifest, resultData, statementId);
    this.compressionCodec = resultManifest.getResultCompression();
    this.linkDownloadService =
        new ChunkLinkDownloadService(
            session,
            statementId,
            chunkCount,
            chunkIndexToChunksMap,
            resultData.getExternalLinks() != null ? resultData.getExternalLinks().size() : 1);
    initializeData();
  }

  @VisibleForTesting
  RemoteChunkProvider(
      IDatabricksStatementInternal parentStatement,
      TFetchResultsResp resultsResp,
      IDatabricksSession session,
      IDatabricksHttpClient httpClient,
      int chunksDownloaderThreadPoolSize,
      CompressionCodec compressionCodec)
      throws DatabricksSQLException {
    RemoteChunkProvider.chunksDownloaderThreadPoolSize = chunksDownloaderThreadPoolSize;
    this.chunkDownloaderExecutorService = createChunksDownloaderExecutorService();
    this.httpClient = httpClient;
    this.compressionCodec = compressionCodec;
    this.rowCount = 0;
    this.statementId = parentStatement.getStatementId();
    this.chunkIndexToChunksMap = initializeChunksMap(resultsResp, parentStatement, session);
    this.linkDownloadService =
        new ChunkLinkDownloadService(
            session, statementId, chunkCount, chunkIndexToChunksMap, chunkCount);
    initializeData();
  }

  /** {@inheritDoc} */
  @Override
  public void downloadProcessed(long chunkIndex) {
    ArrowResultChunk chunk = chunkIndexToChunksMap.get(chunkIndex);
    synchronized (chunk) {
      chunk.notify();
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Fetches the chunk for the given index. If chunk is not already downloaded, will download the
   * chunk first
   *
   * @return the chunk at given index
   */
  @Override
  public ArrowResultChunk getChunk() throws DatabricksSQLException {
    if (currentChunkIndex < 0) {
      return null;
    }
    ArrowResultChunk chunk = chunkIndexToChunksMap.get(currentChunkIndex);
    synchronized (chunk) {
      try {
        while (!isDownloadComplete(chunk.getStatus())) {
          chunk.wait();
        }
        if (chunk.getStatus() != ArrowResultChunk.ChunkStatus.DOWNLOAD_SUCCEEDED) {
          throw new DatabricksSQLException(
              chunk.getErrorMessage(), DatabricksDriverErrorCode.CHUNK_DOWNLOAD_ERROR);
        }
      } catch (InterruptedException e) {
        LOGGER.error(
            e,
            "Caught interrupted exception while waiting for chunk [%s] for statement [%s]. Exception [%s]",
            chunk.getChunkIndex(),
            statementId,
            e.getMessage());
      }
    }

    return chunk;
  }

  @Override
  public CompressionCodec getCompressionCodec() {
    return compressionCodec;
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasNextChunk() {
    return currentChunkIndex < chunkCount - 1;
  }

  /** {@inheritDoc} */
  @Override
  public boolean next() {
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

  /**
   * {@inheritDoc}
   *
   * <p>Release all chunks from memory. This would be called when result-set has been closed.
   */
  @Override
  public void close() {
    this.isClosed = true;
    this.linkDownloadService.shutdown();
    this.chunkDownloaderExecutorService.shutdownNow();
    this.chunkIndexToChunksMap.values().forEach(ArrowResultChunk::releaseChunk);
    DatabricksThreadContextHolder.clearStatementInfo();
  }

  @Override
  public long getRowCount() {
    return rowCount;
  }

  @Override
  public long getChunkCount() {
    return chunkCount;
  }

  /** Release the memory for previous chunk since it is already consumed */
  void releaseChunk() {
    if (chunkIndexToChunksMap.get(currentChunkIndex).releaseChunk()) {
      totalChunksInMemory--;
      downloadNextChunks();
    }
  }

  void downloadNextChunks() {
    while (!this.isClosed
        && nextChunkToDownload < chunkCount
        && totalChunksInMemory < allowedChunksInMemory) {
      ArrowResultChunk chunk = chunkIndexToChunksMap.get(nextChunkToDownload);
      if (chunk.getStatus() != ArrowResultChunk.ChunkStatus.DOWNLOAD_SUCCEEDED) {
        this.chunkDownloaderExecutorService.submit(
            new ChunkDownloadTask(chunk, httpClient, this, linkDownloadService));
        totalChunksInMemory++;
      }
      nextChunkToDownload++;
    }
  }

  void initializeData() {
    DatabricksThreadContextHolder.setStatementId(statementId);
    // No chunks are downloaded, we need to start from first one
    this.nextChunkToDownload = 0;
    // Initialize current chunk to -1, since we don't have anything to read
    this.currentChunkIndex = -1L;
    // We don't have any chunk in downloaded yet
    this.totalChunksInMemory = 0L;
    // Number of worker threads are directly linked to allowed chunks in memory
    this.allowedChunksInMemory = Math.min(chunksDownloaderThreadPoolSize, chunkCount);
    this.isClosed = false;
    // The first link is available
    this.downloadNextChunks();
  }

  private ConcurrentHashMap<Long, ArrowResultChunk> initializeChunksMap(
      TFetchResultsResp resultsResp,
      IDatabricksStatementInternal parentStatement,
      IDatabricksSession session)
      throws DatabricksSQLException {
    ConcurrentHashMap<Long, ArrowResultChunk> chunkIndexMap = new ConcurrentHashMap<>();
    this.chunkCount = 0;
    this.rowCount = 0;
    populateChunkIndexMap(resultsResp.getResults(), chunkIndexMap);
    while (resultsResp.hasMoreRows) {
      resultsResp = session.getDatabricksClient().getMoreResults(parentStatement);
      populateChunkIndexMap(resultsResp.getResults(), chunkIndexMap);
    }
    return chunkIndexMap;
  }

  private void populateChunkIndexMap(
      TRowSet resultData, ConcurrentHashMap<Long, ArrowResultChunk> chunkIndexMap)
      throws DatabricksSQLException {
    rowCount += DatabricksThriftUtil.getRowCount(resultData);
    for (TSparkArrowResultLink resultLink : resultData.getResultLinks()) {
      String chunkInformationLog =
          String.format(
              "Chunk information log - Row Offset: %s, Row Count: %s, Expiry Time: %s",
              resultLink.getStartRowOffset(), resultLink.getRowCount(), resultLink.getExpiryTime());
      LOGGER.debug(chunkInformationLog);
      chunkIndexMap.put(
          chunkCount,
          ArrowResultChunk.builder()
              .statementId(statementId.toString())
              .withThriftChunkInfo(chunkCount, resultLink)
              .build());
      this.chunkCount++;
    }
  }

  private static ExecutorService createChunksDownloaderExecutorService() {
    ThreadFactory threadFactory =
        new ThreadFactory() {
          private final AtomicInteger threadCount = new AtomicInteger(1);

          public Thread newThread(final Runnable r) {
            final Thread thread = new Thread(r);
            thread.setName(CHUNKS_DOWNLOADER_THREAD_POOL_PREFIX + threadCount.getAndIncrement());
            thread.setDaemon(true);
            return thread;
          }
        };
    return Executors.newFixedThreadPool(chunksDownloaderThreadPoolSize, threadFactory);
  }

  private static ConcurrentHashMap<Long, ArrowResultChunk> initializeChunksMap(
      ResultManifest resultManifest, ResultData resultData, StatementId statementId)
      throws DatabricksParsingException {
    ConcurrentHashMap<Long, ArrowResultChunk> chunkIndexMap = new ConcurrentHashMap<>();
    if (resultManifest.getTotalChunkCount() == 0) {
      return chunkIndexMap;
    }
    for (BaseChunkInfo chunkInfo : resultManifest.getChunks()) {
      LOGGER.debug("Manifest chunk information: " + chunkInfo.toString());
      chunkIndexMap.put(
          chunkInfo.getChunkIndex(),
          ArrowResultChunk.builder()
              .statementId(statementId.toString())
              .withChunkInfo(chunkInfo)
              .build());
    }

    for (ExternalLink externalLink : resultData.getExternalLinks()) {
      chunkIndexMap.get(externalLink.getChunkIndex()).setChunkLink(externalLink);
    }
    return chunkIndexMap;
  }

  private boolean isDownloadComplete(ArrowResultChunk.ChunkStatus status) {
    return status == ArrowResultChunk.ChunkStatus.DOWNLOAD_SUCCEEDED
        || status == ArrowResultChunk.ChunkStatus.DOWNLOAD_FAILED
        || status == ArrowResultChunk.ChunkStatus.DOWNLOAD_FAILED_ABORTED;
  }
}
