package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.common.util.DatabricksThriftUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.dbclient.impl.thrift.DatabricksThriftServiceClient;
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
import com.databricks.sdk.service.sql.BaseChunkInfo;
import com.google.common.annotations.VisibleForTesting;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/** Class to manage Arrow chunks and fetch them on proactive basis. */
public class RemoteChunkProviderV2 implements ChunkProvider, ChunkDownloadCallback {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(RemoteChunkProviderV2.class);
  private static int chunksDownloaderThreadPoolSize;
  private final IDatabricksSession session;
  private final StatementId statementId;
  private long chunkCount;
  private long rowCount;
  private final IDatabricksHttpClient httpClient;
  private Long currentChunkIndex;
  private long nextChunkToDownload;
  private Long totalChunksInMemory;
  private long allowedChunksInMemory;
  private boolean isClosed;
  private final CompressionCodec compressionCodec;
  private final ConcurrentHashMap<Long, ArrowResultChunkV2> chunkIndexToChunksMap;

  RemoteChunkProviderV2(
      StatementId statementId,
      ResultManifest resultManifest,
      ResultData resultData,
      IDatabricksSession session,
      IDatabricksHttpClient httpClient,
      int chunksDownloaderThreadPoolSize)
      throws DatabricksParsingException {
    RemoteChunkProviderV2.chunksDownloaderThreadPoolSize = chunksDownloaderThreadPoolSize;
    this.httpClient = httpClient;
    this.session = session;
    this.statementId = statementId;
    this.chunkCount = resultManifest.getTotalChunkCount();
    this.rowCount = resultManifest.getTotalRowCount();
    this.chunkIndexToChunksMap = initializeChunksMap(resultManifest, resultData, statementId);
    this.compressionCodec = resultManifest.getResultCompression();
    initializeData();
  }

  @VisibleForTesting
  RemoteChunkProviderV2(
      IDatabricksStatementInternal parentStatement,
      TFetchResultsResp resultsResp,
      IDatabricksSession session,
      IDatabricksHttpClient httpClient,
      int chunksDownloaderThreadPoolSize,
      CompressionCodec compressionCodec)
      throws DatabricksSQLException {
    RemoteChunkProviderV2.chunksDownloaderThreadPoolSize = chunksDownloaderThreadPoolSize;
    this.httpClient = httpClient;
    this.compressionCodec = compressionCodec;
    this.rowCount = 0;
    this.session = session;
    this.statementId = parentStatement.getStatementId();
    this.chunkIndexToChunksMap = initializeChunksMap(resultsResp, parentStatement, session);
    initializeData();
  }

  /** {@inheritDoc} */
  @Override
  public void downloadProcessed(long chunkIndex) {
    ArrowResultChunkV2 chunk = chunkIndexToChunksMap.get(chunkIndex);
    synchronized (chunk) {
      chunk.notify();
    }
  }

  /** {@inheritDoc} */
  @Override
  public void downloadLinks(long chunkIndexToDownloadLink) throws DatabricksSQLException {
    Collection<ExternalLink> chunks =
        session.getDatabricksClient().getResultChunks(statementId, chunkIndexToDownloadLink);
    for (ExternalLink chunkLink : chunks) {
      setChunkLink(chunkLink);
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
  public ArrowResultChunkV2 getChunk() throws DatabricksSQLException {
    if (currentChunkIndex < 0) {
      return null;
    }
    ArrowResultChunkV2 chunk = chunkIndexToChunksMap.get(currentChunkIndex);
    try {
      chunk.waitForDownload();
      if (chunk.getStatus() != ArrowResultChunkV2.ChunkStatus.DOWNLOAD_SUCCEEDED) {
        throw new DatabricksSQLException(chunk.getErrorMessage());
      }
      String context =
          String.format(
              "Data decompression for chunk index [%d] and statement [%s]",
              chunk.getChunkIndex(), chunk.getStatementId());
      long startTime = System.currentTimeMillis();
      chunk.processArrowData(getCompressionCodec(), context);
      long endTime = System.currentTimeMillis();
      LOGGER.debug(
          "Data decompression for chunk index [%d] and statement [%s] took [%d] ms",
          chunk.getChunkIndex(), chunk.getStatementId(), endTime - startTime);
    } catch (InterruptedException e) {
      LOGGER.error(
          e,
          "Caught interrupted exception while waiting for chunk [%s] for statement [%s]. Exception [%s]",
          chunk.getChunkIndex(),
          statementId,
          e.getMessage());
      Thread.currentThread().interrupt();
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
    this.chunkIndexToChunksMap.values().forEach(ArrowResultChunkV2::releaseChunk);
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

  void downloadNextChunks() {
    while (!this.isClosed
        && nextChunkToDownload < chunkCount
        && totalChunksInMemory < allowedChunksInMemory) {
      ArrowResultChunkV2 chunk = chunkIndexToChunksMap.get(nextChunkToDownload);
      if (chunk.getStatus() != ArrowResultChunkV2.ChunkStatus.DOWNLOAD_SUCCEEDED) {
        totalChunksInMemory++;
        if (chunk.isChunkLinkInvalid()) {
          try {
            downloadLinks(chunk.getChunkIndex());
          } catch (DatabricksSQLException e) {
            throw new RuntimeException(e);
          }
        }
        chunk.downloadDataAsync(httpClient, this);
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
    this.allowedChunksInMemory = Math.min(chunksDownloaderThreadPoolSize, chunkCount);
    this.isClosed = false;
    // The first link is available
    this.downloadNextChunks();
  }

  private ConcurrentHashMap<Long, ArrowResultChunkV2> initializeChunksMap(
      TFetchResultsResp resultsResp,
      IDatabricksStatementInternal parentStatement,
      IDatabricksSession session)
      throws DatabricksSQLException {
    ConcurrentHashMap<Long, ArrowResultChunkV2> chunkIndexMap = new ConcurrentHashMap<>();
    this.chunkCount = 0;
    this.rowCount = 0;
    populateChunkIndexMap(resultsResp.getResults(), chunkIndexMap);
    while (resultsResp.hasMoreRows) {
      resultsResp =
          ((DatabricksThriftServiceClient) session.getDatabricksClient())
              .getMoreResults(parentStatement);
      populateChunkIndexMap(resultsResp.getResults(), chunkIndexMap);
    }
    return chunkIndexMap;
  }

  private void populateChunkIndexMap(
      TRowSet resultData, ConcurrentHashMap<Long, ArrowResultChunkV2> chunkIndexMap)
      throws DatabricksParsingException {
    rowCount += DatabricksThriftUtil.getRowCount(resultData);
    for (TSparkArrowResultLink resultLink : resultData.getResultLinks()) {
      String chunkInformationLog =
          String.format(
              "Chunk information log - Row Offset: %s, Row Count: %s, Expiry Time: %s",
              resultLink.getStartRowOffset(), resultLink.getRowCount(), resultLink.getExpiryTime());
      LOGGER.debug(chunkInformationLog);
      chunkIndexMap.put(
          chunkCount,
          ArrowResultChunkV2.builder()
              .statementId(statementId.toString())
              .withThriftChunkInfo(chunkCount, resultLink)
              .build());
      this.chunkCount++;
    }
  }

  private static ConcurrentHashMap<Long, ArrowResultChunkV2> initializeChunksMap(
      ResultManifest resultManifest, ResultData resultData, StatementId statementId)
      throws DatabricksParsingException {
    ConcurrentHashMap<Long, ArrowResultChunkV2> chunkIndexMap = new ConcurrentHashMap<>();
    if (resultManifest.getTotalChunkCount() == 0) {
      return chunkIndexMap;
    }
    for (BaseChunkInfo chunkInfo : resultManifest.getChunks()) {
      LOGGER.debug("Manifest chunk information: " + chunkInfo.toString());
      chunkIndexMap.put(
          chunkInfo.getChunkIndex(),
          ArrowResultChunkV2.builder()
              .statementId(statementId.toString())
              .withChunkInfo(chunkInfo)
              .build());
    }

    for (ExternalLink externalLink : resultData.getExternalLinks()) {
      chunkIndexMap.get(externalLink.getChunkIndex()).setChunkLink(externalLink);
    }
    return chunkIndexMap;
  }

  private boolean isDownloadComplete(ArrowResultChunkV2.ChunkStatus status) {
    return status == ArrowResultChunkV2.ChunkStatus.DOWNLOAD_SUCCEEDED
        || status == ArrowResultChunkV2.ChunkStatus.DOWNLOAD_FAILED
        || status == ArrowResultChunkV2.ChunkStatus.DOWNLOAD_FAILED_ABORTED;
  }
}
