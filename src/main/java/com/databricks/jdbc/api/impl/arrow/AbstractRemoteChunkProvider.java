package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.common.util.DatabricksThriftUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.dbclient.impl.thrift.DatabricksThriftServiceClient;
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
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Abstract base implementation of both {@link ChunkProvider} and {@link ChunkDownloadManager}
 * interfaces.
 *
 * <p>The provider maintains a concurrent map of chunks and implements a sliding window approach or
 * memory management, releasing consumed chunks and downloading new ones as needed. It ensures that
 * the number of chunks in memory never exceeds the configured parallel download limit.
 *
 * @param <T> The specific type of AbstractArrowResultChunk this provider manages
 */
public abstract class AbstractRemoteChunkProvider<T extends AbstractArrowResultChunk>
    implements ChunkProvider, ChunkDownloadManager {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(AbstractRemoteChunkProvider.class);

  protected final IDatabricksSession session;
  protected final StatementId statementId;
  protected final IDatabricksHttpClient httpClient;
  protected final CompressionCodec compressionCodec;
  protected final ConcurrentMap<Long, T> chunkIndexToChunksMap;
  protected long chunkCount;
  protected long rowCount;
  protected long currentChunkIndex;
  protected long nextChunkToDownload;
  protected long totalChunksInMemory;
  protected long allowedChunksInMemory;
  protected boolean isClosed;

  /** Maximum number of parallel chunk downloads allowed per query. */
  protected final int maxParallelChunkDownloadsPerQuery;

  protected AbstractRemoteChunkProvider(
      StatementId statementId,
      ResultManifest resultManifest,
      ResultData resultData,
      IDatabricksSession session,
      IDatabricksHttpClient httpClient,
      int maxParallelChunkDownloadsPerQuery,
      CompressionCodec compressionCodec)
      throws DatabricksSQLException {
    this.maxParallelChunkDownloadsPerQuery = maxParallelChunkDownloadsPerQuery;
    this.session = session;
    this.httpClient = httpClient;
    this.statementId = statementId;
    this.compressionCodec = compressionCodec;
    this.chunkCount = resultManifest.getTotalChunkCount();
    this.rowCount = resultManifest.getTotalRowCount();
    this.chunkIndexToChunksMap = initializeChunksMap(resultManifest, resultData, statementId);

    initializeData();
  }

  protected AbstractRemoteChunkProvider(
      IDatabricksStatementInternal parentStatement,
      TFetchResultsResp resultsResp,
      IDatabricksSession session,
      IDatabricksHttpClient httpClient,
      int maxParallelChunkDownloadsPerQuery,
      CompressionCodec compressionCodec)
      throws DatabricksSQLException {
    this.maxParallelChunkDownloadsPerQuery = maxParallelChunkDownloadsPerQuery;
    this.session = session;
    this.httpClient = httpClient;
    this.statementId = parentStatement.getStatementId();
    this.compressionCodec = compressionCodec;
    this.chunkIndexToChunksMap = initializeChunksMap(resultsResp, parentStatement, session);

    initializeData();
  }

  /** Creates chunk {@link T} based on the {@link BaseChunkInfo}. Used in SQL Execution API flow. */
  protected abstract T createChunk(String statementId, long chunkIndex, BaseChunkInfo chunkInfo)
      throws DatabricksSQLException;

  /**
   * Creates chunk {@link T} based on the {@link TSparkArrowResultLink}. Used in Thrift CLI flow.
   */
  protected abstract T createChunk(
      String statementId, long chunkIndex, TSparkArrowResultLink resultLink)
      throws DatabricksSQLException;

  /** {@inheritDoc} */
  @Override
  public void downloadLinks(long chunkIndexToDownloadLink) throws DatabricksSQLException {
    Collection<ExternalLink> chunks =
        session.getDatabricksClient().getResultChunks(statementId, chunkIndexToDownloadLink);
    for (ExternalLink chunkLink : chunks) {
      setChunkLink(chunkLink);
    }
  }

  /** {@inheritDoc} */
  @Override
  public CompressionCodec getCompressionCodec() {
    return compressionCodec;
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasNextChunk() {
    return currentChunkIndex < chunkCount - 1;
  }

  @Override
  public long getRowCount() {
    return rowCount;
  }

  @Override
  public long getChunkCount() {
    return chunkCount;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Retrieves and waits for the current chunk to be ready.
   */
  @Override
  public T getChunk() throws DatabricksSQLException {
    if (currentChunkIndex < 0) {
      return null;
    }

    T chunk = chunkIndexToChunksMap.get(currentChunkIndex);

    try {
      chunk.waitForChunkReady();
    } catch (InterruptedException e) {
      LOGGER.error(
          e,
          "Caught interrupted exception while waiting for chunk [%s] for statement [%s]. Exception [%s]",
          chunk.getChunkIndex(),
          statementId,
          e.getMessage());
      Thread.currentThread().interrupt();
      throw new DatabricksSQLException("Operation interrupted while waiting for chunk ready", e);
    } catch (ExecutionException | TimeoutException e) {
      throw new DatabricksSQLException("Failed to ready chunk", e.getCause());
    }

    return chunk;
  }

  /** {@inheritDoc} */
  @Override
  public boolean next() throws DatabricksSQLException {
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

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  public long getAllowedChunksInMemory() {
    return allowedChunksInMemory;
  }

  private void initializeData() throws DatabricksSQLException {
    // No chunks are downloaded, we need to start from first one
    nextChunkToDownload = 0;
    // Initialize current chunk to -1, since we don't have anything to read
    currentChunkIndex = -1L;
    // We don't have any chunk in downloaded yet
    totalChunksInMemory = 0L;
    // Number of worker threads are directly linked to allowed chunks in memory
    allowedChunksInMemory = Math.min(maxParallelChunkDownloadsPerQuery, chunkCount);
    // The first link is available
    downloadNextChunks();
  }

  private void setChunkLink(ExternalLink chunkLink) {
    chunkIndexToChunksMap.get(chunkLink.getChunkIndex()).setChunkLink(chunkLink);
  }

  private ConcurrentMap<Long, T> initializeChunksMap(
      ResultManifest resultManifest, ResultData resultData, StatementId statementId)
      throws DatabricksSQLException {
    ConcurrentMap<Long, T> chunkIndexMap = new ConcurrentHashMap<>();
    if (resultManifest.getTotalChunkCount() == 0) {
      return chunkIndexMap;
    }

    for (BaseChunkInfo chunkInfo : resultManifest.getChunks()) {
      LOGGER.debug("Manifest chunk information: " + chunkInfo.toString());
      chunkIndexMap.put(
          chunkInfo.getChunkIndex(),
          createChunk(statementId.toString(), chunkInfo.getChunkIndex(), chunkInfo));
    }

    for (ExternalLink externalLink : resultData.getExternalLinks()) {
      chunkIndexMap.get(externalLink.getChunkIndex()).setChunkLink(externalLink);
    }

    return chunkIndexMap;
  }

  private ConcurrentMap<Long, T> initializeChunksMap(
      TFetchResultsResp resultsResp,
      IDatabricksStatementInternal parentStatement,
      IDatabricksSession session)
      throws DatabricksSQLException {
    ConcurrentMap<Long, T> chunkIndexMap = new ConcurrentHashMap<>();
    populateChunkIndexMap(resultsResp.getResults(), chunkIndexMap);
    while (resultsResp.hasMoreRows) {
      resultsResp =
          ((DatabricksThriftServiceClient) session.getDatabricksClient())
              .getMoreResults(parentStatement);
      populateChunkIndexMap(resultsResp.getResults(), chunkIndexMap);
    }

    return chunkIndexMap;
  }

  private void populateChunkIndexMap(TRowSet resultData, ConcurrentMap<Long, T> chunkIndexMap)
      throws DatabricksSQLException {
    rowCount += DatabricksThriftUtil.getRowCount(resultData);
    for (TSparkArrowResultLink resultLink : resultData.getResultLinks()) {
      LOGGER.debug(
          "Chunk information log - Row Offset: %s, Row Count: %s, Expiry Time: %s",
          resultLink.getStartRowOffset(), resultLink.getRowCount(), resultLink.getExpiryTime());
      chunkIndexMap.put(chunkCount, createChunk(statementId.toString(), chunkCount, resultLink));
      chunkCount++;
    }
  }

  /** Release the memory for previous chunk since it is already consumed */
  private void releaseChunk() throws DatabricksSQLException {
    if (chunkIndexToChunksMap.get(currentChunkIndex).releaseChunk()) {
      totalChunksInMemory--;
      downloadNextChunks();
    }
  }
}
