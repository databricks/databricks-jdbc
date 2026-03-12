package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.client.thrift.generated.TFetchResultsResp;
import com.databricks.jdbc.model.client.thrift.generated.TSparkArrowResultLink;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.sdk.service.sql.BaseChunkInfo;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class RemoteChunkProvider extends AbstractRemoteChunkProvider<ArrowResultChunk> {
  private static final String CHUNKS_DOWNLOADER_THREAD_POOL_PREFIX =
      "databricks-jdbc-chunks-downloader-";
  private ExecutorService chunkDownloaderExecutorService;

  RemoteChunkProvider(
      StatementId statementId,
      ResultManifest resultManifest,
      ResultData resultData,
      IDatabricksSession session,
      IDatabricksHttpClient httpClient,
      int chunksDownloaderThreadPoolSize)
      throws DatabricksSQLException {
    super(
        statementId,
        resultManifest,
        resultData,
        session,
        httpClient,
        chunksDownloaderThreadPoolSize,
        resultManifest.getResultCompression());
  }

  RemoteChunkProvider(
      IDatabricksStatementInternal parentStatement,
      TFetchResultsResp resultsResp,
      IDatabricksSession session,
      IDatabricksHttpClient httpClient,
      int chunksDownloaderThreadPoolSize,
      CompressionCodec compressionCodec)
      throws SQLException {
    super(
        parentStatement,
        resultsResp,
        session,
        httpClient,
        chunksDownloaderThreadPoolSize,
        compressionCodec);
  }

  /** {@inheritDoc} */
  @Override
  protected ArrowResultChunk createChunk(
      StatementId statementId, long chunkIndex, BaseChunkInfo chunkInfo)
      throws DatabricksSQLException {
    return ArrowResultChunk.builder()
        .withStatementId(statementId)
        .withChunkInfo(chunkInfo)
        .withChunkReadyTimeoutSeconds(chunkReadyTimeoutSeconds)
        .withConnectionContext(session.getConnectionContext())
        .build();
  }

  /** {@inheritDoc} */
  @Override
  protected ArrowResultChunk createChunk(
      StatementId statementId, long chunkIndex, TSparkArrowResultLink resultLink)
      throws DatabricksSQLException {
    return ArrowResultChunk.builder()
        .withStatementId(statementId)
        .withThriftChunkInfo(chunkIndex, resultLink)
        .withChunkReadyTimeoutSeconds(chunkReadyTimeoutSeconds)
        .withConnectionContext(session.getConnectionContext())
        .build();
  }

  /**
   * {@inheritDoc}
   *
   * <p>Downloads the next set of available chunks asynchronously using a thread pool executor. This
   * method:
   *
   * <ul>
   *   <li>Initializes a thread pool executor if not already created
   *   <li>Submits chunk download tasks to the executor while:
   *       <ul>
   *         <li>The provider is not closed
   *         <li>There are more chunks available to download
   *         <li>The number of chunks in memory is below the allowed limit
   *       </ul>
   *   <li>Tracks the total chunks in memory and the next chunk to download
   * </ul>
   *
   * Each chunk download is handled by a separate {@link ChunkDownloadTask} running in the executor
   * service. This implementation provides non-blocking downloads using a custom thread pool for
   * chunk downloads.
   */
  @Override
  public void downloadNextChunks() {
    if (chunkDownloaderExecutorService == null) {
      chunkDownloaderExecutorService = createChunksDownloaderExecutorService();
    }

    while (!isClosed
        && nextChunkToDownload < chunkCount
        && totalChunksInMemory < allowedChunksInMemory) {
      ArrowResultChunk chunk = chunkIndexToChunksMap.get(nextChunkToDownload);
      chunkDownloaderExecutorService.submit(
          new ChunkDownloadTask(chunk, httpClient, this, linkDownloadService));
      totalChunksInMemory++;
      nextChunkToDownload++;
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void doClose() {
    isClosed = true;
    chunkDownloaderExecutorService.shutdownNow();
    chunkIndexToChunksMap.values().forEach(ArrowResultChunk::releaseChunk);
    DatabricksThreadContextHolder.clearStatementInfo();
  }

  private ExecutorService createChunksDownloaderExecutorService() {
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
    return Executors.newFixedThreadPool(maxParallelChunkDownloadsPerQuery, threadFactory);
  }
}
