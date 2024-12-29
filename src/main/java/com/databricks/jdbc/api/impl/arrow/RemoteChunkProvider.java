package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.client.thrift.generated.TFetchResultsResp;
import com.databricks.jdbc.model.client.thrift.generated.TSparkArrowResultLink;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.sdk.service.sql.BaseChunkInfo;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;

/**
 * Original implementation of chunk provider that manages downloads using a thread pool executor.
 * Each chunk download is handled by a separate thread from a fixed-size thread pool.
 */
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
      throws DatabricksSQLException {
    super(
        parentStatement,
        resultsResp,
        session,
        httpClient,
        chunksDownloaderThreadPoolSize,
        compressionCodec);
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
      chunkDownloaderExecutorService.submit(new ChunkDownloadTask(chunk, httpClient, this));
      totalChunksInMemory++;
      nextChunkToDownload++;
    }
  }

  /** {@inheritDoc} */
  @Override
  protected ArrowResultChunk createChunk(
      String statementId, long chunkIndex, BaseChunkInfo chunkInfo)
      throws DatabricksParsingException {
    return ArrowResultChunk.builder().statementId(statementId).withChunkInfo(chunkInfo).build();
  }

  /** {@inheritDoc} */
  @Override
  protected ArrowResultChunk createChunk(
      String statementId, long chunkIndex, TSparkArrowResultLink resultLink)
      throws DatabricksParsingException {
    return ArrowResultChunk.builder()
        .statementId(statementId)
        .withThriftChunkInfo(chunkCount, resultLink)
        .build();
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    isClosed = true;
    chunkDownloaderExecutorService.shutdownNow();
    chunkIndexToChunksMap.values().forEach(ArrowResultChunk::releaseChunk);
  }

  private ExecutorService createChunksDownloaderExecutorService() {
    ThreadFactory threadFactory =
        new ThreadFactory() {
          private final AtomicInteger threadCount = new AtomicInteger(1);

          public Thread newThread(@Nonnull final Runnable r) {
            final Thread thread = new Thread(r);
            thread.setName(CHUNKS_DOWNLOADER_THREAD_POOL_PREFIX + threadCount.getAndIncrement());
            thread.setDaemon(true);
            return thread;
          }
        };
    return Executors.newFixedThreadPool(maxParallelChunkDownloadsPerQuery, threadFactory);
  }
}
