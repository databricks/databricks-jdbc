package com.databricks.jdbc.api.impl.arrow.incubator;

import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.api.impl.arrow.AbstractRemoteChunkProvider;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.client.thrift.generated.TFetchResultsResp;
import com.databricks.jdbc.model.client.thrift.generated.TSparkArrowResultLink;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.sdk.service.sql.BaseChunkInfo;

/**
 * A V2 implementation of chunk provider that handles chunk downloads using Apache's async HTTP
 * client. Each chunk is downloaded sequentially but processed asynchronously through streaming
 * responses.
 */
public class RemoteChunkProviderV2 extends AbstractRemoteChunkProvider<ArrowResultChunkV2> {

  public RemoteChunkProviderV2(
      StatementId statementId,
      ResultManifest resultManifest,
      ResultData resultData,
      IDatabricksSession session,
      IDatabricksHttpClient httpClient,
      int maxParallelChunkDownloadsPerQuery)
      throws DatabricksSQLException {
    super(
        statementId,
        resultManifest,
        resultData,
        session,
        httpClient,
        maxParallelChunkDownloadsPerQuery,
        resultManifest.getResultCompression());
  }

  public RemoteChunkProviderV2(
      IDatabricksStatementInternal parentStatement,
      TFetchResultsResp resultsResp,
      IDatabricksSession session,
      IDatabricksHttpClient httpClient,
      int maxParallelChunkDownloadsPerQuery,
      CompressionCodec compressionCodec)
      throws DatabricksSQLException {
    super(
        parentStatement,
        resultsResp,
        session,
        httpClient,
        maxParallelChunkDownloadsPerQuery,
        compressionCodec);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Downloads the next set of available chunks using Apache's async HTTP client. This method
   * processes chunks sequentially but each chunk download is performed asynchronously. For each
   * chunk, it:
   *
   * <ul>
   *   <li>Checks if the provider is not closed
   *   <li>Verifies more chunks are available to download
   *   <li>Ensures the number of chunks in memory is below the allowed limit
   *   <li>Downloads the chunk data if these conditions are met
   * </ul>
   *
   * The actual download is performed using {@link ArrowResultChunkV2}'s streaming response
   * consumer, which handles:
   *
   * <ul>
   *   <li>Asynchronous data streaming
   *   <li>Automatic retries with exponential backoff
   *   <li>Download statistics tracking
   *   <li>Memory-efficient data processing
   * </ul>
   *
   * @throws DatabricksSQLException If there's an error during the chunk download process, including
   *     errors in downloading links or chunk data
   */
  @Override
  public void downloadNextChunks() throws DatabricksSQLException {
    while (!isClosed
        && nextChunkToDownload < chunkCount
        && totalChunksInMemory < allowedChunksInMemory) {
      ArrowResultChunkV2 chunk = chunkIndexToChunksMap.get(nextChunkToDownload);
      totalChunksInMemory++;
      if (chunk.isChunkLinkInvalid()) {
        downloadLinks(chunk.getChunkIndex());
      }
      chunk.downloadData(httpClient, getCompressionCodec());
      nextChunkToDownload++;
    }
  }

  /** {@inheritDoc} */
  @Override
  protected ArrowResultChunkV2 createChunk(
      String statementId, long chunkIndex, BaseChunkInfo chunkInfo) {
    return ArrowResultChunkV2.builder().statementId(statementId).withChunkInfo(chunkInfo).build();
  }

  /** {@inheritDoc} */
  @Override
  protected ArrowResultChunkV2 createChunk(
      String statementId, long chunkIndex, TSparkArrowResultLink resultLink) {
    return ArrowResultChunkV2.builder()
        .statementId(statementId)
        .withThriftChunkInfo(chunkCount, resultLink)
        .build();
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    isClosed = true;
    chunkIndexToChunksMap.values().forEach(ArrowResultChunkV2::releaseChunk);
  }
}
