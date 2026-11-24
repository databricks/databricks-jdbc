package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.common.util.ArrowUtil.getColumnInfoList;
import static com.databricks.jdbc.common.util.DatabricksThriftUtil.createExternalLink;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.TFetchResultsResp;
import com.databricks.jdbc.model.client.thrift.generated.TSparkArrowResultLink;
import com.databricks.jdbc.model.core.ChunkLinkFetchResult;
import com.databricks.jdbc.model.core.ColumnInfo;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.google.common.annotations.VisibleForTesting;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** Result container for Arrow-based query results. */
public class ArrowStreamResult extends AbstractArrowResult {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ArrowStreamResult.class);

  public ArrowStreamResult(
      ResultManifest resultManifest,
      ResultData resultData,
      StatementId statementId,
      IDatabricksSession session)
      throws DatabricksSQLException {
    this(
        resultManifest,
        resultData,
        statementId,
        session,
        DatabricksHttpClientFactory.getInstance().getClient(session.getConnectionContext()));
  }

  @VisibleForTesting
  ArrowStreamResult(
      ResultManifest resultManifest,
      ResultData resultData,
      StatementId statementId,
      IDatabricksSession session,
      IDatabricksHttpClient httpClient)
      throws DatabricksSQLException {
    this(
        createSqlExecChunkProvider(resultManifest, resultData, statementId, session, httpClient),
        resultManifest,
        session);
  }

  private ArrowStreamResult(
      ChunkProvider chunkProvider, ResultManifest resultManifest, IDatabricksSession session) {
    super(
        chunkProvider,
        resultManifest.getSchema().getColumnCount() == 0
            ? new ArrayList<>()
            : new ArrayList<>(resultManifest.getSchema().getColumns()),
        session);
  }

  private static ChunkProvider createSqlExecChunkProvider(
      ResultManifest resultManifest,
      ResultData resultData,
      StatementId statementId,
      IDatabricksSession session,
      IDatabricksHttpClient httpClient)
      throws DatabricksSQLException {
    Long totalChunkCount = resultManifest.getTotalChunkCount();
    if (totalChunkCount != null && totalChunkCount == 0) {
      LOGGER.debug(
          "Empty result (total_chunk_count=0) for statementId: {}, skipping chunk fetching",
          statementId.toSQLExecStatementId());
      return new EmptyChunkProvider();
    }

    boolean isInlineArrow = resultData.getAttachment() != null;
    IDatabricksConnectionContext connectionContext = session.getConnectionContext();
    if (isInlineArrow && connectionContext.isBoundedSeaApiEnabled()) {
      LOGGER.debug(
          "Creating ArrowStreamResult with SEA inline Arrow for statementId: {}",
          statementId.toSQLExecStatementId());
      return new SeaInlineArrowChunkProvider(resultData, resultManifest, statementId, session);
    }
    if (isInlineArrow) {
      LOGGER.debug(
          "Creating ArrowStreamResult with inline attachment for statementId: {}",
          statementId.toSQLExecStatementId());
      return new InlineChunkProvider(resultData, resultManifest);
    }
    LOGGER.debug(
        "Creating ArrowStreamResult with remote links for statementId: {}",
        statementId.toSQLExecStatementId());
    return createRemoteChunkProvider(statementId, resultManifest, resultData, session, httpClient);
  }

  /**
   * Creates the appropriate remote chunk provider based on configuration.
   *
   * @param statementId The statement ID
   * @param resultManifest The result manifest containing chunk metadata
   * @param resultData The result data containing initial external links
   * @param session The session for fetching additional chunks
   * @param httpClient The HTTP client for downloading chunk data
   * @return A ChunkProvider instance
   */
  private static ChunkProvider createRemoteChunkProvider(
      StatementId statementId,
      ResultManifest resultManifest,
      ResultData resultData,
      IDatabricksSession session,
      IDatabricksHttpClient httpClient)
      throws DatabricksSQLException {

    IDatabricksConnectionContext connectionContext = session.getConnectionContext();

    // Bounded SEA API forces StreamingChunkProvider — it doesn't rely on total_chunk_count
    if (connectionContext.isStreamingChunkProviderEnabled()
        || connectionContext.isBoundedSeaApiEnabled()) {
      LOGGER.info(
          "Using StreamingChunkProvider for statementId: {}", statementId.toSQLExecStatementId());

      ChunkLinkFetcher linkFetcher = new SeaChunkLinkFetcher(session, statementId);
      CompressionCodec compressionCodec = resultManifest.getResultCompression();
      int maxChunksInMemory = connectionContext.getCloudFetchThreadPoolSize();
      int linkPrefetchWindow = connectionContext.getLinkPrefetchWindow();
      int chunkReadyTimeoutSeconds = connectionContext.getChunkReadyTimeoutSeconds();
      double cloudFetchSpeedThreshold = connectionContext.getCloudFetchSpeedThreshold();

      // Convert ExternalLinks to ChunkLinkFetchResult for the provider.
      // Bounded SEA API: pass null for totalChunkCount — we must not depend on
      // manifest.{chunks, total_chunk_count, total_row_count} per the bounded API contract.
      Long totalChunkCount =
          connectionContext.isBoundedSeaApiEnabled() ? null : resultManifest.getTotalChunkCount();
      ChunkLinkFetchResult initialLinks =
          convertToChunkLinkFetchResult(resultData.getExternalLinks(), totalChunkCount);

      return new StreamingChunkProvider(
          linkFetcher,
          httpClient,
          compressionCodec,
          statementId,
          maxChunksInMemory,
          linkPrefetchWindow,
          chunkReadyTimeoutSeconds,
          cloudFetchSpeedThreshold,
          connectionContext,
          initialLinks);
    } else {
      // Use the original RemoteChunkProvider
      return new RemoteChunkProvider(
          statementId,
          resultManifest,
          resultData,
          session,
          httpClient,
          connectionContext.getCloudFetchThreadPoolSize());
    }
  }

  public ArrowStreamResult(
      TFetchResultsResp resultsResp,
      IDatabricksStatementInternal parentStatementId,
      IDatabricksSession session)
      throws SQLException {
    this(
        resultsResp,
        parentStatementId,
        session,
        DatabricksHttpClientFactory.getInstance().getClient(session.getConnectionContext()));
  }

  @VisibleForTesting
  ArrowStreamResult(
      TFetchResultsResp resultsResp,
      IDatabricksStatementInternal parentStatement,
      IDatabricksSession session,
      IDatabricksHttpClient httpClient)
      throws SQLException {
    this(createThriftRemoteResult(resultsResp, parentStatement, session, httpClient), session);
  }

  private ArrowStreamResult(ThriftRemoteResult result, IDatabricksSession session)
      throws DatabricksSQLException {
    super(result.chunkProvider, result.columnInfoList, session);
  }

  private static ThriftRemoteResult createThriftRemoteResult(
      TFetchResultsResp resultsResp,
      IDatabricksStatementInternal parentStatement,
      IDatabricksSession session,
      IDatabricksHttpClient httpClient)
      throws SQLException {
    List<ColumnInfo> columnInfoList = getColumnInfoList(resultsResp.getResultSetMetadata());
    ChunkProvider chunkProvider =
        createThriftRemoteChunkProvider(resultsResp, parentStatement, session, httpClient);
    return new ThriftRemoteResult(chunkProvider, columnInfoList);
  }

  private static final class ThriftRemoteResult {
    private final ChunkProvider chunkProvider;
    private final List<ColumnInfo> columnInfoList;

    private ThriftRemoteResult(ChunkProvider chunkProvider, List<ColumnInfo> columnInfoList) {
      this.chunkProvider = chunkProvider;
      this.columnInfoList = columnInfoList;
    }
  }

  /**
   * Creates the appropriate remote chunk provider for Thrift based on configuration.
   *
   * @param resultsResp The Thrift fetch results response
   * @param parentStatement The parent statement for fetching additional chunks
   * @param session The session for fetching additional chunks
   * @param httpClient The HTTP client for downloading chunk data
   * @return A ChunkProvider instance
   */
  private static ChunkProvider createThriftRemoteChunkProvider(
      TFetchResultsResp resultsResp,
      IDatabricksStatementInternal parentStatement,
      IDatabricksSession session,
      IDatabricksHttpClient httpClient)
      throws SQLException {

    IDatabricksConnectionContext connectionContext = session.getConnectionContext();
    CompressionCodec compressionCodec =
        CompressionCodec.getCompressionMapping(resultsResp.getResultSetMetadata());

    if (connectionContext.isStreamingChunkProviderEnabled()) {
      StatementId statementId = parentStatement.getStatementId();
      LOGGER.info(
          "Using StreamingChunkProvider for Thrift statementId: {}",
          StatementId.loggableStatementId(statementId));

      ChunkLinkFetcher linkFetcher = new ThriftChunkLinkFetcher(session, statementId);
      int maxChunksInMemory = connectionContext.getCloudFetchThreadPoolSize();
      int linkPrefetchWindow = connectionContext.getLinkPrefetchWindow();
      int chunkReadyTimeoutSeconds = connectionContext.getChunkReadyTimeoutSeconds();
      double cloudFetchSpeedThreshold = connectionContext.getCloudFetchSpeedThreshold();

      // Convert initial Thrift links to ChunkLinkFetchResult
      ChunkLinkFetchResult initialLinks = convertThriftLinksToChunkLinkFetchResult(resultsResp);

      return new StreamingChunkProvider(
          linkFetcher,
          httpClient,
          compressionCodec,
          statementId,
          maxChunksInMemory,
          linkPrefetchWindow,
          chunkReadyTimeoutSeconds,
          cloudFetchSpeedThreshold,
          connectionContext,
          initialLinks);
    } else {
      // Use the original RemoteChunkProvider
      return new RemoteChunkProvider(
          parentStatement,
          resultsResp,
          session,
          httpClient,
          connectionContext.getCloudFetchThreadPoolSize(),
          compressionCodec);
    }
  }

  /**
   * Converts a collection of ExternalLinks to a ChunkLinkFetchResult.
   *
   * @param externalLinks The external links to convert, may be null
   * @param totalChunkCount The total chunk count from result manifest, may be null
   * @return A ChunkLinkFetchResult, or endOfStream if chunk count is zero, or null if unknown
   */
  private static ChunkLinkFetchResult convertToChunkLinkFetchResult(
      Collection<ExternalLink> externalLinks, Long totalChunkCount) {
    if (externalLinks == null || externalLinks.isEmpty()) {
      // total_chunk_count == 0: explicit empty result
      if (totalChunkCount != null && totalChunkCount == 0) {
        LOGGER.debug("Total chunk count is zero, returning end of stream");
        return ChunkLinkFetchResult.endOfStream();
      }
      // Bounded-SEA mode omits total_chunk_count; empty links means the server has no chunks.
      if (totalChunkCount == null) {
        LOGGER.debug("No external links and total_chunk_count absent — treating as end of stream");
        return ChunkLinkFetchResult.endOfStream();
      }
      return null;
    }

    List<ExternalLink> linkList =
        externalLinks instanceof List
            ? (List<ExternalLink>) externalLinks
            : new ArrayList<>(externalLinks);

    // Derive hasMore and nextRowOffset from last link (SEA style)
    ExternalLink lastLink = linkList.get(linkList.size() - 1);
    boolean hasMore = lastLink.getNextChunkIndex() != null;
    long nextFetchIndex = hasMore ? lastLink.getNextChunkIndex() : -1;
    long nextRowOffset = lastLink.getRowOffset() + lastLink.getRowCount();

    LOGGER.debug(
        "Converting ExternalLinks to ChunkLinkFetchResult: linkCount={}, hasMore={}, nextFetchIndex={}, nextRowOffset={}",
        linkList.size(),
        hasMore,
        nextFetchIndex,
        nextRowOffset);

    return ChunkLinkFetchResult.of(linkList, hasMore, nextFetchIndex, nextRowOffset);
  }

  /**
   * Converts Thrift result links to a ChunkLinkFetchResult.
   *
   * <p>This method converts TSparkArrowResultLink from the Thrift response to the unified
   * ChunkLinkFetchResult format used by StreamingChunkProvider.
   *
   * @param resultsResp The Thrift fetch results response containing initial links
   * @return A ChunkLinkFetchResult, or endOfStream if no more rows, or null if unknown
   */
  private static ChunkLinkFetchResult convertThriftLinksToChunkLinkFetchResult(
      TFetchResultsResp resultsResp) {
    List<TSparkArrowResultLink> resultLinks = resultsResp.getResults().getResultLinks();
    if (resultLinks == null || resultLinks.isEmpty()) {
      // If hasMoreRows is false, return end of stream
      if (!resultsResp.hasMoreRows) {
        LOGGER.debug("No result links and hasMoreRows is false, returning end of stream");
        return ChunkLinkFetchResult.endOfStream();
      }
      return null;
    }

    List<ExternalLink> chunkLinks = new ArrayList<>();
    int lastIndex = resultLinks.size() - 1;
    boolean hasMoreRows = resultsResp.hasMoreRows;

    for (int linkIndex = 0; linkIndex < resultLinks.size(); linkIndex++) {
      TSparkArrowResultLink thriftLink = resultLinks.get(linkIndex);

      // Convert Thrift link to ExternalLink (sets chunkIndex, rowOffset, rowCount, etc.)
      ExternalLink externalLink = createExternalLink(thriftLink, linkIndex);

      // For the last link, set nextChunkIndex based on hasMoreRows
      if (linkIndex == lastIndex) {
        if (hasMoreRows) {
          // More chunks available - next fetch should start from lastIndex + 1
          externalLink.setNextChunkIndex((long) linkIndex + 1);
        }
        // If hasMoreRows is false, nextChunkIndex remains null (end of stream)
      } else {
        // Not the last link - next chunk follows immediately
        externalLink.setNextChunkIndex((long) linkIndex + 1);
      }

      chunkLinks.add(externalLink);
    }

    // Calculate next fetch positions from last link
    TSparkArrowResultLink lastThriftLink = resultLinks.get(lastIndex);
    long nextFetchIndex = hasMoreRows ? lastIndex + 1 : -1;
    long nextRowOffset = lastThriftLink.getStartRowOffset() + lastThriftLink.getRowCount();

    return ChunkLinkFetchResult.of(chunkLinks, hasMoreRows, nextFetchIndex, nextRowOffset);
  }
}
