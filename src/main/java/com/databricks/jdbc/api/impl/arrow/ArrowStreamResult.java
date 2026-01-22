package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.common.util.ArrowUtil.getColumnInfoList;
import static com.databricks.jdbc.common.util.DatabricksThriftUtil.createExternalLink;

import com.databricks.jdbc.api.impl.ComplexDataTypeParser;
import com.databricks.jdbc.api.impl.IExecutionResult;
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
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** Result container for Arrow-based query results. */
public class ArrowStreamResult implements IExecutionResult {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ArrowStreamResult.class);
  private final ChunkProvider chunkProvider;
  private long currentRowIndex = -1;
  private boolean isClosed;
  private ArrowResultChunkIterator chunkIterator;
  private List<ColumnInfo> columnInfos;
  private final IDatabricksSession session;

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
    this.session = session;
    // Check if the result data contains the arrow data inline
    boolean isInlineArrow = resultData.getAttachment() != null;
    if (isInlineArrow) {
      LOGGER.debug(
          "Creating ArrowStreamResult with inline attachment for statementId: {}",
          statementId.toSQLExecStatementId());
      this.chunkProvider = new InlineChunkProvider(resultData, resultManifest);
    } else {
      LOGGER.debug(
          "Creating ArrowStreamResult with remote links for statementId: {}",
          statementId.toSQLExecStatementId());
      this.chunkProvider =
          createRemoteChunkProvider(statementId, resultManifest, resultData, session, httpClient);
    }
    this.columnInfos =
        resultManifest.getSchema().getColumnCount() == 0
            ? new ArrayList<>()
            : new ArrayList<>(resultManifest.getSchema().getColumns());
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

    if (connectionContext.isStreamingChunkProviderEnabled()) {
      LOGGER.info(
          "Using StreamingChunkProvider for statementId: {}", statementId.toSQLExecStatementId());

      ChunkLinkFetcher linkFetcher = new SeaChunkLinkFetcher(session, statementId);
      CompressionCodec compressionCodec = resultManifest.getResultCompression();
      int maxChunksInMemory = connectionContext.getCloudFetchThreadPoolSize();
      int linkPrefetchWindow = connectionContext.getLinkPrefetchWindow();
      int chunkReadyTimeoutSeconds = connectionContext.getChunkReadyTimeoutSeconds();
      double cloudFetchSpeedThreshold = connectionContext.getCloudFetchSpeedThreshold();

      // Convert ExternalLinks to ChunkLinkFetchResult for the provider
      ChunkLinkFetchResult initialLinks =
          convertToChunkLinkFetchResult(
              resultData.getExternalLinks(), resultManifest.getTotalChunkCount());

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
      throws DatabricksSQLException {
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
      throws DatabricksSQLException {
    this.session = session;
    this.columnInfos = getColumnInfoList(resultsResp.getResultSetMetadata());
    this.chunkProvider =
        createThriftRemoteChunkProvider(resultsResp, parentStatement, session, httpClient);
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
      throws DatabricksSQLException {

    IDatabricksConnectionContext connectionContext = session.getConnectionContext();
    CompressionCodec compressionCodec =
        CompressionCodec.getCompressionMapping(resultsResp.getResultSetMetadata());

    if (connectionContext.isStreamingChunkProviderEnabled()) {
      StatementId statementId = parentStatement.getStatementId();
      LOGGER.info("Using StreamingChunkProvider for Thrift statementId: {}", statementId);

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

  public List<String> getArrowMetadata() throws DatabricksSQLException {
    if (chunkProvider == null || chunkProvider.getChunk() == null) {
      return null;
    }
    return chunkProvider.getChunk().getArrowMetadata();
  }

  /** {@inheritDoc} */
  @Override
  public Object getObject(int columnIndex) throws DatabricksSQLException {
    ColumnInfo columnInfo = columnInfos.get(columnIndex);
    ColumnInfoTypeName requiredType = columnInfo.getTypeName();
    String arrowMetadata = chunkIterator.getType(columnIndex);
    if (arrowMetadata == null) {
      arrowMetadata = columnInfo.getTypeText();
    }

    return getObjectWithComplexTypeHandling(
        session, chunkIterator, columnIndex, requiredType, arrowMetadata, columnInfo);
  }

  /**
   * Checks if the given type is a complex type (ARRAY, MAP, STRUCT, GEOMETRY, or GEOGRAPHY).
   *
   * @param type The type to check
   * @return true if the type is a complex type, false otherwise
   */
  @VisibleForTesting
  public static boolean isComplexType(ColumnInfoTypeName type) {
    return type == ColumnInfoTypeName.ARRAY
        || type == ColumnInfoTypeName.MAP
        || type == ColumnInfoTypeName.STRUCT
        || type == ColumnInfoTypeName.GEOMETRY
        || type == ColumnInfoTypeName.GEOGRAPHY;
  }

  /**
   * Checks if the given type is a geospatial type (GEOMETRY or GEOGRAPHY).
   *
   * @param type The type to check
   * @return true if the type is a geospatial type, false otherwise
   */
  @VisibleForTesting
  public static boolean isGeospatialType(ColumnInfoTypeName type) {
    return type == ColumnInfoTypeName.GEOMETRY || type == ColumnInfoTypeName.GEOGRAPHY;
  }

  /** {@inheritDoc} */
  @Override
  public long getCurrentRow() {
    return currentRowIndex;
  }

  /** {@inheritDoc} */
  @Override
  public boolean next() throws DatabricksSQLException {
    if (!hasNext()) {
      return false;
    }

    currentRowIndex++;
    if (chunkIterator == null || !chunkIterator.hasNextRow()) {
      chunkProvider.next();
      chunkIterator = chunkProvider.getChunk().getChunkIterator();
    }

    return chunkIterator.nextRow();
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasNext() {
    if (isClosed) {
      return false;
    }

    // Check if there are any more rows available in the current chunk
    if (chunkIterator != null && chunkIterator.hasNextRow()) {
      return true;
    }

    // For inline arrow, check if the chunk extractor has more chunks
    // Otherwise, check the chunk downloader
    return chunkProvider.hasNextChunk();
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    isClosed = true;
    chunkProvider.close();
  }

  @Override
  public long getRowCount() {
    return chunkProvider.getRowCount();
  }

  @Override
  public long getChunkCount() {
    return chunkProvider.getChunkCount();
  }

  /**
   * Returns the chunk provider for testing purposes.
   *
   * @return the chunk provider
   */
  @VisibleForTesting
  public ChunkProvider getChunkProvider() {
    return chunkProvider;
  }

  /**
   * Helper method to handle complex type and geospatial type conversion when support is disabled.
   *
   * <p>This method is also used by LazyThriftInlineArrowResult for consistent type handling.
   *
   * @param session The databricks session
   * @param chunkIterator The chunk iterator
   * @param columnIndex The column index
   * @param requiredType The required column type
   * @param arrowMetadata The arrow metadata
   * @param columnInfo The column info
   * @return The object value (converted if complex/geospatial type and support disabled)
   * @throws DatabricksSQLException if an error occurs
   */
  protected static Object getObjectWithComplexTypeHandling(
      IDatabricksSession session,
      ArrowResultChunkIterator chunkIterator,
      int columnIndex,
      ColumnInfoTypeName requiredType,
      String arrowMetadata,
      ColumnInfo columnInfo)
      throws DatabricksSQLException {
    boolean isComplexDatatypeSupportEnabled =
        session.getConnectionContext().isComplexDatatypeSupportEnabled();
    boolean isGeoSpatialSupportEnabled =
        session.getConnectionContext().isGeoSpatialSupportEnabled();

    // Check if we need to convert geospatial types to string when geospatial support is disabled
    // This check must come before the general complex type check
    if (!isGeoSpatialSupportEnabled && isGeospatialType(requiredType)) {
      LOGGER.debug("Geospatial support is disabled, converting {} to STRING", requiredType);

      Object result =
          chunkIterator.getColumnObjectAtCurrentRow(
              columnIndex, ColumnInfoTypeName.STRING, "STRING", columnInfo);
      if (result == null) {
        return null;
      }
      // Return raw string for geospatial types when support is disabled
      return result;
    }

    if (!isComplexDatatypeSupportEnabled && isComplexType(requiredType)) {
      LOGGER.debug("Complex datatype support is disabled, converting complex type to STRING");
      Object result =
          chunkIterator.getColumnObjectAtCurrentRow(
              columnIndex, ColumnInfoTypeName.STRING, "STRING", columnInfo);
      if (result == null) {
        return null;
      }
      ComplexDataTypeParser parser = new ComplexDataTypeParser();

      return parser.formatComplexTypeString(result.toString(), requiredType.name(), arrowMetadata);
    }

    return chunkIterator.getColumnObjectAtCurrentRow(
        columnIndex, requiredType, arrowMetadata, columnInfo);
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
      // If total chunk count is zero, return end of stream
      if (totalChunkCount != null && totalChunkCount == 0) {
        LOGGER.debug("Total chunk count is zero, returning end of stream");
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
