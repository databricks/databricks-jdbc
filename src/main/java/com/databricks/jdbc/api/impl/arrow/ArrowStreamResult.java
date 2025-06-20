package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.common.util.DatabricksThriftUtil.getColumnInfoFromTColumnDesc;

import com.databricks.jdbc.api.impl.ComplexDataTypeParser;
import com.databricks.jdbc.api.impl.IExecutionResult;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.TColumnDesc;
import com.databricks.jdbc.model.client.thrift.generated.TFetchResultsResp;
import com.databricks.jdbc.model.client.thrift.generated.TGetResultSetMetadataResp;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.sdk.service.sql.ColumnInfo;
import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;

/** Result container for Arrow-based query results. */
public class ArrowStreamResult implements IExecutionResult {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ArrowStreamResult.class);
  private final ChunkProvider chunkProvider;
  private long currentRowIndex = -1;
  private boolean isClosed;
  private ArrowResultChunk.ArrowResultChunkIterator chunkIterator;
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
      this.chunkProvider = new InlineChunkProvider(resultData, resultManifest);
    } else {
      this.chunkProvider =
          new RemoteChunkProvider(
              statementId,
              resultManifest,
              resultData,
              session,
              httpClient,
              session.getConnectionContext().getCloudFetchThreadPoolSize());
    }
    this.columnInfos =
        resultManifest.getSchema().getColumnCount() == 0
            ? new ArrayList<>()
            : new ArrayList<>(resultManifest.getSchema().getColumns());
  }

  public ArrowStreamResult(
      TFetchResultsResp resultsResp,
      boolean isInlineArrow,
      IDatabricksStatementInternal parentStatementId,
      IDatabricksSession session)
      throws DatabricksSQLException {
    this(
        resultsResp,
        isInlineArrow,
        parentStatementId,
        session,
        DatabricksHttpClientFactory.getInstance().getClient(session.getConnectionContext()));
  }

  @VisibleForTesting
  ArrowStreamResult(
      TFetchResultsResp resultsResp,
      boolean isInlineArrow,
      IDatabricksStatementInternal parentStatement,
      IDatabricksSession session,
      IDatabricksHttpClient httpClient)
      throws DatabricksSQLException {
    this.session = session;
    setColumnInfo(resultsResp.getResultSetMetadata());
    if (isInlineArrow) {
      this.chunkProvider = new InlineChunkProvider(resultsResp, parentStatement, session);
    } else {
      CompressionCodec compressionCodec =
          CompressionCodec.getCompressionMapping(resultsResp.getResultSetMetadata());
      this.chunkProvider =
          new RemoteChunkProvider(
              parentStatement,
              resultsResp,
              session,
              httpClient,
              session.getConnectionContext().getCloudFetchThreadPoolSize(),
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
    ColumnInfoTypeName requiredType = columnInfos.get(columnIndex).getTypeName();
    String arrowMetadata = chunkIterator.getType(columnIndex);
    if (arrowMetadata == null) {
      arrowMetadata = columnInfos.get(columnIndex).getTypeText();
    }

    // Handle complex type conversion when complex datatype support is disabled
    boolean isComplexDatatypeSupportEnabled =
        this.session.getConnectionContext().isComplexDatatypeSupportEnabled();
    if (!isComplexDatatypeSupportEnabled && isComplexType(requiredType)) {
      LOGGER.debug("Complex datatype support is disabled, converting complex type to STRING");

      Object result =
          chunkIterator.getColumnObjectAtCurrentRow(
              columnIndex, ColumnInfoTypeName.STRING, "STRING", columnInfos.get(columnIndex));
      ComplexDataTypeParser parser = new ComplexDataTypeParser();
      return parser.formatComplexTypeString(result.toString(), requiredType.name(), arrowMetadata);
    }

    return chunkIterator.getColumnObjectAtCurrentRow(
        columnIndex, requiredType, arrowMetadata, columnInfos.get(columnIndex));
  }

  /**
   * Checks if the given type is a complex type (ARRAY, MAP, or STRUCT).
   *
   * @param type The type to check
   * @return true if the type is a complex type, false otherwise
   */
  @VisibleForTesting
  public static boolean isComplexType(ColumnInfoTypeName type) {
    return type == ColumnInfoTypeName.ARRAY
        || type == ColumnInfoTypeName.MAP
        || type == ColumnInfoTypeName.STRUCT;
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

  private void setColumnInfo(TGetResultSetMetadataResp resultManifest) {
    columnInfos = new ArrayList<>();
    if (resultManifest.getSchema() == null) {
      return;
    }
    for (TColumnDesc tColumnDesc : resultManifest.getSchema().getColumns()) {
      columnInfos.add(getColumnInfoFromTColumnDesc(tColumnDesc));
    }
  }
}
