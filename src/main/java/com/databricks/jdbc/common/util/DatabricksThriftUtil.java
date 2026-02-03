package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.ARROW_METADATA_KEY;
import static com.databricks.jdbc.common.EnvironmentVariables.DEFAULT_RESULT_ROW_LIMIT;
import static com.databricks.jdbc.common.util.DatabricksTypeUtil.*;
import static com.databricks.jdbc.model.client.thrift.generated.TTypeId.*;

import com.databricks.jdbc.api.impl.ColumnarRowView;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.*;
import com.databricks.jdbc.model.core.ColumnInfo;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.core.StatementStatus;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.service.sql.StatementState;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.SchemaUtility;

public class DatabricksThriftUtil {

  private static final Map<TTypeId, ColumnInfoTypeName> T_TYPE_ID_COLUMN_INFO_TYPE_NAME_MAP =
      Map.ofEntries(
          Map.entry(BOOLEAN_TYPE, ColumnInfoTypeName.BOOLEAN),
          Map.entry(TINYINT_TYPE, ColumnInfoTypeName.BYTE),
          Map.entry(SMALLINT_TYPE, ColumnInfoTypeName.SHORT),
          Map.entry(INT_TYPE, ColumnInfoTypeName.INT),
          Map.entry(BIGINT_TYPE, ColumnInfoTypeName.LONG),
          Map.entry(FLOAT_TYPE, ColumnInfoTypeName.FLOAT),
          Map.entry(DOUBLE_TYPE, ColumnInfoTypeName.DOUBLE),
          Map.entry(STRING_TYPE, ColumnInfoTypeName.STRING),
          Map.entry(VARCHAR_TYPE, ColumnInfoTypeName.STRING),
          Map.entry(TIMESTAMP_TYPE, ColumnInfoTypeName.TIMESTAMP),
          Map.entry(BINARY_TYPE, ColumnInfoTypeName.BINARY),
          Map.entry(DECIMAL_TYPE, ColumnInfoTypeName.DECIMAL),
          Map.entry(DATE_TYPE, ColumnInfoTypeName.DATE),
          Map.entry(CHAR_TYPE, ColumnInfoTypeName.CHAR),
          Map.entry(INTERVAL_YEAR_MONTH_TYPE, ColumnInfoTypeName.INTERVAL),
          Map.entry(INTERVAL_DAY_TIME_TYPE, ColumnInfoTypeName.INTERVAL),
          Map.entry(ARRAY_TYPE, ColumnInfoTypeName.ARRAY),
          Map.entry(MAP_TYPE, ColumnInfoTypeName.MAP),
          Map.entry(NULL_TYPE, ColumnInfoTypeName.STRING),
          Map.entry(STRUCT_TYPE, ColumnInfoTypeName.STRUCT));

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DatabricksThriftUtil.class);
  private static final List<TStatusCode> SUCCESS_STATUS_LIST =
      List.of(TStatusCode.SUCCESS_STATUS, TStatusCode.SUCCESS_WITH_INFO_STATUS);

  public static TNamespace getNamespace(String catalog, String schema) {
    return new TNamespace().setCatalogName(catalog).setSchemaName(schema);
  }

  public static String byteBufferToString(ByteBuffer buffer) {
    ByteBuffer newBuffer = buffer.duplicate(); // This is to avoid a BufferUnderflowException
    // sessionId and statementID have guid which are 16 bytes long
    if (newBuffer.remaining() >= DatabricksJdbcConstants.UUID_LENGTH) {
      long mostSigBits = newBuffer.getLong();
      long leastSigBits = newBuffer.getLong();
      return new UUID(mostSigBits, leastSigBits).toString();
    } else {
      long sigBits = newBuffer.getLong();
      return new UUID(sigBits, sigBits).toString();
    }
  }

  public static ExternalLink createExternalLink(TSparkArrowResultLink chunkInfo, long chunkIndex) {
    return new ExternalLink()
        .setExternalLink(chunkInfo.getFileLink())
        .setChunkIndex(chunkIndex)
        .setExpiration(Instant.ofEpochMilli(chunkInfo.getExpiryTime()).toString())
        .setRowOffset(chunkInfo.getStartRowOffset())
        .setByteCount(chunkInfo.getBytesNum())
        .setRowCount(chunkInfo.getRowCount());
  }

  public static void verifySuccessStatus(TStatus status, String errorContext)
      throws DatabricksHttpException {
    verifySuccessStatus(status, errorContext, null);
  }

  public static void verifySuccessStatus(TStatus status, String errorContext, String statementId)
      throws DatabricksHttpException {
    if (!SUCCESS_STATUS_LIST.contains(status.getStatusCode())) {
      String errorMessage =
          statementId != null
              ? String.format(
                  "Error thrift response received [%s] for statementId [%s]",
                  errorContext, statementId)
              : String.format("Error thrift response received [%s]", errorContext);
      throw new DatabricksHttpException(errorMessage, status.getSqlState());
    }
  }

  public static int getColumnCount(TGetResultSetMetadataResp resultManifest) {
    if (resultManifest == null || resultManifest.getSchema() == null) {
      return 0;
    }
    return resultManifest.getSchema().getColumnsSize();
  }

  /**
   * This functions extracts columnar data from a RowSet into rows
   *
   * @param rowSet that contains columnar data
   * @return a list of rows
   */
  public static List<List<Object>> extractRowsFromColumnar(TRowSet rowSet)
      throws DatabricksSQLException {
    if (rowSet == null || rowSet.getColumns() == null || rowSet.getColumns().isEmpty()) {
      return Collections.emptyList();
    }
    List<List<Object>> rows = new ArrayList<>();
    List<Iterator<?>> columnIterators = new ArrayList<>();
    for (TColumn column : rowSet.getColumns()) {
      columnIterators.add(getColumnValues(column).iterator());
    }
    while (columnIterators.get(0).hasNext()) {
      List<Object> row = new ArrayList<>();
      columnIterators.forEach(columnIterator -> row.add(columnIterator.next()));
      rows.add(row);
    }
    return rows;
  }

  /**
   * Memory-efficient alternative that creates a columnar view instead of materializing all rows.
   * Use this method to significantly reduce memory allocations when accessing row data.
   *
   * @param rowSet that contains columnar data
   * @return a columnar view that provides row-based access without full materialization
   */
  public static ColumnarRowView createColumnarView(TRowSet rowSet) throws DatabricksSQLException {
    return new ColumnarRowView(rowSet);
  }

  /** Returns statement status for given operation status response */
  public static StatementStatus getStatementStatus(TGetOperationStatusResp resp) {
    StatementState state = null;
    switch (resp.getOperationState()) {
      case INITIALIZED_STATE:
      case PENDING_STATE:
        state = StatementState.PENDING;
        break;

      case RUNNING_STATE:
        state = StatementState.RUNNING;
        break;

      case FINISHED_STATE:
        state = StatementState.SUCCEEDED;
        break;

      case ERROR_STATE:
      case TIMEDOUT_STATE:
        // TODO: Also set the sql_state and error message
        state = StatementState.FAILED;
        break;

      case CLOSED_STATE:
        state = StatementState.CLOSED;
        break;

      case CANCELED_STATE:
        state = StatementState.CANCELED;
        break;

      case UKNOWN_STATE:
        state = StatementState.FAILED;
    }

    return new StatementStatus().setState(state);
  }

  /** Returns statement status for given status response */
  public static StatementStatus getAsyncStatus(TStatus status) {
    StatementStatus statementStatus = new StatementStatus();
    StatementState state = null;

    switch (status.getStatusCode()) {
        // For async mode, success would just mean that statement was successfully submitted
        // actual status should be checked using GetOperationStatus
      case SUCCESS_STATUS:
      case SUCCESS_WITH_INFO_STATUS:
      case STILL_EXECUTING_STATUS:
        state = StatementState.RUNNING;
        break;

      case INVALID_HANDLE_STATUS:
      case ERROR_STATUS:
        // TODO: set sql_state in case of error
        state = StatementState.FAILED;
        break;

      default:
        state = StatementState.FAILED;
    }

    return new StatementStatus().setState(state);
  }

  public static String getTypeTextFromTypeDesc(TTypeDesc typeDesc) {
    TPrimitiveTypeEntry primitiveTypeEntry = getTPrimitiveTypeOrDefault(typeDesc);
    return primitiveTypeEntry.getType().name().replace("_TYPE", "");
  }

  public static ColumnInfo getColumnInfoFromTColumnDesc(
      TColumnDesc columnDesc, String arrowMetadata) {
    TPrimitiveTypeEntry primitiveTypeEntry = getTPrimitiveTypeOrDefault(columnDesc.getTypeDesc());
    ColumnInfoTypeName columnInfoTypeName =
        T_TYPE_ID_COLUMN_INFO_TYPE_NAME_MAP.get(primitiveTypeEntry.getType());

    String typeText = getTypeTextFromTypeDesc(columnDesc.getTypeDesc());

    if (arrowMetadata != null && isComplexType(arrowMetadata)) {
      typeText = arrowMetadata;
      if (arrowMetadata.startsWith(GEOMETRY)) {
        columnInfoTypeName = ColumnInfoTypeName.GEOMETRY;
      } else if (arrowMetadata.startsWith(GEOGRAPHY)) {
        columnInfoTypeName = ColumnInfoTypeName.GEOGRAPHY;
      }
    }

    ColumnInfo columnInfo =
        new ColumnInfo()
            .setName(columnDesc.getColumnName())
            .setPosition((long) columnDesc.getPosition())
            .setTypeName(columnInfoTypeName)
            .setTypeText(typeText);

    if (primitiveTypeEntry.isSetTypeQualifiers()) {
      TTypeQualifiers typeQualifiers = primitiveTypeEntry.getTypeQualifiers();
      String scaleQualifierKey = TCLIServiceConstants.SCALE,
          precisionQualifierKey = TCLIServiceConstants.PRECISION;
      if (typeQualifiers.getQualifiers().get(scaleQualifierKey) != null) {
        columnInfo.setTypeScale(
            (long) typeQualifiers.getQualifiers().get(scaleQualifierKey).getI32Value());
      }
      if (typeQualifiers.getQualifiers().get(precisionQualifierKey) != null) {
        columnInfo.setTypePrecision(
            (long) typeQualifiers.getQualifiers().get(precisionQualifierKey).getI32Value());
      }
    }
    return columnInfo;
  }

  /**
   * Extracts values from a TColumn based on the data type set in the column.
   *
   * @param column the TColumn from which to extract values
   * @return a list of values from the specified column
   */
  private static List<?> getColumnValues(TColumn column) throws DatabricksSQLException {
    if (column.isSetBinaryVal())
      return getColumnValuesWithNulls(
          column.getBinaryVal().getValues(), column.getBinaryVal().getNulls());
    if (column.isSetBoolVal())
      return getColumnValuesWithNulls(
          column.getBoolVal().getValues(), column.getBoolVal().getNulls());
    if (column.isSetByteVal())
      return getColumnValuesWithNulls(
          column.getByteVal().getValues(), column.getByteVal().getNulls());
    if (column.isSetDoubleVal())
      return getColumnValuesWithNulls(
          column.getDoubleVal().getValues(), column.getDoubleVal().getNulls());
    if (column.isSetI16Val())
      return getColumnValuesWithNulls(
          column.getI16Val().getValues(), column.getI16Val().getNulls());
    if (column.isSetI32Val())
      return getColumnValuesWithNulls(
          column.getI32Val().getValues(), column.getI32Val().getNulls());
    if (column.isSetI64Val())
      return getColumnValuesWithNulls(
          column.getI64Val().getValues(), column.getI64Val().getNulls());
    if (column.isSetDoubleVal())
      return getColumnValuesWithNulls(
          column.getDoubleVal().getValues(), column.getDoubleVal().getNulls());
    if (column.isSetStringVal())
      return getColumnValuesWithNulls(
          column.getStringVal().getValues(), column.getStringVal().getNulls());

    throw new DatabricksSQLException(
        "Unsupported column type: " + column, DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }

  private static <T> List<T> getColumnValuesWithNulls(List<T> values, byte[] nulls) {
    List<T> result = new ArrayList<>();
    if (nulls != null) {
      BitSet nullBits = BitSet.valueOf(nulls);
      for (int i = 0; i < values.size(); i++) {
        if (nullBits.get(i)) {
          result.add(null); // Add null if the value is null
        } else {
          result.add(values.get(i));
        }
      }
    } else {
      result.addAll(values);
    }
    return result;
  }

  public static List<List<Object>> convertColumnarToRowBased(
      TFetchResultsResp resultsResp,
      IDatabricksStatementInternal parentStatement,
      IDatabricksSession session)
      throws DatabricksSQLException {
    int statementMaxRows =
        parentStatement != null ? parentStatement.getMaxRows() : DEFAULT_RESULT_ROW_LIMIT;
    boolean hasRowLimit = statementMaxRows > 0;
    List<List<Object>> rows = extractRowsFromColumnar(resultsResp.getResults());
    while (resultsResp.hasMoreRows) {
      resultsResp = session.getDatabricksClient().getMoreResults(parentStatement);
      rows.addAll(extractRowsFromColumnar(resultsResp.getResults()));
      if (hasRowLimit
          && rows.size() >= statementMaxRows) { // check if we have reached requested row limit
        break;
      }
    }
    if (hasRowLimit
        && rows.size() > statementMaxRows) { // truncate rows to get exact number of rows requested
      rows = rows.subList(0, statementMaxRows);
    }
    return rows;
  }

  public static TOperationHandle getOperationHandle(StatementId statementId) {
    THandleIdentifier identifier = statementId.toOperationIdentifier();
    // This will help logging the statement-Id in readable format for debugging purposes
    LOGGER.debug("getOperationHandle for statementId {}", byteBufferToString(identifier.guid));
    return new TOperationHandle()
        .setOperationId(identifier)
        .setOperationType(TOperationType.UNKNOWN);
  }

  public static long getRowCount(TRowSet resultData) throws DatabricksSQLException {
    if (resultData == null) {
      return 0;
    } else if (resultData.isSetColumns()) {
      List<TColumn> columns = resultData.getColumns();
      return columns == null || columns.isEmpty() ? 0 : getColumnValues(columns.get(0)).size();
    } else if (resultData.isSetResultLinks()) {
      return resultData.getResultLinks().stream()
          .mapToLong(link -> link.isSetRowCount() ? link.getRowCount() : 0)
          .sum();
    } else if (resultData.isSetArrowBatches()) {
      return resultData.getArrowBatches().stream()
          .mapToLong(batch -> batch.isSetRowCount() ? batch.getRowCount() : 0)
          .sum();
    }

    return 0;
  }

  public static void checkDirectResultsForErrorStatus(
      TSparkDirectResults directResults, String context, String statementId)
      throws DatabricksHttpException {
    if (directResults.isSetOperationStatus()) {
      LOGGER.debug(
          "direct result operation status being verified for success response for statementId {}",
          statementId);
      verifySuccessStatus(directResults.getOperationStatus().getStatus(), context, statementId);
    }
    if (directResults.isSetResultSetMetadata()) {
      LOGGER.debug(
          "direct results metadata being verified for success response for statementId {}",
          statementId);
      verifySuccessStatus(directResults.getResultSetMetadata().getStatus(), context, statementId);
    }
    if (directResults.isSetCloseOperation()) {
      LOGGER.debug(
          "direct results close operation verified for success response for statementId {}",
          statementId);
      verifySuccessStatus(directResults.getCloseOperation().getStatus(), context, statementId);
    }
    if (directResults.isSetResultSet()) {
      LOGGER.debug(
          "direct result set being verified for success response for statementId {}", statementId);
      verifySuccessStatus(directResults.getResultSet().getStatus(), context, statementId);
    }
  }

  /**
   * Deserializes the Arrow schema from TGetResultSetMetadataResp.
   *
   * @param metadata the TGetResultSetMetadataResp containing the binary Arrow schema
   * @return the deserialized Arrow Schema
   */
  public static List<String> getArrowMetadata(TGetResultSetMetadataResp metadata)
      throws DatabricksSQLException {
    if (metadata == null
        || metadata.getArrowSchema() == null
        || metadata.getArrowSchema().length == 0) {
      return null;
    }
    byte[] arrowSchemaBytes = metadata.getArrowSchema();
    try {
      Schema arrowSchema = SchemaUtility.deserialize(arrowSchemaBytes, null);
      return arrowSchema.getFields().stream()
          .map(Field::getMetadata)
          .map(e -> e.get(ARROW_METADATA_KEY))
          .collect(Collectors.toList());
    } catch (IOException e) {
      String errorMessage = "Failed to deserialize Arrow schema: " + e.getMessage();
      LOGGER.error(errorMessage, e);
      throw new DatabricksSQLException(errorMessage, e, DatabricksDriverErrorCode.RESULT_SET_ERROR);
    }
  }
}
