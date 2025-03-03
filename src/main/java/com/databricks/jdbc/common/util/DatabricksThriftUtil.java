package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.common.util.DatabricksTypeUtil.*;

import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.*;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.core.StatementStatus;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import com.databricks.sdk.service.sql.StatementState;
import java.nio.ByteBuffer;
import java.util.*;

public class DatabricksThriftUtil {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DatabricksThriftUtil.class);
  private static final List<TStatusCode> SUCCESS_STATUS_LIST =
      List.of(TStatusCode.SUCCESS_STATUS, TStatusCode.SUCCESS_WITH_INFO_STATUS);

  public static TNamespace getNamespace(String catalog, String schema) {
    return new TNamespace().setCatalogName(catalog).setSchemaName(schema);
  }

  public static String byteBufferToString(ByteBuffer buffer) {
    ByteBuffer newBuffer = buffer.duplicate(); // This is to avoid a BufferUnderflowException
    long sigBits = newBuffer.getLong();
    return new UUID(sigBits, sigBits).toString();
  }

  public static ExternalLink createExternalLink(TSparkArrowResultLink chunkInfo, long chunkIndex) {
    return new ExternalLink()
        .setExternalLink(chunkInfo.getFileLink())
        .setChunkIndex(chunkIndex)
        .setExpiration(Long.toString(chunkInfo.getExpiryTime()));
  }

  public static void verifySuccessStatus(TStatus status, String errorContext)
      throws DatabricksHttpException {
    if (!SUCCESS_STATUS_LIST.contains(status.getStatusCode())) {
      String errorMessage = "Error thrift response received. " + errorContext;
      LOGGER.error(errorMessage);
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
      columnIterators.add(getIteratorForColumn(column));
    }
    while (columnIterators.get(0).hasNext()) {
      List<Object> row = new ArrayList<>();
      columnIterators.forEach(columnIterator -> row.add(columnIterator.next()));
      rows.add(row);
    }
    return rows;
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

  private static <T> Iterator<T> wrapIterator(final List<T> values, final byte[] nulls) {
    return new Iterator<T>() {
      int idx = 0;

      @Override
      public boolean hasNext() {
        return idx < values.size();
      }

      @Override
      public T next() {
        // Check if the nulls array indicates a null at this index.
        boolean isNull = (nulls != null && nulls.length > idx && nulls[idx] == 1);
        T value = isNull ? null : values.get(idx);
        idx++;
        return value;
      }
    };
  }

  private static Iterator<?> getIteratorForColumn(TColumn column) throws DatabricksSQLException {
    if (column.isSetStringVal()) {
      return wrapIterator(column.getStringVal().getValues(), column.getStringVal().getNulls());
    } else if (column.isSetBoolVal()) {
      return wrapIterator(column.getBoolVal().getValues(), column.getBoolVal().getNulls());
    } else if (column.isSetDoubleVal()) {
      return wrapIterator(column.getDoubleVal().getValues(), column.getDoubleVal().getNulls());
    } else if (column.isSetI16Val()) {
      return wrapIterator(column.getI16Val().getValues(), column.getI16Val().getNulls());
    } else if (column.isSetI32Val()) {
      return wrapIterator(column.getI32Val().getValues(), column.getI32Val().getNulls());
    } else if (column.isSetI64Val()) {
      return wrapIterator(column.getI64Val().getValues(), column.getI64Val().getNulls());
    } else if (column.isSetBinaryVal()) {
      return wrapIterator(column.getBinaryVal().getValues(), column.getBinaryVal().getNulls());
    } else if (column.isSetByteVal()) {
      return wrapIterator(column.getByteVal().getValues(), column.getByteVal().getNulls());
    }
    throw new DatabricksSQLException(
        "Unsupported column type: " + column, DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }

  public static String getTypeTextFromTypeDesc(TTypeDesc typeDesc) {
    TTypeId type = getThriftTypeFromTypeDesc(typeDesc);
    return type.name().replace("_TYPE", "");
  }

  public static ColumnInfoTypeName getTypeFromTypeDesc(TTypeDesc typeDesc) {
    TTypeId type = getThriftTypeFromTypeDesc(typeDesc);
    switch (type) {
      case BOOLEAN_TYPE:
        return ColumnInfoTypeName.BOOLEAN;
      case TINYINT_TYPE:
        return ColumnInfoTypeName.BYTE;
      case SMALLINT_TYPE:
        return ColumnInfoTypeName.SHORT;
      case INT_TYPE:
        return ColumnInfoTypeName.INT;
      case BIGINT_TYPE:
        return ColumnInfoTypeName.LONG;
      case FLOAT_TYPE:
        return ColumnInfoTypeName.FLOAT;
      case DOUBLE_TYPE:
        return ColumnInfoTypeName.DOUBLE;
      case TIMESTAMP_TYPE:
        return ColumnInfoTypeName.TIMESTAMP;
      case BINARY_TYPE:
        return ColumnInfoTypeName.BINARY;
      case DECIMAL_TYPE:
        return ColumnInfoTypeName.DECIMAL;
      case DATE_TYPE:
        return ColumnInfoTypeName.DATE;
      case CHAR_TYPE:
        return ColumnInfoTypeName.CHAR;
      case INTERVAL_YEAR_MONTH_TYPE:
      case INTERVAL_DAY_TIME_TYPE:
        return ColumnInfoTypeName.INTERVAL;
      case ARRAY_TYPE:
        return ColumnInfoTypeName.ARRAY;
      case MAP_TYPE:
        return ColumnInfoTypeName.MAP;
      case STRUCT_TYPE:
        return ColumnInfoTypeName.STRUCT;
      default:
        return ColumnInfoTypeName.STRING;
    }
  }

  /**
   * Extracts values from a TColumn based on the data type set in the column.
   *
   * @param column the TColumn from which to extract values
   * @return a list of values from the specified column
   */
  private static List<?> getColumnValues(TColumn column) {
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

    return getColumnValuesWithNulls(
        column.getStringVal().getValues(), column.getStringVal().getNulls()); // default to string
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
    List<List<Object>> rows = extractRowsFromColumnar(resultsResp.getResults());
    while (resultsResp.hasMoreRows) {
      resultsResp = session.getDatabricksClient().getMoreResults(parentStatement);
      rows.addAll(extractRowsFromColumnar(resultsResp.getResults()));
    }
    return rows;
  }

  public static TOperationHandle getOperationHandle(StatementId statementId) {
    THandleIdentifier identifier = statementId.toOperationIdentifier();
    // This will help logging the statement-Id in readable format for debugging purposes
    LOGGER.debug(
        "getOperationHandle {%s} for statementId {%s}",
        statementId, byteBufferToString(identifier.guid));
    return new TOperationHandle()
        .setOperationId(identifier)
        .setOperationType(TOperationType.UNKNOWN);
  }

  public static long getRowCount(TRowSet resultData) {
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
      TSparkDirectResults directResults, String context) throws DatabricksHttpException {
    if (directResults.isSetOperationStatus()) {
      LOGGER.debug("direct result operation status being verified for success response");
      verifySuccessStatus(directResults.getOperationStatus().getStatus(), context);
    }
    if (directResults.isSetResultSetMetadata()) {
      LOGGER.debug("direct results metadata being verified for success response");
      verifySuccessStatus(directResults.getResultSetMetadata().getStatus(), context);
    }
    if (directResults.isSetCloseOperation()) {
      LOGGER.debug("direct results close operation verified for success response");
      verifySuccessStatus(directResults.getCloseOperation().getStatus(), context);
    }
    if (directResults.isSetResultSet()) {
      LOGGER.debug("direct result set being verified for success response");
      verifySuccessStatus(directResults.getResultSet().getStatus(), context);
    }
  }
}
