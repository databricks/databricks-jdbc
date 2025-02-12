package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.common.MetadataResultConstants.NULL_STRING;
import static com.databricks.jdbc.common.util.DatabricksTypeUtil.*;

import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.dbclient.impl.thrift.DatabricksThriftServiceClient;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.*;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.core.StatementStatus;
import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import com.databricks.sdk.service.sql.StatementState;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
   * In metadata operations, a list of singleton lists is obtained. This function extracts metadata
   * values from these TColumn lists based on the data type set in the column.
   *
   * @param columnList the TColumn from which to extract values
   * @return a singleton list of metadata result
   */
  public static List<List<Object>> extractValues(List<TColumn> columnList) {
    if (columnList == null) {
      return null;
    }
    List<Object> obj =
        columnList.stream()
            .map(
                column -> {
                  try {
                    return getColumnFirstValue(column);
                  } catch (Exception e) {
                    // In case a column doesn't have an object, add the default null value
                    return NULL_STRING;
                  }
                })
            .collect(Collectors.toList());
    return new ArrayList<>(Collections.singletonList(obj));
  }

  public static List<List<Object>> extractValuesColumnar(List<TColumn> columnList) {
    if (columnList == null || columnList.isEmpty()) {
      return null;
    }
    int numberOfItems = columnList.get(0).getStringVal().getValuesSize();
    return IntStream.range(0, numberOfItems)
        .mapToObj(
            i ->
                columnList.stream()
                    .map(column -> getObjectInColumn(column, i))
                    .collect(Collectors.toList()))
        .collect(Collectors.toList());
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

  private static Object getObjectInColumn(TColumn column, int index) {
    if (column == null) {
      return NULL_STRING;
    }
    if (column.isSetStringVal()) {
      return column.getStringVal().getValues().get(index);
    } else if (column.isSetBoolVal()) {
      return column.getBoolVal().getValues().get(index);
    } else if (column.isSetDoubleVal()) {
      return column.getDoubleVal().getValues().get(index);
    } else if (column.isSetI16Val()) {
      return column.getI16Val().getValues().get(index);
    } else if (column.isSetI32Val()) {
      return column.getI32Val().getValues().get(index);
    } else if (column.isSetI64Val()) {
      return column.getI64Val().getValues().get(index);
    } else if (column.isSetBinaryVal()) {
      return column.getBinaryVal().getValues().get(index);
    } else if (column.isSetByteVal()) {
      return column.getByteVal().getValues().get(index);
    }
    return NULL_STRING;
  }

  private static Object getColumnFirstValue(TColumn column) {
    return getColumnValues(column).get(0);
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

  /**
   * Converts columnar data from a TRowSet to a row-based list format.
   *
   * @param rowSet the TRowSet containing the data
   * @return a list where each sublist represents a row with column values, or an empty list if
   *     rowSet is empty
   */
  private static List<List<Object>> convertRowSetToList(TRowSet rowSet) {
    List<List<Object>> columnarData = extractValuesFromRowSet(rowSet);
    if (columnarData.isEmpty()) {
      return Collections.emptyList();
    }
    int numRows =
        columnarData.get(0).size(); // Number of rows (if the data was displayed in row format)
    List<List<Object>> rowBasedData =
        IntStream.range(0, numRows)
            .mapToObj(i -> new ArrayList<>(columnarData.size()))
            .collect(Collectors.toList());
    for (List<Object> column : columnarData) {
      for (int rowIndex = 0; rowIndex < numRows; rowIndex++) {
        rowBasedData.get(rowIndex).add(column.get(rowIndex));
      }
    }
    return rowBasedData;
  }

  public static List<List<Object>> convertColumnarToRowBased(
      TFetchResultsResp resultsResp,
      IDatabricksStatementInternal parentStatement,
      IDatabricksSession session)
      throws DatabricksSQLException {
    List<List<Object>> columnarData = convertRowSetToList(resultsResp.getResults());
    while (resultsResp.hasMoreRows) {
      resultsResp =
          ((DatabricksThriftServiceClient) session.getDatabricksClient())
              .getMoreResults(parentStatement);
      columnarData.addAll(convertRowSetToList(resultsResp.getResults()));
    }
    return columnarData;
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

  /**
   * Extracts and returns the values from each column of a TRowSet as a list of lists. Each sublist
   * represents a column of values. Returns an empty list if the input is null or contains no
   * columns.
   *
   * @param rowSet the TRowSet to extract values from
   * @return a list of lists, each containing the values of a column, or an empty list if the input
   *     is invalid
   */
  public static List<List<Object>> extractValuesFromRowSet(TRowSet rowSet) {
    if (rowSet == null || rowSet.getColumns() == null) {
      return Collections.emptyList();
    }
    return rowSet.getColumns().stream()
        .map(DatabricksThriftUtil::getColumnValues)
        .map(list -> new ArrayList<Object>(list))
        .collect(Collectors.toUnmodifiableList());
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
