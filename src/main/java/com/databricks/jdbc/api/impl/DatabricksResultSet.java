package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.EMPTY_STRING;

import com.databricks.jdbc.api.IDatabricksResultSet;
import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.api.impl.converters.ConverterHelper;
import com.databricks.jdbc.api.impl.converters.ObjectConverter;
import com.databricks.jdbc.api.impl.volume.VolumeOperationResult;
import com.databricks.jdbc.api.internal.IDatabricksResultSetInternal;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.Nullable;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.util.WarningUtil;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksSQLFeatureNotSupportedException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.TFetchResultsResp;
import com.databricks.jdbc.model.core.ColumnMetadata;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.core.StatementStatus;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.support.ToStringer;
import com.google.common.annotations.VisibleForTesting;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.http.entity.InputStreamEntity;

public class DatabricksResultSet implements IDatabricksResultSet, IDatabricksResultSetInternal {

  enum ResultSetType {
    SEA_ARROW_ENABLED,
    SEA_INLINE,
    THRIFT_ARROW_ENABLED,
    THRIFT_INLINE,
    UNASSIGNED
  }

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DatabricksResultSet.class);
  protected static final String AFFECTED_ROWS_COUNT = "num_affected_rows";
  private final StatementStatus statementStatus;
  private final StatementId statementId;
  private final IExecutionResult executionResult;
  private final DatabricksResultSetMetaData resultSetMetaData;
  private final StatementType statementType;
  private final IDatabricksStatementInternal parentStatement;
  private Long updateCount;
  private boolean isClosed;
  private SQLWarning warnings = null;
  private boolean wasNull;

  private ResultSetType resultSetType = ResultSetType.UNASSIGNED;

  private boolean complexDatatypeSupport = false;

  // Constructor for SEA result set
  public DatabricksResultSet(
      StatementStatus statementStatus,
      StatementId statementId,
      ResultData resultData,
      ResultManifest resultManifest,
      StatementType statementType,
      IDatabricksSession session,
      IDatabricksStatementInternal parentStatement)
      throws DatabricksSQLException {
    this.statementStatus = statementStatus;
    this.statementId = statementId;
    if (resultData != null) {
      this.executionResult =
          ExecutionResultFactory.getResultSet(
              resultData, resultManifest, statementId, session, parentStatement);
      this.resultSetMetaData = new DatabricksResultSetMetaData(statementId, resultManifest);
      switch (resultManifest.getFormat()) {
        case ARROW_STREAM:
          this.resultSetType = ResultSetType.SEA_ARROW_ENABLED;
          break;
        case JSON_ARRAY:
          this.resultSetType = ResultSetType.SEA_INLINE;
          break;
      }
    } else {
      executionResult = null;
      resultSetMetaData = null;
    }
    this.complexDatatypeSupport = session.getConnectionContext().isComplexDatatypeSupportEnabled();
    this.statementType = statementType;
    this.updateCount = null;
    this.parentStatement = parentStatement;
    this.isClosed = false;
    this.wasNull = false;
  }

  @VisibleForTesting
  public DatabricksResultSet(
      StatementStatus statementStatus,
      StatementId statementId,
      StatementType statementType,
      IDatabricksStatementInternal parentStatement,
      IExecutionResult executionResult,
      DatabricksResultSetMetaData resultSetMetaData,
      boolean complexDatatypeSupport) {
    this.statementStatus = statementStatus;
    this.statementId = statementId;
    this.executionResult = executionResult;
    this.resultSetMetaData = resultSetMetaData;
    this.statementType = statementType;
    this.updateCount = null;
    this.parentStatement = parentStatement;
    this.isClosed = false;
    this.wasNull = false;
    this.complexDatatypeSupport = complexDatatypeSupport;
  }

  // Constructor for thrift result set
  public DatabricksResultSet(
      StatementStatus statementStatus,
      StatementId statementId,
      TFetchResultsResp resultsResp,
      StatementType statementType,
      IDatabricksStatementInternal parentStatement,
      IDatabricksSession session)
      throws SQLException {
    this.statementStatus = statementStatus;
    this.statementId = statementId;
    if (resultsResp != null) {
      this.executionResult =
          ExecutionResultFactory.getResultSet(resultsResp, session, parentStatement);
      long rowSize = executionResult.getRowCount();
      this.resultSetMetaData =
          new DatabricksResultSetMetaData(
              statementId,
              resultsResp.getResultSetMetadata(),
              rowSize,
              executionResult.getChunkCount());
      switch (resultsResp.getResultSetMetadata().getResultFormat()) {
        case COLUMN_BASED_SET:
          this.resultSetType = ResultSetType.THRIFT_INLINE;
          break;
        case URL_BASED_SET:
        case ARROW_BASED_SET:
          this.resultSetType = ResultSetType.THRIFT_ARROW_ENABLED;
          break;
      }
    } else {
      this.executionResult = null;
      this.resultSetMetaData = null;
    }
    this.complexDatatypeSupport = session.getConnectionContext().isComplexDatatypeSupportEnabled();
    this.statementType = statementType;
    this.updateCount = null;
    this.parentStatement = parentStatement;
    this.isClosed = false;
    this.wasNull = false;
  }

  /* Constructing results for getUDTs, getTypeInfo, getProcedures metadata calls */
  public DatabricksResultSet(
      StatementStatus statementStatus,
      StatementId statementId,
      List<String> columnNames,
      List<String> columnTypeText,
      int[] columnTypes,
      int[] columnTypePrecisions,
      int[] isNullables,
      Object[][] rows,
      StatementType statementType) {
    this.statementStatus = statementStatus;
    this.statementId = statementId;
    this.executionResult = ExecutionResultFactory.getResultSet(rows);
    this.resultSetMetaData =
        new DatabricksResultSetMetaData(
            statementId,
            columnNames,
            columnTypeText,
            columnTypes,
            columnTypePrecisions,
            isNullables,
            rows.length);
    this.statementType = statementType;
    this.updateCount = null;
    this.parentStatement = null;
    this.isClosed = false;
    this.wasNull = false;
  }

  // Constructing metadata result set in thrift flow
  public DatabricksResultSet(
      StatementStatus statementStatus,
      StatementId statementId,
      List<String> columnNames,
      List<String> columnTypeText,
      List<Integer> columnTypes,
      List<Integer> columnTypePrecisions,
      List<Nullable> columnNullables,
      List<List<Object>> rows,
      StatementType statementType) {
    this.statementStatus = statementStatus;
    this.statementId = statementId;
    this.executionResult = ExecutionResultFactory.getResultSet(rows);
    this.resultSetMetaData =
        new DatabricksResultSetMetaData(
            statementId,
            columnNames,
            columnTypeText,
            columnTypes,
            columnTypePrecisions,
            columnNullables,
            rows.size());
    this.statementType = statementType;
    this.updateCount = null;
    this.parentStatement = null;
    this.isClosed = false;
    this.wasNull = false;
  }

  // Constructing metadata result set in SEA flow
  public DatabricksResultSet(
      StatementStatus statementStatus,
      StatementId statementId,
      List<ColumnMetadata> columnMetadataList,
      List<List<Object>> rows,
      StatementType statementType) {
    this.statementStatus = statementStatus;
    this.statementId = statementId;
    this.executionResult = ExecutionResultFactory.getResultSet(rows);
    this.resultSetMetaData =
        new DatabricksResultSetMetaData(statementId, columnMetadataList, rows.size());
    this.statementType = statementType;
    this.updateCount = null;
    this.parentStatement = null;
    this.isClosed = false;
    this.wasNull = false;
  }

  @Override
  public boolean next() throws SQLException {
    checkIfClosed();
    return this.executionResult.next();
  }

  @Override
  public void close() throws DatabricksSQLException {
    isClosed = true;
    this.executionResult.close();
    if (parentStatement != null) {
      parentStatement.handleResultSetClose(this);
    }
  }

  @Override
  public boolean wasNull() throws SQLException {
    checkIfClosed();
    return this.wasNull;
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    return getConvertedObject(columnIndex, ObjectConverter::toString, () -> null);
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    return getConvertedObject(columnIndex, ObjectConverter::toBoolean, () -> false);
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    return getConvertedObject(columnIndex, ObjectConverter::toByte, () -> (byte) 0);
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    return getConvertedObject(columnIndex, ObjectConverter::toShort, () -> (short) 0);
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    return getConvertedObject(columnIndex, ObjectConverter::toInt, () -> 0);
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    return getConvertedObject(columnIndex, ObjectConverter::toLong, () -> 0L);
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    return getConvertedObject(columnIndex, ObjectConverter::toFloat, () -> 0.0f);
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    return getConvertedObject(columnIndex, ObjectConverter::toDouble, () -> 0.0);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    return getConvertedObject(
        columnIndex,
        (converter, object) -> {
          BigDecimal bd = converter.toBigDecimal(object);
          return applyScaleToBigDecimal(bd, columnIndex, scale);
        },
        () -> BigDecimal.ZERO.setScale(scale, RoundingMode.HALF_UP));
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    return getConvertedObject(columnIndex, ObjectConverter::toByteArray, () -> null);
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    return getConvertedObject(columnIndex, ObjectConverter::toDate, () -> null);
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    return getConvertedObject(columnIndex, ObjectConverter::toTime, () -> null);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    return getConvertedObject(columnIndex, ObjectConverter::toTimestamp, () -> null);
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    return getConvertedObject(columnIndex, ObjectConverter::toAsciiStream, () -> null);
  }

  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    return getConvertedObject(columnIndex, ObjectConverter::toUnicodeStream, () -> null);
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    return getConvertedObject(columnIndex, ObjectConverter::toBinaryStream, () -> null);
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    return getString(getColumnNameIndex(columnLabel));
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    return getBoolean(getColumnNameIndex(columnLabel));
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    return getByte(getColumnNameIndex(columnLabel));
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    return getShort(getColumnNameIndex(columnLabel));
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    return getInt(getColumnNameIndex(columnLabel));
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    return getLong(getColumnNameIndex(columnLabel));
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    return getFloat(getColumnNameIndex(columnLabel));
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    return getDouble(getColumnNameIndex(columnLabel));
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    return getBigDecimal(getColumnNameIndex(columnLabel));
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    return getBytes(getColumnNameIndex(columnLabel));
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    return getDate(getColumnNameIndex(columnLabel));
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    return getTime(getColumnNameIndex(columnLabel));
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    return getTimestamp(getColumnNameIndex(columnLabel));
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    return getAsciiStream(getColumnNameIndex(columnLabel));
  }

  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    return getUnicodeStream(getColumnNameIndex(columnLabel));
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    return getBinaryStream(getColumnNameIndex(columnLabel));
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    checkIfClosed();
    return warnings;
  }

  @Override
  public void clearWarnings() throws SQLException {
    checkIfClosed();
    warnings = null;
  }

  @Override
  public String getCursorName() throws SQLException {
    checkIfClosed();
    return EMPTY_STRING;
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return resultSetMetaData;
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    checkIfClosed();
    Object obj = getObjectInternal(columnIndex);
    if (obj == null) {
      return null;
    }
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    String columnName = resultSetMetaData.getColumnTypeName(columnIndex);
    // separate handling for complex data types
    if (columnName.equals("ARRAY") || columnName.equals("MAP") || columnName.equals("STRUCT")) {
      return handleComplexDataTypes(obj, columnIndex);
    }
    return ConverterHelper.convertSqlTypeToJavaType(columnType, obj);
  }

  private Object handleComplexDataTypes(Object obj, int columnIndex) {
    if (complexDatatypeSupport) return obj;
    return obj.toString();
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    checkIfClosed();
    return getObject(getColumnNameIndex(columnLabel));
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    checkIfClosed();
    return getColumnNameIndex(columnLabel);
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    checkIfClosed();
    Object obj = getObjectInternal(columnIndex);
    if (obj == null) {
      return null;
    }
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    ObjectConverter converter = ConverterHelper.getConverterForSqlType(columnType);
    return converter.toCharacterStream(obj);
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    return getCharacterStream(getColumnNameIndex(columnLabel));
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    return getBigDecimal(columnIndex, resultSetMetaData.getScale(columnIndex));
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    return getBigDecimal(getColumnNameIndex(columnLabel));
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    checkIfClosed();
    return executionResult.getCurrentRow() == -1;
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    checkIfClosed();
    return executionResult.getCurrentRow() >= resultSetMetaData.getTotalRows();
  }

  @Override
  public boolean isFirst() throws SQLException {
    checkIfClosed();
    return executionResult.getCurrentRow() == 0;
  }

  @Override
  public boolean isLast() throws SQLException {
    checkIfClosed();
    return executionResult.getCurrentRow() == resultSetMetaData.getTotalRows() - 1;
  }

  @Override
  public void beforeFirst() throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC does not support random access (beforeFirst)");
  }

  @Override
  public void afterLast() throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC does not support random access (afterLast)");
  }

  @Override
  public boolean first() throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC does not support random access (first)");
  }

  @Override
  public boolean last() throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC does not support random access (last)");
  }

  @Override
  public int getRow() throws SQLException {
    checkIfClosed();
    return (int) executionResult.getCurrentRow() + 1;
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    checkIfClosed();
    if (row < 1 || row < executionResult.getCurrentRow()) {
      throw new DatabricksSQLFeatureNotSupportedException(
          "Invalid operation for forward only ResultSets");
    }
    while (executionResult.getCurrentRow() < row - 1) {
      if (!next()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC does not support random access (relative)");
  }

  @Override
  public boolean previous() throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC does not support random access (previous)");
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    checkIfClosed();
    // Only allow forward direction
    if (direction != ResultSet.FETCH_FORWARD) {
      throw new DatabricksSQLFeatureNotSupportedException(
          "Databricks JDBC only supports FETCH_FORWARD direction");
    }
  }

  @Override
  public int getFetchDirection() throws SQLException {
    checkIfClosed();
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    /* As we fetch chunks of data together,
    setting fetchSize is an overkill.
    Hence, we don't support it.*/
    LOGGER.debug(String.format("public void setFetchSize(int rows = {%s})", rows));
    checkIfClosed();
    addWarningAndLog("As FetchSize is not supported in the Databricks JDBC, ignoring it");
  }

  @Override
  public int getFetchSize() throws SQLException {
    LOGGER.debug("public int getFetchSize()");
    checkIfClosed();
    addWarningAndLog(
        "As FetchSize is not supported in the Databricks JDBC, we don't set it in the first place");
    return 0;
  }

  @Override
  public int getType() throws SQLException {
    checkIfClosed();
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public int getConcurrency() throws SQLException {
    checkIfClosed();
    return ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support the function : rowUpdated");
  }

  @Override
  public boolean rowInserted() throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support the function : rowInserted");
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support the function : rowDeleted");
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateNull");
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateBoolean");
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateByte");
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateShort");
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateInt");
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateLong");
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateFloat");
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateDouble");
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateBigDecimal");
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateString");
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateBytes");
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateDate");
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateTime");
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateTimestamp");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateAsciiStream");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateBinaryStream");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateCharacterStream");
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateObject");
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateObject");
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateNull");
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateBoolean");
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateByte");
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateShort");
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateInt");
  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateLong");
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateFloat");
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateDouble");
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateBigDecimal");
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateString");
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateBytes");
  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateDate");
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateTime");
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateTimestamp");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateAsciiStream");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateBinaryStream");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length)
      throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateCharacterStream");
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateObject");
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateObject");
  }

  @Override
  public void insertRow() throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support insert function : insertRow");
  }

  @Override
  public void updateRow() throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateRow");
  }

  @Override
  public void deleteRow() throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support deleteRow.");
  }

  @Override
  public void refreshRow() throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : refreshRow");
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support any row updates in the first place.");
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support moveToInsertRow.");
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support deleteRow.");
  }

  @Override
  public Statement getStatement() throws SQLException {
    checkIfClosed();
    /*
     *Retrieves the Statement object that produced this ResultSet object.
     *In case the resultSet is produced as a response of meta-data operations, this method returns null.
     */
    return (parentStatement != null) ? parentStatement.getStatement() : null;
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    checkIfClosed();
    Object obj = getObjectInternal(columnIndex);
    if (obj == null) {
      return null;
    }
    int columnSqlType = resultSetMetaData.getColumnType(columnIndex);
    String columnTypeText = resultSetMetaData.getColumnTypeName(columnIndex);
    Class<?> returnObjectType = map.get(columnTypeText);
    if (returnObjectType != null) {
      try {
        return ConverterHelper.convertSqlTypeToSpecificJavaType(
            returnObjectType, columnSqlType, obj);
      } catch (Exception e) {
        addWarningAndLog(
            "Exception occurred while converting object into corresponding return object type using getObject(int columnIndex, Map<String, Class<?>> map). Returning null. Exception: "
                + e.getMessage());
        return null;
      }
    }
    addWarningAndLog(
        "Corresponding return object type not found while using getObject(int columnIndex, Map<String, Class<?>> map). Returning null. Object type: "
            + columnTypeText);
    return null;
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksResultSet - getRef(int columnIndex)");
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksResultSet - getBlob(int columnIndex)");
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksResultSet - getClob(int columnIndex)");
  }

  @Override
  /**
   * Retrieves the SQL `Array` from the specified column index in the result set.
   *
   * @param columnIndex the index of the column in the result set (1-based)
   * @return an `Array` object if the column contains an array; `null` if the value is SQL `NULL`
   * @throws SQLException if the column is not of `ARRAY` type or if any SQL error occurs
   */
  public Array getArray(int columnIndex) throws SQLException {
    LOGGER.debug("Getting Array from column index: {}", columnIndex);
    if (!complexDatatypeSupport) {
      throw new SQLException("Complex datatype support support is disabled");
    }
    if (this.resultSetType.equals(ResultSetType.THRIFT_INLINE)
        || this.resultSetType.equals(ResultSetType.SEA_INLINE)) {
      throw new SQLException("Complex data types are not supported in inline mode");
    }
    checkIfClosed();
    Object obj = getObjectInternal(columnIndex);

    return (DatabricksArray) obj;
  }

  /**
   * Retrieves the SQL `Struct` from the specified column index in the result set.
   *
   * @param columnIndex the index of the column in the result set (1-based)
   * @return a `Struct` object if the column contains a struct; `null` if the value is SQL `NULL`
   * @throws SQLException if the column is not of `STRUCT` type or if any SQL error occurs
   */
  @Override
  public Struct getStruct(int columnIndex) throws SQLException {
    LOGGER.debug("Getting Struct from column index: {}", columnIndex);
    if (!complexDatatypeSupport) {
      throw new SQLException("Complex datatype support support is disabled");
    }
    if (this.resultSetType.equals(ResultSetType.THRIFT_INLINE)
        || this.resultSetType.equals(ResultSetType.SEA_INLINE)) {
      throw new SQLException("Complex data types are not supported in inline mode");
    }
    checkIfClosed();
    Object obj = getObjectInternal(columnIndex);

    return (DatabricksStruct) obj;
  }

  /**
   * Retrieves the SQL `Map` from the specified column index in the result set.
   *
   * @param columnIndex the index of the column in the result set (1-based)
   * @return a `Map<String, Object>` if the column contains a map; `null` if the value is SQL `NULL`
   * @throws SQLException if the column is not of `MAP` type or if any SQL error occurs
   */
  @Override
  public Map getMap(int columnIndex) throws SQLException {
    LOGGER.debug("Getting Map from column index: {}", columnIndex);
    if (!complexDatatypeSupport) {
      throw new SQLException("Complex datatype support support is disabled");
    }
    if (this.resultSetType.equals(ResultSetType.THRIFT_INLINE)
        || this.resultSetType.equals(ResultSetType.SEA_INLINE)) {
      throw new SQLException("Complex data types are not supported in inline mode");
    }
    checkIfClosed();
    Object obj = getObjectInternal(columnIndex);

    return (Map<String, Object>) obj;
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    return getObject(getColumnNameIndex(columnLabel), map);
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    checkIfClosed();
    return getRef(getColumnNameIndex(columnLabel));
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    checkIfClosed();
    return getBlob(getColumnNameIndex(columnLabel));
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    checkIfClosed();
    return getClob(getColumnNameIndex(columnLabel));
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException {
    checkIfClosed();
    return getArray(getColumnNameIndex(columnLabel));
  }

  public Struct getStruct(String columnLabel) throws SQLException {
    checkIfClosed();
    return getStruct(getColumnNameIndex(columnLabel));
  }

  public Map getMap(String columnLabel) throws SQLException {
    checkIfClosed();
    return getMap(getColumnNameIndex(columnLabel));
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    Date date = getDate(columnIndex);

    // For date columns, we only get the date, calibrate it to the provided calendar timezone
    if (date != null && cal != null) {
      Instant instant = Instant.ofEpochMilli(date.getTime());
      LocalDate localDate = LocalDate.ofInstant(instant, cal.getTimeZone().toZoneId());
      return Date.valueOf(localDate);
    }

    return date;
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    checkIfClosed();
    return getDate(getColumnNameIndex(columnLabel), cal);
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    // In Databricks, we always get the epoch value directly from the server
    // Hence, no need to enforce calendar
    return getTime(columnIndex);
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    checkIfClosed();
    return getTime(getColumnNameIndex(columnLabel), cal);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    // In Databricks, we always get the epoch value directly from the server
    // Hence, no need to enforce calendar
    return getTimestamp(columnIndex);
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    return getTimestamp(getColumnNameIndex(columnLabel), cal);
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksResultSet - getURL(int columnIndex)");
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksResultSet - getURL(String columnLabel)");
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksResultSet - updateRef(int columnIndex, Ref x)");
  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksResultSet - updateRef(String columnLabel, Ref x)");
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksResultSet - updateBlob(int columnIndex, Blob x)");
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksResultSet - updateBlob(String columnLabel, Blob x)");
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksResultSet - updateClob(int columnIndex, Clob x)");
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksResultSet - updateClob(String columnLabel, Clob x)");
  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksResultSet - updateArray(int columnIndex, Array x)");
  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksResultSet - updateArray(String columnLabel, Array x)");
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksResultSet - getRowId(int columnIndex)");
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksResultSet - getRowId(String columnLabel)");
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksResultSet - updateRowId(int columnIndex, RowId x)");
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksResultSet - updateRowId(String columnLabel, RowId x)");
  }

  @Override
  public int getHoldability() throws SQLException {
    checkIfClosed();
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateNString");
  }

  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateNString");
  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateNClob");
  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateNClob");
  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksResultSet - getNClob(int columnIndex)");
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksResultSet - getNClob(String columnLabel)");
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksResultSet - getSQLXML(int columnIndex)");
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksResultSet - getSQLXML(String columnLabel)");
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateSQLXML");
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateSQLXML");
  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksResultSet - getNString(int columnIndex)");
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksResultSet - getNString(String columnLabel)");
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksResultSet - getNCharacterStream(int columnIndex)");
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksResultSet - getNCharacterStream(String columnLabel)");
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateNCharacterStream");
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateNCharacterStream");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateAsciiStream");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateBinaryStream");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateCharacterStream");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateAsciiStream");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateBinaryStream");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateCharacterStream");
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateBlob");
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length)
      throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateBlob");
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateClob");
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateClob");
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateNClob");
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateNClob");
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateNCharacterStream");
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateNCharacterStream");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateAsciiStream");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateBinaryStream");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateCharacterStream");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateAsciiStream");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateBinaryStream");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateCharacterStream");
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateBlob");
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateBlob");
  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateClob");
  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateClob");
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateNClob");
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC has ResultSet as CONCUR_READ_ONLY. Doesn't support update function : updateNClob");
  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    checkIfClosed();
    Object obj = getObjectInternal(columnIndex);
    int columnSqlType = resultSetMetaData.getColumnType(columnIndex);
    try {
      return (T) ConverterHelper.convertSqlTypeToSpecificJavaType(type, columnSqlType, obj);
    } catch (Exception e) {
      String errorMessage =
          String.format(
              "Exception occurred while converting object into corresponding return object type using getObject(int columnIndex, Class<T> type). ErrorMessage: %s",
              e.getMessage());
      LOGGER.error(errorMessage);
      throw new DatabricksSQLException(errorMessage, e, DatabricksDriverErrorCode.INVALID_STATE);
    }
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    return getObject(getColumnNameIndex(columnLabel), type);
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    LOGGER.debug("public <T> T unwrap(Class<T> iface)");
    if (iface.isInstance(this)) {
      return (T) this;
    }
    throw new DatabricksValidationException(
        String.format(
            "Class {%s} cannot be wrapped from {%s}", this.getClass().getName(), iface.getName()));
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    LOGGER.debug("public boolean isWrapperFor(Class<?> iface)");
    return iface.isInstance(this);
  }

  @Override
  public String getStatementId() {
    return statementId.toString();
  }

  @Override
  public StatementStatus getStatementStatus() {
    return statementStatus;
  }

  @Override
  public long getUpdateCount() throws SQLException {
    checkIfClosed();
    if (updateCount != null) {
      return updateCount;
    }
    if (this.statementType == StatementType.METADATA || this.statementType == StatementType.QUERY) {
      updateCount = 0L;
    } else if (hasUpdateCount()) {
      long rowsUpdated = 0;
      while (next()) {
        rowsUpdated += this.getLong(AFFECTED_ROWS_COUNT);
      }
      updateCount = rowsUpdated;
    } else {
      updateCount = 0L;
    }
    return updateCount;
  }

  @Override
  public boolean hasUpdateCount() throws SQLException {
    checkIfClosed();
    if (this.statementType == StatementType.UPDATE) {
      return true;
    }
    return this.resultSetMetaData.getColumnNameIndex(AFFECTED_ROWS_COUNT) > -1
        && this.resultSetMetaData.getTotalRows() == 1;
  }

  @Override
  public InputStreamEntity getVolumeOperationInputStream() throws SQLException {
    checkIfClosed();
    if (executionResult instanceof VolumeOperationResult) {
      return ((VolumeOperationResult) executionResult).getVolumeOperationInputStream();
    }
    throw new DatabricksValidationException("Invalid volume operation");
  }

  private void addWarningAndLog(String warningMessage) {
    LOGGER.warn(warningMessage);
    warnings = WarningUtil.addWarning(warnings, warningMessage);
  }

  private Object getObjectInternal(int columnIndex) throws SQLException {
    if (columnIndex <= 0) {
      throw new DatabricksSQLException(
          "Invalid column index", DatabricksDriverErrorCode.INVALID_STATE);
    }
    Object object = executionResult.getObject(columnIndex - 1);
    this.wasNull = object == null;
    return object;
  }

  private int getColumnNameIndex(String columnName) {
    return this.resultSetMetaData.getColumnNameIndex(columnName);
  }

  private void checkIfClosed() throws SQLException {
    if (this.isClosed) {
      throw new DatabricksSQLException(
          "Operation not allowed - ResultSet is closed",
          DatabricksDriverErrorCode.RESULT_SET_CLOSED);
    }
  }

  @FunctionalInterface
  private interface ConverterFunction<T> {
    T apply(ObjectConverter converter, Object obj) throws SQLException;
  }

  private <T> T getConvertedObject(
      int columnIndex, ConverterFunction<T> convertMethod, Supplier<T> defaultValue)
      throws SQLException {
    checkIfClosed();
    Object obj = getObjectInternal(columnIndex);
    if (obj == null) {
      return defaultValue.get();
    }
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    ObjectConverter converter = ConverterHelper.getConverterForSqlType(columnType);
    return convertMethod.apply(converter, obj);
  }

  private BigDecimal applyScaleToBigDecimal(BigDecimal bigDecimal, int columnIndex, int scale)
      throws SQLException {
    if (bigDecimal == null) {
      return null;
    }
    // Double/Float columns do not have scale defined, hence, return them at full scale
    if (resultSetMetaData.getColumnType(columnIndex) == Types.DOUBLE
        || resultSetMetaData.getColumnType(columnIndex) == Types.FLOAT) {
      return bigDecimal;
    }
    return bigDecimal.setScale(scale, RoundingMode.HALF_UP);
  }

  @Override
  public String toString() {
    return (new ToStringer(DatabricksResultSet.class))
        .add("statementStatus", this.statementStatus)
        .add("statementId", this.statementId)
        .add("statementType", this.statementType)
        .add("updateCount", this.updateCount)
        .add("isClosed", this.isClosed)
        .add("wasNull", this.wasNull)
        .add("resultSetType", this.resultSetType)
        .toString();
  }
}
