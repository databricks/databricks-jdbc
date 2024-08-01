package com.databricks.jdbc.core;

import static com.databricks.jdbc.client.impl.thrift.commons.DatabricksThriftHelper.*;
import static com.databricks.jdbc.core.converters.ConverterHelper.getConvertedObject;
import static com.databricks.jdbc.core.converters.ConverterHelper.getObjectConverter;

import com.databricks.jdbc.client.StatementType;
import com.databricks.jdbc.client.impl.thrift.generated.TGetResultSetMetadataResp;
import com.databricks.jdbc.client.impl.thrift.generated.TRowSet;
import com.databricks.jdbc.client.impl.thrift.generated.TStatus;
import com.databricks.jdbc.client.sqlexec.ResultData;
import com.databricks.jdbc.client.sqlexec.ResultManifest;
import com.databricks.jdbc.commons.LogLevel;
import com.databricks.jdbc.commons.util.LoggingUtil;
import com.databricks.jdbc.commons.util.WarningUtil;
import com.databricks.jdbc.core.converters.*;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpEntity;

public class DatabricksResultSet implements ResultSet, IDatabricksResultSet {
  protected static final String AFFECTED_ROWS_COUNT = "num_affected_rows";
  private final StatementStatus statementStatus;
  private final String statementId;
  private final IExecutionResult executionResult;
  private final DatabricksResultSetMetaData resultSetMetaData;
  private final StatementType statementType;
  private final IDatabricksStatement parentStatement;
  private Long updateCount;
  private boolean isClosed;
  private SQLWarning warnings = null;
  private boolean wasNull;
  private VolumeInputStream volumeInputStream = null;

  public DatabricksResultSet(
      StatementStatus statementStatus,
      String statementId,
      ResultData resultData,
      ResultManifest resultManifest,
      StatementType statementType,
      IDatabricksSession session,
      IDatabricksStatement parentStatement) {
    this.statementStatus = statementStatus;
    this.statementId = statementId;
    this.executionResult =
        ExecutionResultFactory.getResultSet(resultData, resultManifest, statementId, session);
    this.resultSetMetaData = new DatabricksResultSetMetaData(statementId, resultManifest);
    this.statementType = statementType;
    this.updateCount = null;
    this.parentStatement = parentStatement;
    this.isClosed = false;
    this.wasNull = false;
  }

  @VisibleForTesting
  public DatabricksResultSet(
      StatementStatus statementStatus,
      String statementId,
      StatementType statementType,
      IDatabricksStatement parentStatement,
      IExecutionResult executionResult,
      DatabricksResultSetMetaData resultSetMetaData) {
    this.statementStatus = statementStatus;
    this.statementId = statementId;
    this.executionResult = executionResult;
    this.resultSetMetaData = resultSetMetaData;
    this.statementType = statementType;
    this.updateCount = null;
    this.parentStatement = parentStatement;
    this.isClosed = false;
    this.wasNull = false;
  }

  public DatabricksResultSet(
      TStatus statementStatus,
      String statementId,
      TRowSet resultData,
      TGetResultSetMetadataResp resultManifest,
      StatementType statementType,
      IDatabricksStatement parentStatement,
      IDatabricksSession session)
      throws DatabricksSQLException {
    if (SUCCESS_STATUS_LIST.contains(statementStatus.getStatusCode())) {
      this.statementStatus = new StatementStatus().setState(StatementState.SUCCEEDED);
    } else {
      this.statementStatus = new StatementStatus().setState(StatementState.FAILED);
    }
    this.statementId = statementId;
    this.executionResult =
        ExecutionResultFactory.getResultSet(resultData, resultManifest, statementId, session);
    long rowSize = getRowCount(resultData);
    this.resultSetMetaData =
        new DatabricksResultSetMetaData(
            statementId, resultManifest, rowSize, resultData.getResultLinksSize());
    this.statementType = statementType;
    this.updateCount = null;
    this.parentStatement = parentStatement;
    this.isClosed = false;
    this.wasNull = false;
  }

  public DatabricksResultSet(
      StatementStatus statementStatus,
      String statementId,
      List<String> columnNames,
      List<String> columnTypeText,
      List<Integer> columnTypes,
      List<Integer> columnTypePrecisions,
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
            rows.length);
    this.statementType = statementType;
    this.updateCount = null;
    this.parentStatement = null;
    this.isClosed = false;
    this.wasNull = false;
  }

  public DatabricksResultSet(
      StatementStatus statementStatus,
      String statementId,
      List<String> columnNames,
      List<String> columnTypeText,
      List<Integer> columnTypes,
      List<Integer> columnTypePrecisions,
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
            rows.size());
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
  public void close() throws SQLException {
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

  // TODO (Madhav): Clean up code by removing code duplicity by having common functions that branch
  // out and to reuse converter objects.
  @Override
  public String getString(int columnIndex) throws SQLException {
    checkIfClosed();
    Object obj = getObjectInternal(columnIndex);
    if (obj == null) {
      return null;
    }
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    AbstractObjectConverter converter = getObjectConverter(obj, columnType);
    return converter.convertToString();
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    Object obj = getObjectInternal(columnIndex);
    if (obj == null) {
      return false;
    }
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    AbstractObjectConverter converter = getObjectConverter(obj, columnType);
    return converter.convertToBoolean();
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    checkIfClosed();
    Object obj = getObjectInternal(columnIndex);
    if (obj == null) {
      return 0;
    }
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    AbstractObjectConverter converter = getObjectConverter(obj, columnType);
    return converter.convertToByte();
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    checkIfClosed();
    Object obj = getObjectInternal(columnIndex);
    if (obj == null) {
      return 0;
    }
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    AbstractObjectConverter converter = getObjectConverter(obj, columnType);
    return converter.convertToShort();
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    checkIfClosed();
    Object obj = getObjectInternal(columnIndex);
    if (obj == null) {
      return 0;
    }
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    AbstractObjectConverter converter = getObjectConverter(obj, columnType);
    return converter.convertToInt();
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    checkIfClosed();
    Object obj = getObjectInternal(columnIndex);
    if (obj == null) {
      return 0;
    }
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    AbstractObjectConverter converter = getObjectConverter(obj, columnType);
    return converter.convertToLong();
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    checkIfClosed();
    Object obj = getObjectInternal(columnIndex);
    if (obj == null) {
      return 0f;
    }
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    AbstractObjectConverter converter = getObjectConverter(obj, columnType);
    return converter.convertToFloat();
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    checkIfClosed();
    Object obj = getObjectInternal(columnIndex);
    if (obj == null) {
      return 0;
    }
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    AbstractObjectConverter converter = getObjectConverter(obj, columnType);
    return converter.convertToDouble();
  }

  // TODO (Madhav): Handle case when scale is not provided when getScale is implemented.
  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    checkIfClosed();
    Object obj = getObjectInternal(columnIndex);
    if (obj == null) {
      return BigDecimal.ZERO;
    }
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    AbstractObjectConverter converter = getObjectConverter(obj, columnType);
    return converter.convertToBigDecimal();
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    checkIfClosed();
    Object obj = getObjectInternal(columnIndex);
    if (obj == null) {
      return null;
    }
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    AbstractObjectConverter converter = getObjectConverter(obj, columnType);
    return converter.convertToByteArray();
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    checkIfClosed();
    Object obj = getObjectInternal(columnIndex);
    if (obj == null) {
      return null;
    }
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    AbstractObjectConverter converter = getObjectConverter(obj, columnType);
    return converter.convertToDate();
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksResultSet - getTime(int columnIndex)");
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    checkIfClosed();
    Object obj = getObjectInternal(columnIndex);
    if (obj == null) {
      return null;
    }
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    AbstractObjectConverter converter = getObjectConverter(obj, columnType);
    return converter.convertToTimestamp();
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    checkIfClosed();
    Object obj = getObjectInternal(columnIndex);
    if (obj == null) {
      return null;
    }
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    AbstractObjectConverter converter = getObjectConverter(obj, columnType);
    return converter.convertToAsciiStream();
  }

  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    checkIfClosed();
    Object obj = getObjectInternal(columnIndex);
    if (obj == null) {
      return null;
    }
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    AbstractObjectConverter converter = getObjectConverter(obj, columnType);
    return converter.convertToUnicodeStream();
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    checkIfClosed();
    Object obj = getObjectInternal(columnIndex);
    if (obj == null) {
      return null;
    }
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    AbstractObjectConverter converter = getObjectConverter(obj, columnType);
    return converter.convertToBinaryStream();
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
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not supported in DatabricksResultSet - getCursorName()");
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
    return getConvertedObject(columnType, obj);
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
    AbstractObjectConverter converter = getObjectConverter(obj, columnType);
    return converter.convertToCharacterStream();
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
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks JDBC does not support random access (absolute)");
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
    LoggingUtil.log(
        LogLevel.DEBUG, String.format("public void setFetchSize(int rows = {%s})", rows));
    checkIfClosed();
    addWarningAndLog("As FetchSize is not supported in the Databricks JDBC, ignoring it");
  }

  @Override
  public int getFetchSize() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getFetchSize()");
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
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    String columnTypeText = resultSetMetaData.getColumnTypeName(columnIndex);
    Class<?> returnObjectType = map.get(columnTypeText);
    if (returnObjectType != null) {
      try {
        AbstractObjectConverter converter = getObjectConverter(obj, columnType);
        return getConvertedObject(returnObjectType, converter);
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

  private void addWarningAndLog(String warningMessage) {
    LoggingUtil.log(LogLevel.WARN, warningMessage);
    warnings = WarningUtil.addWarning(warnings, warningMessage);
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
  public Array getArray(int columnIndex) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksResultSet - getArray(int columnIndex)");
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

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksResultSet - getDate(int columnIndex, Calendar cal)");
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    checkIfClosed();
    return getDate(getColumnNameIndex(columnLabel), cal);
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksResultSet - getTime(int columnIndex, Calendar cal)");
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    checkIfClosed();
    return getTime(getColumnNameIndex(columnLabel), cal);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    Timestamp defaultTimestamp = getTimestamp(columnIndex);

    if (defaultTimestamp != null && cal != null) {
      // Clone the calendar to avoid modifying the passed instance
      Calendar tempCal = (Calendar) cal.clone();
      tempCal.setTimeInMillis(defaultTimestamp.getTime());
      return new Timestamp(tempCal.getTimeInMillis());
    }

    return defaultTimestamp;
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
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksResultSet - getHoldability()");
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
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    try {
      AbstractObjectConverter converter = getObjectConverter(obj, columnType);
      return (T) getConvertedObject(type, converter);
    } catch (Exception e) {
      String errorMessage =
          String.format(
              "Exception occurred while converting object into corresponding return object type using getObject(int columnIndex, Class<T> type). ErrorMessage: %s",
              e.getMessage());
      LoggingUtil.log(LogLevel.ERROR, errorMessage);
      throw new DatabricksSQLException(errorMessage, e);
    }
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    return getObject(getColumnNameIndex(columnLabel), type);
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksResultSet - unwrap(Class<T> iface)");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksResultSet - isWrapperFor(Class<?> iface)");
  }

  @Override
  public String statementId() {
    return statementId;
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
  public void setVolumeOperationEntityStream(HttpEntity httpEntity)
      throws SQLException, IOException {
    checkIfClosed();
    this.volumeInputStream =
        new VolumeInputStream(httpEntity, executionResult, this.parentStatement);
  }

  @Override
  public InputStream getVolumeOperationInputStream() throws SQLException {
    checkIfClosed();
    return this.volumeInputStream;
  }

  private Object getObjectInternal(int columnIndex) throws SQLException {
    if (columnIndex <= 0) {
      throw new DatabricksSQLException("Invalid column index");
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
      throw new DatabricksSQLException("Operation not allowed - ResultSet is closed");
    }
  }
}
