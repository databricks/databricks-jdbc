package com.databricks.jdbc.core;

import com.databricks.jdbc.client.StatementType;
import com.databricks.jdbc.client.sqlexec.ResultData;
import com.databricks.jdbc.core.converters.*;
import com.databricks.sdk.service.sql.ResultManifest;
import com.databricks.sdk.service.sql.StatementStatus;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

public class DatabricksResultSet implements ResultSet, IDatabricksResultSet {
  private static final String DECIMAL = ".";
  private static final String AFFECTED_ROWS_COUNT = "num_affected_rows";
  private final StatementStatus statementStatus;
  private final String statementId;
  private final IExecutionResult executionResult;
  private final DatabricksResultSetMetaData resultSetMetaData;
  private final StatementType statementType;
  private final IDatabricksStatement parentStatement;
  private Long updateCount;
  private boolean isClosed;

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
    this.resultSetMetaData = new DatabricksResultSetMetaData(statementId, resultManifest, session);
    this.statementType = statementType;
    this.updateCount = null;
    this.parentStatement = parentStatement;
    this.isClosed = false;
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
    // TODO: fix implementation
    return this == null;
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
    throw new UnsupportedOperationException(
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
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getAsciiStream(int columnIndex)");
  }

  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getUnicodeStream(int columnIndex)");
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getBinaryStream(int columnIndex)");
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    checkIfClosed();
    return getString(getColumnNameIndex(columnLabel));
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    checkIfClosed();
    return getBoolean(getColumnNameIndex(columnLabel));
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    checkIfClosed();
    return getByte(getColumnNameIndex(columnLabel));
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    checkIfClosed();
    return getShort(getColumnNameIndex(columnLabel));
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    checkIfClosed();
    return getInt(getColumnNameIndex(columnLabel));
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    checkIfClosed();
    return getLong(getColumnNameIndex(columnLabel));
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    checkIfClosed();
    return getFloat(getColumnNameIndex(columnLabel));
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    checkIfClosed();
    return getDouble(getColumnNameIndex(columnLabel));
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    checkIfClosed();
    return getBigDecimal(getColumnNameIndex(columnLabel));
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    checkIfClosed();
    return getBytes(getColumnNameIndex(columnLabel));
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    checkIfClosed();
    return getDate(getColumnNameIndex(columnLabel));
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getTime(String columnLabel)");
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    checkIfClosed();
    return getTimestamp(getColumnNameIndex(columnLabel));
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getAsciiStream(String columnLabel)");
  }

  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getUnicodeStream(String columnLabel)");
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getBinaryStream(String columnLabel)");
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getWarnings()");
  }

  @Override
  public void clearWarnings() throws SQLException {
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - clearWarnings()");
  }

  @Override
  public String getCursorName() throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getCursorName()");
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return resultSetMetaData;
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    checkIfClosed();
    return getObjectInternal(columnIndex);
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    checkIfClosed();
    return getObject(getColumnNameIndex(columnLabel));
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - findColumn(String columnLabel)");
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getCharacterStream(int columnIndex)");
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getCharacterStream(String columnLabel)");
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    checkIfClosed();
    return getBigDecimal(columnIndex, resultSetMetaData.getScale(columnIndex));
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    checkIfClosed();
    return getBigDecimal(getColumnNameIndex(columnLabel));
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    checkIfClosed();
    // TODO: Madhav to check the best way to implement this, below is not correct
    return !executionResult.hasNext();
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - isAfterLast()");
  }

  @Override
  public boolean isFirst() throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException("Not implemented in DatabricksResultSet - isFirst()");
  }

  @Override
  public boolean isLast() throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException("Not implemented in DatabricksResultSet - isLast()");
  }

  @Override
  public void beforeFirst() throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - beforeFirst()");
  }

  @Override
  public void afterLast() throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException("Not implemented in DatabricksResultSet - afterLast()");
  }

  @Override
  public boolean first() throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException("Not implemented in DatabricksResultSet - first()");
  }

  @Override
  public boolean last() throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException("Not implemented in DatabricksResultSet - last()");
  }

  @Override
  public int getRow() throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException("Not implemented in DatabricksResultSet - getRow()");
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - absolute(int row)");
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - relative(int rows)");
  }

  @Override
  public boolean previous() throws SQLException {
    checkIfClosed();
    // We only support forward direction
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    checkIfClosed();
    // Only allow forward direction
    if (direction != ResultSet.FETCH_FORWARD) {
      throw new SQLFeatureNotSupportedException();
    }
  }

  @Override
  public int getFetchDirection() throws SQLException {
    checkIfClosed();
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - setFetchSize(int rows)");
  }

  @Override
  public int getFetchSize() throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getFetchSize()");
  }

  @Override
  public int getType() throws SQLException {
    throw new UnsupportedOperationException("Not implemented in DatabricksResultSet - getType()");
  }

  @Override
  public int getConcurrency() throws SQLException {
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getConcurrency()");
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - rowUpdated()");
  }

  @Override
  public boolean rowInserted() throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - rowInserted()");
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - rowDeleted()");
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateNull(int columnIndex)");
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateBoolean(int columnIndex, boolean x)");
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateByte(int columnIndex, byte x)");
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateShort(int columnIndex, short x)");
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateInt(int columnIndex, int x)");
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateLong(int columnIndex, long x)");
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateFloat(int columnIndex, float x)");
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateDouble(int columnIndex, double x)");
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateBigDecimal(int columnIndex, BigDecimal x)");
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateString(int columnIndex, String x)");
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateBytes(int columnIndex, byte[] x)");
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateDate(int columnIndex, Date x)");
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateTime(int columnIndex, Time x)");
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateTimestamp(int columnIndex, Timestamp x)");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateAsciiStream(int columnIndex, InputStream x, int length)");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateBinaryStream(int columnIndex, InputStream x, int length)");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateCharacterStream(int columnIndex, Reader x, int length)");
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateObject(int columnIndex, Object x, int scaleOrLength)");
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateObject(int columnIndex, Object x)");
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateNull(String columnLabel)");
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateBoolean(String columnLabel, boolean x)");
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateByte(String columnLabel, byte x)");
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateShort(String columnLabel, short x)");
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateInt(String columnLabel, int x)");
  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateLong(String columnLabel, long x)");
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateFloat(String columnLabel, float x)");
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateDouble(String columnLabel, double x)");
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateBigDecimal(String columnLabel, BigDecimal x)");
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateString(String columnLabel, String x)");
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateBytes(String columnLabel, byte[] x)");
  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateDate(String columnLabel, Date x)");
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateTime(String columnLabel, Time x)");
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateTimestamp(String columnLabel, Timestamp x)");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateAsciiStream(String columnLabel, InputStream x, int length)");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateBinaryStream(String columnLabel, InputStream x, int length)");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length)
      throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateCharacterStream(String columnLabel, Reader reader, int length)");
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateObject(String columnLabel, Object x, int scaleOrLength)");
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateObject(String columnLabel, Object x)");
  }

  @Override
  public void insertRow() throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException("Not implemented in DatabricksResultSet - insertRow()");
  }

  @Override
  public void updateRow() throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException("Not implemented in DatabricksResultSet - updateRow()");
  }

  @Override
  public void deleteRow() throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException("Not implemented in DatabricksResultSet - deleteRow()");
  }

  @Override
  public void refreshRow() throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - refreshRow()");
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - cancelRowUpdates()");
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - moveToInsertRow()");
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - moveToCurrentRow()");
  }

  @Override
  public Statement getStatement() throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getStatement()");
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getObject(int columnIndex, Map<String, Class<?>> map)");
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getRef(int columnIndex)");
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getBlob(int columnIndex)");
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getClob(int columnIndex)");
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getArray(int columnIndex)");
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getObject(String columnLabel, Map<String, Class<?>> map)");
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
    throw new UnsupportedOperationException(
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
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getTime(int columnIndex, Calendar cal)");
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    checkIfClosed();
    return getTime(getColumnNameIndex(columnLabel), cal);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getTimestamp(int columnIndex, Calendar cal)");
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getTimestamp(String columnLabel, Calendar cal)");
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getURL(int columnIndex)");
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getURL(String columnLabel)");
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateRef(int columnIndex, Ref x)");
  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateRef(String columnLabel, Ref x)");
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateBlob(int columnIndex, Blob x)");
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateBlob(String columnLabel, Blob x)");
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateClob(int columnIndex, Clob x)");
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateClob(String columnLabel, Clob x)");
  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateArray(int columnIndex, Array x)");
  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateArray(String columnLabel, Array x)");
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getRowId(int columnIndex)");
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getRowId(String columnLabel)");
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateRowId(int columnIndex, RowId x)");
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateRowId(String columnLabel, RowId x)");
  }

  @Override
  public int getHoldability() throws SQLException {
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getHoldability()");
  }

  @Override
  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateNString(int columnIndex, String nString)");
  }

  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateNString(String columnLabel, String nString)");
  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateNClob(int columnIndex, NClob nClob)");
  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateNClob(String columnLabel, NClob nClob)");
  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getNClob(int columnIndex)");
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getNClob(String columnLabel)");
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getSQLXML(int columnIndex)");
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getSQLXML(String columnLabel)");
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateSQLXML(int columnIndex, SQLXML xmlObject)");
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateSQLXML(String columnLabel, SQLXML xmlObject)");
  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getNString(int columnIndex)");
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getNString(String columnLabel)");
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getNCharacterStream(int columnIndex)");
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getNCharacterStream(String columnLabel)");
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateNCharacterStream(int columnIndex, Reader x, long length)");
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateNCharacterStream(String columnLabel, Reader reader, long length)");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateAsciiStream(int columnIndex, InputStream x, long length)");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateBinaryStream(int columnIndex, InputStream x, long length)");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateCharacterStream(int columnIndex, Reader x, long length)");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateAsciiStream(String columnLabel, InputStream x, long length)");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateBinaryStream(String columnLabel, InputStream x, long length)");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateCharacterStream(String columnLabel, Reader reader, long length)");
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateBlob(int columnIndex, InputStream inputStream, long length)");
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length)
      throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateBlob(String columnLabel, InputStream inputStream, long length)");
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateClob(int columnIndex, Reader reader, long length)");
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateClob(String columnLabel, Reader reader, long length)");
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateNClob(int columnIndex, Reader reader, long length)");
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateNClob(String columnLabel, Reader reader, long length)");
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateNCharacterStream(int columnIndex, Reader x)");
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateNCharacterStream(String columnLabel, Reader reader)");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateAsciiStream(int columnIndex, InputStream x)");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateBinaryStream(int columnIndex, InputStream x)");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateCharacterStream(int columnIndex, Reader x)");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateAsciiStream(String columnLabel, InputStream x)");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateBinaryStream(String columnLabel, InputStream x)");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateCharacterStream(String columnLabel, Reader reader)");
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateBlob(int columnIndex, InputStream inputStream)");
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateBlob(String columnLabel, InputStream inputStream)");
  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateClob(int columnIndex, Reader reader)");
  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateClob(String columnLabel, Reader reader)");
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateNClob(int columnIndex, Reader reader)");
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - updateNClob(String columnLabel, Reader reader)");
  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getObject(int columnIndex, Class<T> type)");
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - getObject(String columnLabel, Class<T> type)");
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksResultSet - unwrap(Class<T> iface)");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new UnsupportedOperationException(
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

  private Object getObjectInternal(int columnIndex) throws SQLException {
    if (columnIndex <= 0) {
      throw new DatabricksSQLException("Invalid column index");
    }
    // Handle null and other errors
    return executionResult.getObject(columnIndex - 1);
  }

  /** For String values, return value without decimal fraction */
  private String getNumberStringWithoutDecimal(String s, int columnType) {
    if (s.contains(DECIMAL) && (columnType == Types.DOUBLE || columnType == Types.FLOAT)) {
      return s.substring(0, s.indexOf(DECIMAL));
    }
    return s;
  }

  private AbstractObjectConverter getObjectConverter(Object object, int columnType)
      throws DatabricksSQLException {
    switch (columnType) {
      case Types.TINYINT:
        return new ByteConverter(object);
      case Types.SMALLINT:
        return new ShortConverter(object);
      case Types.INTEGER:
        return new IntConverter(object);
      case Types.BIGINT:
        return new LongConverter(object);
      case Types.FLOAT:
        return new FloatConverter(object);
      case Types.DOUBLE:
        return new DoubleConverter(object);
      case Types.DECIMAL:
        return new BigDecimalConverter(object);
      case Types.BOOLEAN:
        return new BooleanConverter(object);
      case Types.VARCHAR:
      case Types.CHAR:
        return new StringConverter(object);
      case Types.DATE:
        return new DateConverter(object);
      case Types.TIMESTAMP:
        return new TimestampConverter(object);
      default:
        throw new DatabricksSQLException("Bad object type");
    }
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
