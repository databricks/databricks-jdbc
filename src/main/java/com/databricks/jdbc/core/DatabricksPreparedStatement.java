package com.databricks.jdbc.core;

import static com.databricks.jdbc.core.DatabricksTypes.*;

import com.databricks.jdbc.client.StatementType;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabricksPreparedStatement extends DatabricksStatement implements PreparedStatement {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksPreparedStatement.class);
  private final String sql;
  private final Map<Integer, ImmutableSqlParameter> parameterBindings;

  public DatabricksPreparedStatement(DatabricksConnection connection, String sql) {
    super(connection);
    this.sql = sql;
    this.parameterBindings = new HashMap<Integer, ImmutableSqlParameter>();
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    LOGGER.debug("public ResultSet executeQuery()");
    return executeInternal(sql, parameterBindings, StatementType.QUERY);
  }

  @Override
  public int executeUpdate() throws SQLException {
    LOGGER.debug("public int executeUpdate()");
    executeInternal(sql, parameterBindings, StatementType.UPDATE);
    return (int) resultSet.getUpdateCount();
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    LOGGER.debug("public void setNull(int parameterIndex, int sqlType)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    LOGGER.debug("public void setBoolean(int parameterIndex, boolean x)");
    checkIfClosed();
    setObject(parameterIndex, x, DatabricksTypes.BOOLEAN);
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    LOGGER.debug("public void setByte(int parameterIndex, byte x)");
    checkIfClosed();
    setObject(parameterIndex, x, DatabricksTypes.TINYINT);
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    LOGGER.debug("public void setShort(int parameterIndex, short x)");
    checkIfClosed();
    setObject(parameterIndex, x, DatabricksTypes.SMALLINT);
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    LOGGER.debug("public void setInt(int parameterIndex, int x)");
    checkIfClosed();
    setObject(parameterIndex, x, DatabricksTypes.INT);
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    LOGGER.debug("public void setLong(int parameterIndex, long x)");
    checkIfClosed();
    setObject(parameterIndex, x, DatabricksTypes.BIGINT);
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    LOGGER.debug("public void setFloat(int parameterIndex, float x)");
    checkIfClosed();
    setObject(parameterIndex, x, DatabricksTypes.FLOAT);
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    LOGGER.debug("public void setDouble(int parameterIndex, double x)");
    checkIfClosed();
    setObject(parameterIndex, x, DatabricksTypes.DOUBLE);
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    LOGGER.debug("public void setBigDecimal(int parameterIndex, BigDecimal x)");
    checkIfClosed();
    setObject(parameterIndex, x, DatabricksTypes.DECIMAL);
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    LOGGER.debug("public void setString(int parameterIndex, String x)");
    checkIfClosed();
    setObject(parameterIndex, x, DatabricksTypes.STRING);
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    LOGGER.debug("public void setBytes(int parameterIndex, byte[] x)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException {
    LOGGER.debug("public void setDate(int parameterIndex, Date x)");
    checkIfClosed();
    setObject(parameterIndex, x, DatabricksTypes.DATE);
  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException {
    LOGGER.debug("public void setTime(int parameterIndex, Time x)");
    checkIfClosed();
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    LOGGER.debug("public void setTimestamp(int parameterIndex, Timestamp x)");
    checkIfClosed();
    setObject(parameterIndex, x, DatabricksTypes.TIMESTAMP);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    LOGGER.debug("public void setAsciiStream(int parameterIndex, InputStream x, int length)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    LOGGER.debug("public void setUnicodeStream(int parameterIndex, InputStream x, int length)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    LOGGER.debug("public void setBinaryStream(int parameterIndex, InputStream x, int length)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void clearParameters() throws SQLException {
    LOGGER.debug("public void clearParameters()");
    checkIfClosed();
    this.parameterBindings.clear();
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    LOGGER.debug("public void setObject(int parameterIndex, Object x, int targetSqlType)");
    checkIfClosed();
    String databricksType = getDatabricksTypeFromSQLType(targetSqlType);
    if (databricksType != null) {
      setObject(parameterIndex, x, databricksType);
      return;
    }
    // TODO: handle other types
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    LOGGER.debug("public void setObject(int parameterIndex, Object x)");
    checkIfClosed();
    String type = inferDatabricksType(x);
    if (type != null) {
      setObject(parameterIndex, x, type);
      return;
    }
    // TODO: handle other types and generic objects
    throw new UnsupportedOperationException("Not implemented");
  }

  private void setObject(int parameterIndex, Object x, String databricksType) {
    this.parameterBindings.put(
        parameterIndex,
        ImmutableSqlParameter.builder()
            .type(databricksType)
            .value(x)
            .cardinal(parameterIndex)
            .build());
  }

  @Override
  public boolean execute() throws SQLException {
    LOGGER.debug("public boolean execute()");
    checkIfClosed();
    executeInternal(sql, parameterBindings, StatementType.SQL);
    return !resultSet.hasUpdateCount();
  }

  @Override
  public void addBatch() throws SQLException {
    LOGGER.debug("public void addBatch()");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length)
      throws SQLException {
    LOGGER.debug("public void setCharacterStream(int parameterIndex, Reader reader, int length)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException {
    LOGGER.debug("public void setRef(int parameterIndex, Ref x)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    LOGGER.debug("public void setBlob(int parameterIndex, Blob x)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException {
    LOGGER.debug("public void setClob(int parameterIndex, Clob x)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException {
    LOGGER.debug("public void setArray(int parameterIndex, Array x)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    LOGGER.debug("public ResultSetMetaData getMetaData()");
    checkIfClosed();
    return resultSet.getMetaData();
  }

  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    LOGGER.debug("public void setDate(int parameterIndex, Date x, Calendar cal)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    LOGGER.debug("public void setTime(int parameterIndex, Time x, Calendar cal)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    LOGGER.debug("public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    LOGGER.debug("public void setNull(int parameterIndex, int sqlType, String typeName)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException {
    LOGGER.debug("public void setURL(int parameterIndex, URL x)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    LOGGER.debug("public ParameterMetaData getParameterMetaData()");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    LOGGER.debug("public void setRowId(int parameterIndex, RowId x)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException {
    LOGGER.debug("public void setNString(int parameterIndex, String value)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length)
      throws SQLException {
    LOGGER.debug("public void setNCharacterStream(int parameterIndex, Reader value, long length)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    LOGGER.debug("public void setNClob(int parameterIndex, NClob value)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    LOGGER.debug("public void setClob(int parameterIndex, Reader reader, long length)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length)
      throws SQLException {
    LOGGER.debug("public void setBlob(int parameterIndex, InputStream inputStream, long length)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    LOGGER.debug("public void setNClob(int parameterIndex, Reader reader, long length)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    LOGGER.debug("public void setSQLXML(int parameterIndex, SQLXML xmlObject)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
      throws SQLException {
    LOGGER.debug(
        "public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    LOGGER.debug("public void setAsciiStream(int parameterIndex, InputStream x, long length)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    LOGGER.debug("public void setBinaryStream(int parameterIndex, InputStream x, long length)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length)
      throws SQLException {
    LOGGER.debug("public void setCharacterStream(int parameterIndex, Reader reader, long length)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    LOGGER.debug("public void setAsciiStream(int parameterIndex, InputStream x)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    LOGGER.debug("public void setBinaryStream(int parameterIndex, InputStream x)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    LOGGER.debug("public void setCharacterStream(int parameterIndex, Reader reader)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    LOGGER.debug("public void setNCharacterStream(int parameterIndex, Reader value)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    LOGGER.debug("public void setClob(int parameterIndex, Reader reader)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    LOGGER.debug("public void setBlob(int parameterIndex, InputStream inputStream)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    LOGGER.debug("public void setNClob(int parameterIndex, Reader reader)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    throw new DatabricksSQLException("Method not supported in PreparedStatement");
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    throw new DatabricksSQLException("Method not supported in PreparedStatement");
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    throw new DatabricksSQLException("Method not supported in PreparedStatement");
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    throw new DatabricksSQLException("Method not supported in PreparedStatement");
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw new DatabricksSQLException("Method not supported in PreparedStatement");
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    throw new DatabricksSQLException("Method not supported in PreparedStatement");
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    throw new DatabricksSQLException("Method not supported in PreparedStatement");
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    throw new DatabricksSQLException("Method not supported in PreparedStatement");
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    throw new DatabricksSQLException("Method not supported in PreparedStatement");
  }
}
