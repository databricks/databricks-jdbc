package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.common.util.DatabricksTypeUtil.getDatabricksTypeFromSQLType;
import static com.databricks.jdbc.common.util.DatabricksTypeUtil.inferDatabricksType;
import static com.databricks.jdbc.common.util.SQLInterpolator.interpolateSQL;

import com.databricks.jdbc.common.AllPurposeCluster;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.util.DatabricksTypeUtil;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksSQLFeatureNotImplementedException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.Calendar;

public class DatabricksPreparedStatement extends DatabricksStatement implements PreparedStatement {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksPreparedStatement.class);
  private final String sql;
  private DatabricksParameterMetaData databricksParameterMetaData;
  private List<DatabricksParameterMetaData> databricksBatchParameterMetaData;
  private final boolean interpolateParameters;
  private final int CHUNK_SIZE = 8192;

  public DatabricksPreparedStatement(DatabricksConnection connection, String sql) {
    super(connection);
    this.sql = sql;
    this.interpolateParameters =
        connection.getSession().getConnectionContext().supportManyParameters()
            || connection.getSession().getConnectionContext().getComputeResource()
                instanceof AllPurposeCluster;
    this.databricksParameterMetaData = new DatabricksParameterMetaData();
    this.databricksBatchParameterMetaData = new ArrayList<>();
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    LOGGER.debug("public ResultSet executeQuery()");
    checkIfBatchOperation();
    return interpolateIfRequiredAndExecute(StatementType.QUERY);
  }

  @Override
  public int executeUpdate() throws SQLException {
    LOGGER.debug("public int executeUpdate()");
    checkIfBatchOperation();
    interpolateIfRequiredAndExecute(StatementType.UPDATE);
    return (int) resultSet.getUpdateCount();
  }

  @Override
  public int[] executeBatch() {
    LOGGER.debug("public int executeBatch()");
    int[] updateCount = new int[databricksBatchParameterMetaData.size()];

    for (int i = 0; i < databricksBatchParameterMetaData.size(); i++) {
      DatabricksParameterMetaData databricksParameterMetaData =
          databricksBatchParameterMetaData.get(i);
      try {
        executeInternal(
            sql, databricksParameterMetaData.getParameterBindings(), StatementType.UPDATE, false);
        updateCount[i] = (int) resultSet.getUpdateCount();
      } catch (SQLException e) {
        LOGGER.error(e.getMessage());
        updateCount[i] = -1;
      }
    }
    return updateCount;
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    LOGGER.debug("public void setNull(int parameterIndex, int sqlType)");
    setObject(parameterIndex, null, sqlType);
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    LOGGER.debug("public void setBoolean(int parameterIndex, boolean x)");
    checkIfClosed();
    setObject(parameterIndex, x, DatabricksTypeUtil.BOOLEAN);
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    LOGGER.debug("public void setByte(int parameterIndex, byte x)");
    checkIfClosed();
    setObject(parameterIndex, x, DatabricksTypeUtil.TINYINT);
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    LOGGER.debug("public void setShort(int parameterIndex, short x)");
    checkIfClosed();
    setObject(parameterIndex, x, DatabricksTypeUtil.SMALLINT);
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    LOGGER.debug("public void setInt(int parameterIndex, int x)");
    checkIfClosed();
    setObject(parameterIndex, x, DatabricksTypeUtil.INT);
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    LOGGER.debug("public void setLong(int parameterIndex, long x)");
    checkIfClosed();
    setObject(parameterIndex, x, DatabricksTypeUtil.BIGINT);
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    LOGGER.debug("public void setFloat(int parameterIndex, float x)");
    checkIfClosed();
    setObject(parameterIndex, x, DatabricksTypeUtil.FLOAT);
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    LOGGER.debug("public void setDouble(int parameterIndex, double x)");
    checkIfClosed();
    setObject(parameterIndex, x, DatabricksTypeUtil.DOUBLE);
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    LOGGER.debug("public void setBigDecimal(int parameterIndex, BigDecimal x)");
    checkIfClosed();
    setObject(parameterIndex, x, DatabricksTypeUtil.DECIMAL);
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    LOGGER.debug("public void setString(int parameterIndex, String x)");
    checkIfClosed();
    setObject(parameterIndex, x, DatabricksTypeUtil.STRING);
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    LOGGER.debug("public void setBytes(int parameterIndex, byte[] x)");
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksPreparedStatement - setBytes(int parameterIndex, byte[] x)");
  }

  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException {
    LOGGER.debug("public void setDate(int parameterIndex, Date x)");
    checkIfClosed();
    setObject(parameterIndex, x, DatabricksTypeUtil.DATE);
  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException {
    LOGGER.debug("public void setTime(int parameterIndex, Time x)");
    checkIfClosed();
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksPreparedStatement - setTime(int parameterIndex, Time x)");
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    LOGGER.debug("public void setTimestamp(int parameterIndex, Timestamp x)");
    checkIfClosed();
    setObject(parameterIndex, x, DatabricksTypeUtil.TIMESTAMP);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    LOGGER.debug("public void setAsciiStream(int parameterIndex, InputStream x, int length)");
    checkIfClosed();
    byte[] bytes = readBytesFromInputStream(x, length);
    String asciiString = new String(bytes, StandardCharsets.US_ASCII);
    setObject(parameterIndex, asciiString, DatabricksTypeUtil.STRING);
  }

  @Override
  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    LOGGER.debug("public void setUnicodeStream(int parameterIndex, InputStream x, int length)");
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksPreparedStatement - setUnicodeStream(int parameterIndex, InputStream x, int length)");
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    LOGGER.debug("public void setBinaryStream(int parameterIndex, InputStream x, int length)");
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksPreparedStatement - setBinaryStream(int parameterIndex, InputStream x, int length)");
  }

  @Override
  public void clearParameters() throws SQLException {
    LOGGER.debug("public void clearParameters()");
    checkIfClosed();
    this.databricksParameterMetaData.getParameterBindings().clear();
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
    throw new DatabricksSQLFeatureNotImplementedException(
        "Not implemented in DatabricksPreparedStatement - setObject(int parameterIndex, Object x, int targetSqlType)");
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
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksPreparedStatement - setObject(int parameterIndex, Object x)");
  }

  @Override
  public boolean execute() throws SQLException {
    LOGGER.debug("public boolean execute()");
    checkIfClosed();
    checkIfBatchOperation();
    interpolateIfRequiredAndExecute(StatementType.SQL);
    return shouldReturnResultSet(sql);
  }

  @Override
  public void addBatch() {
    LOGGER.debug("public void addBatch()");
    this.databricksBatchParameterMetaData.add(databricksParameterMetaData);
    this.databricksParameterMetaData = new DatabricksParameterMetaData();
  }

  @Override
  public void clearBatch() throws DatabricksSQLException {
    LOGGER.debug("public void clearBatch()");
    checkIfClosed();
    this.databricksParameterMetaData = new DatabricksParameterMetaData();
    this.databricksBatchParameterMetaData = new ArrayList<>();
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length)
      throws SQLException {
    LOGGER.debug("public void setCharacterStream(int parameterIndex, Reader reader, int length)");
    checkIfClosed();
    try {
      char[] buffer = new char[length];
      int charsRead = reader.read(buffer);
      checkLength(charsRead, length);
      String str = new String(buffer);
      setObject(parameterIndex, str, DatabricksTypeUtil.STRING);
    } catch (IOException e) {
      String errorMessage = "Error reading from the Reader";
      LOGGER.error(errorMessage);
      throw new DatabricksSQLException(errorMessage, e);
    }
  }

  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException {
    LOGGER.debug("public void setRef(int parameterIndex, Ref x)");
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksPreparedStatement - setRef(int parameterIndex, Ref x)");
  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    LOGGER.debug("public void setBlob(int parameterIndex, Blob x)");
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksPreparedStatement - setBlob(int parameterIndex, Blob x)");
  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException {
    LOGGER.debug("public void setClob(int parameterIndex, Clob x)");
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksPreparedStatement - setClob(int parameterIndex, Clob x)");
  }

  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException {
    LOGGER.debug("public void setArray(int parameterIndex, Array x)");
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksPreparedStatement - setArray(int parameterIndex, Array x)");
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    LOGGER.debug("public ResultSetMetaData getMetaData()");
    checkIfClosed();
    if (resultSet == null) {
      return null;
    }
    return resultSet.getMetaData();
  }

  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    LOGGER.debug("public void setDate(int parameterIndex, Date x, Calendar cal)");
    // TODO (PECO-1702): Use the calendar object
    checkIfClosed();
    setObject(parameterIndex, x, DatabricksTypeUtil.DATE);
  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    LOGGER.debug("public void setTime(int parameterIndex, Time x, Calendar cal)");
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksPreparedStatement - setTime(int parameterIndex, Time x, Calendar cal)");
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    LOGGER.debug("public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)");
    checkIfClosed();
    if (cal != null) {
      TimeZone originalTimeZone = TimeZone.getDefault();
      TimeZone.setDefault(cal.getTimeZone());
      x = new Timestamp(x.getTime());
      TimeZone.setDefault(originalTimeZone);
      setObject(parameterIndex, x, DatabricksTypeUtil.TIMESTAMP);
    } else {
      setTimestamp(parameterIndex, x);
    }
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    LOGGER.debug("public void setNull(int parameterIndex, int sqlType, String typeName)");
    setObject(parameterIndex, null, sqlType);
  }

  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException {
    LOGGER.debug("public void setURL(int parameterIndex, URL x)");
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksPreparedStatement - setURL(int parameterIndex, URL x)");
  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    LOGGER.debug("public ParameterMetaData getParameterMetaData()");
    return this.databricksParameterMetaData;
  }

  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    LOGGER.debug("public void setRowId(int parameterIndex, RowId x)");
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksPreparedStatement - setRowId(int parameterIndex, RowId x)");
  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException {
    LOGGER.debug("public void setNString(int parameterIndex, String value)");
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksPreparedStatement - setNString(int parameterIndex, String value)");
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length)
      throws SQLException {
    LOGGER.debug("public void setNCharacterStream(int parameterIndex, Reader value, long length)");
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksPreparedStatement - setNCharacterStream(int parameterIndex, Reader value, long length)");
  }

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    LOGGER.debug("public void setNClob(int parameterIndex, NClob value)");
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksPreparedStatement - setNClob(int parameterIndex, NClob value)");
  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    LOGGER.debug("public void setClob(int parameterIndex, Reader reader, long length)");
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksPreparedStatement - setClob(int parameterIndex, Reader reader, long length)");
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length)
      throws SQLException {
    LOGGER.debug("public void setBlob(int parameterIndex, InputStream inputStream, long length)");
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksPreparedStatement - setBlob(int parameterIndex, InputStream inputStream, long length)");
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    LOGGER.debug("public void setNClob(int parameterIndex, Reader reader, long length)");
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksPreparedStatement - setNClob(int parameterIndex, Reader reader, long length)");
  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    LOGGER.debug("public void setSQLXML(int parameterIndex, SQLXML xmlObject)");
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksPreparedStatement - setSQLXML(int parameterIndex, SQLXML xmlObject)");
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
      throws SQLException {
    LOGGER.debug(
        "public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)");
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksPreparedStatement - setCharacterStream(int parameterIndex, Reader reader)");
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    LOGGER.debug("public void setAsciiStream(int parameterIndex, InputStream x, long length)");
    checkIfClosed();
    setObject(
        parameterIndex,
        readStringFromInputStream(x, length, StandardCharsets.US_ASCII),
        DatabricksTypeUtil.STRING);
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    LOGGER.debug("public void setBinaryStream(int parameterIndex, InputStream x, long length)");
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksPreparedStatement - setBinaryStream(int parameterIndex, InputStream x, long length)");
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length)
      throws SQLException {
    LOGGER.debug("public void setCharacterStream(int parameterIndex, Reader reader, long length)");
    checkIfClosed();
    setObject(parameterIndex, readStringFromReader(reader, length), DatabricksTypeUtil.STRING);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    LOGGER.debug("public void setAsciiStream(int parameterIndex, InputStream x)");
    checkIfClosed();
    setObject(
        parameterIndex,
        readStringFromInputStream(x, -1, StandardCharsets.US_ASCII),
        DatabricksTypeUtil.STRING);
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    LOGGER.debug("public void setBinaryStream(int parameterIndex, InputStream x)");
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksPreparedStatement - setBinaryStream(int parameterIndex, InputStream x)");
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    LOGGER.debug("public void setCharacterStream(int parameterIndex, Reader reader)");
    checkIfClosed();
    setObject(parameterIndex, readStringFromReader(reader, -1), DatabricksTypeUtil.STRING);
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    LOGGER.debug("public void setNCharacterStream(int parameterIndex, Reader value)");
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksPreparedStatement - setNCharacterStream(int parameterIndex, Reader value)");
  }

  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    LOGGER.debug("public void setClob(int parameterIndex, Reader reader)");
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksPreparedStatement - setClob(int parameterIndex, Reader reader)");
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    LOGGER.debug("public void setBlob(int parameterIndex, InputStream inputStream)");
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksPreparedStatement - setBlob(int parameterIndex, InputStream inputStream)");
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    LOGGER.debug("public void setNClob(int parameterIndex, Reader reader)");
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksPreparedStatement - setNClob(int parameterIndex, Reader reader)");
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

  private void checkLength(long targetLength, long sourceLength) throws SQLException {
    if (targetLength != sourceLength) {
      String errorMessage =
          String.format(
              "Unexpected number of bytes read from the stream. Expected: %d, got: %d",
              targetLength, sourceLength);
      LOGGER.error(errorMessage);
      throw new DatabricksSQLException(errorMessage);
    }
  }

  private void checkIfBatchOperation() throws DatabricksSQLException {
    if (!this.databricksBatchParameterMetaData.isEmpty()) {
      String errorMessage =
          "Batch must either be executed with executeBatch() or cleared with clearBatch()";
      LOGGER.error(errorMessage);
      throw new DatabricksSQLException(errorMessage);
    }
  }

  private byte[] readBytesFromInputStream(InputStream x, int length) throws SQLException {
    if (x == null) {
      String errorMessage = "InputStream cannot be null";
      LOGGER.error(errorMessage);
      throw new DatabricksSQLException(errorMessage);
    }
    byte[] bytes = new byte[length];
    try {
      int bytesRead = x.read(bytes);
      checkLength(bytesRead, length);
    } catch (IOException e) {
      String errorMessage = "Error reading from the InputStream";
      LOGGER.error(errorMessage);
      throw new DatabricksSQLException(errorMessage, e);
    }
    return bytes;
  }

  /**
   * Reads bytes from the provided {@link InputStream} up to the specified length and returns them
   * as a {@link String} decoded using the specified {@link Charset}. If the specified length is -1,
   * reads until the end of the stream.
   *
   * @param inputStream the {@link InputStream} to read from; must not be null.
   * @param length the maximum number of bytes to read; if -1, reads until EOF.
   * @param charset the {@link Charset} to use for decoding the bytes into a string; must not be
   *     null.
   * @return a {@link String} containing the decoded bytes from the input stream.
   * @throws SQLException if the inputStream or charset is null, or if an I/O error occurs.
   */
  private String readStringFromInputStream(InputStream inputStream, long length, Charset charset)
      throws SQLException {
    if (inputStream == null) {
      String message = "InputStream cannot be null";
      LOGGER.error(message);
      throw new DatabricksSQLException(message);
    }
    try (ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
      byte[] chunk = new byte[CHUNK_SIZE];
      long bytesRead = 0;
      int nRead;
      while ((length != -1 && bytesRead < length) && (nRead = inputStream.read(chunk)) != -1) {
        buffer.write(chunk, 0, nRead);
        bytesRead += nRead;
      }
      if (length != -1) {
        checkLength(length, bytesRead);
      }
      return new String(buffer.toByteArray(), charset);
    } catch (IOException e) {
      String message = "Error reading from the InputStream";
      LOGGER.error(message);
      throw new DatabricksSQLException(message, e);
    }
  }

  /**
   * Reads characters from the provided {@link Reader} up to the specified length and returns them
   * as a {@link String}. If the specified length is -1, reads until the end of the stream.
   *
   * @param reader the {@link Reader} to read from; must not be null.
   * @param length the maximum number of characters to read; if -1, reads until EOF.
   * @return a {@link String} containing the characters read from the reader.
   * @throws SQLException if the reader is null or if an I/O error occurs.
   */
  private String readStringFromReader(Reader reader, long length) throws SQLException {
    if (reader == null) {
      String message = "Reader cannot be null";
      LOGGER.error(message);
      throw new DatabricksSQLException(message);
    }
    try {
      StringBuilder buffer = new StringBuilder();
      char[] chunk = new char[CHUNK_SIZE];
      long charsRead = 0;
      int nRead;
      while ((length != -1 && charsRead < length) && (nRead = reader.read(chunk)) != -1) {
        buffer.append(chunk, 0, nRead);
        charsRead += nRead;
      }
      if (length != -1) {
        checkLength(length, charsRead);
      }
      return buffer.toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void setObject(int parameterIndex, Object x, String databricksType) {
    this.databricksParameterMetaData.put(
        parameterIndex,
        ImmutableSqlParameter.builder()
            .type(DatabricksTypeUtil.getColumnInfoType(databricksType))
            .value(x)
            .cardinal(parameterIndex)
            .build());
  }

  private DatabricksResultSet interpolateIfRequiredAndExecute(StatementType statementType)
      throws SQLException {
    String interpolatedSql =
        this.interpolateParameters
            ? interpolateSQL(sql, this.databricksParameterMetaData.getParameterBindings())
            : sql;
    Map<Integer, ImmutableSqlParameter> paramMap =
        this.interpolateParameters
            ? new HashMap<Integer, ImmutableSqlParameter>()
            : this.databricksParameterMetaData.getParameterBindings();
    return executeInternal(interpolatedSql, paramMap, statementType);
  }
}
