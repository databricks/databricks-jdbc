package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.exception.DatabricksSQLFeatureNotSupportedException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Databricks implementation of {@link CallableStatement} with IN-parameter-only support.
 *
 * <p>This class extends {@link DatabricksPreparedStatement} to reuse all IN parameter binding
 * ({@code setXXX(int, value)}), execution, and batch functionality. The JDBC escape syntax {@code
 * {call proc(?, ?)}} is converted to {@code CALL proc(?, ?)} by the existing escape processing in
 * {@link com.databricks.jdbc.common.util.StringUtil#convertJdbcEscapeSequences}.
 *
 * <p>OUT and INOUT parameters are not supported. All {@code registerOutParameter()}, output
 * retrieval ({@code getXXX(int/String)}), and named parameter ({@code setXXX(String, value)})
 * methods throw {@link SQLFeatureNotSupportedException}.
 *
 * <p>Since stored procedures may return result sets, {@code shouldReturnResultSet} is always {@code
 * true}. Use {@code execute()} or {@code executeQuery()} to invoke procedures. {@code
 * executeUpdate()} will throw because it conflicts with result-set-returning semantics — use {@code
 * execute()} for DML procedures and check {@code getUpdateCount()} afterward.
 */
public class DatabricksCallableStatement extends DatabricksPreparedStatement
    implements CallableStatement {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksCallableStatement.class);

  /**
   * Pattern to detect the JDBC return-value escape syntax: {@code {? = call ...}}. This syntax
   * requires server-side support for typed return values which is not available.
   */
  private static final Pattern RETURN_VALUE_SYNTAX =
      Pattern.compile("\\{\\s*\\?\\s*=\\s*call\\b", Pattern.CASE_INSENSITIVE);

  /**
   * Matches JDBC callable escape syntax: {@code {call proc(...)}}. Uses {@code [^}]*} to match
   * arguments, which does not handle nested JDBC escape sequences like {@code {call proc({fn
   * CONCAT('a','b')})}}. This is consistent with {@link
   * com.databricks.jdbc.common.util.StringUtil#convertJdbcEscapeSequences} which has the same
   * limitation. Nested escapes should be pre-processed or avoided in favor of native SQL syntax.
   */
  private static final Pattern CALL_ESCAPE_SYNTAX =
      Pattern.compile("\\{\\s*call\\s+([^}]*)\\}", Pattern.CASE_INSENSITIVE);

  private static final String OUT_PARAM_NOT_SUPPORTED =
      "OUT and INOUT parameters are not supported. "
          + "Only IN parameters are supported for callable statements. "
          + "Use SQL scripting with DECLARE and SELECT as a workaround for output values.";

  private static final String NAMED_PARAM_NOT_SUPPORTED =
      "Named parameters are not supported. Use positional parameters (setXXX(int, value)) instead.";

  public DatabricksCallableStatement(DatabricksConnection connection, String sql)
      throws SQLException {
    // Validate and convert before super() to avoid partially initializing the parent
    // when the SQL is invalid (e.g., {? = call ...} return-value syntax).
    super(connection, validateAndConvert(sql));
    LOGGER.debug("Created DatabricksCallableStatement for SQL: {}", sql);
  }

  /**
   * Validates the SQL and converts JDBC callable escape syntax before passing to the parent
   * constructor. Validation runs first so that invalid SQL (e.g., {@code {? = call ...}}) is
   * rejected before any parent initialization occurs.
   *
   * <p>The {@code {call proc(?)}} → {@code CALL proc(?)} conversion is done unconditionally (not
   * gated by escapeProcessing) because the {@code {call ...}} form is specific to callable
   * statements and must always be resolved.
   */
  private static String validateAndConvert(String sql) throws SQLException {
    if (sql != null && RETURN_VALUE_SYNTAX.matcher(sql).find()) {
      throw new DatabricksSQLFeatureNotSupportedException(
          "Return value syntax {? = call ...} is not supported. "
              + "Use {call proc(...)} and retrieve results via the ResultSet.");
    }
    if (sql == null) {
      return null;
    }
    return CALL_ESCAPE_SYNTAX.matcher(sql).replaceAll("CALL $1");
  }

  /**
   * {@inheritDoc}
   *
   * <p>Callable statements always set {@code shouldReturnResultSet = true} because stored
   * procedures may return result sets. Use {@link #execute()} for DML procedures and check {@link
   * #getUpdateCount()} afterward.
   */
  @Override
  public int executeUpdate() throws SQLException {
    LOGGER.debug("public int executeUpdate()");
    throw new DatabricksSQLFeatureNotSupportedException(
        "executeUpdate() is not supported for callable statements because stored procedures "
            + "may return result sets. Use execute() instead, then call getUpdateCount() to "
            + "retrieve the update count for DML procedures.");
  }

  /**
   * Throws for methods with non-void return types. Always throws, never returns. The generic return
   * type avoids separate helpers for each return type.
   */
  private <T> T throwOutParamNotSupported() throws SQLException {
    throw new DatabricksSQLFeatureNotSupportedException(OUT_PARAM_NOT_SUPPORTED);
  }

  /** Throws for void methods (registerOutParameter, named setXXX). */
  private void throwOutParamNotSupportedVoid() throws SQLException {
    throw new DatabricksSQLFeatureNotSupportedException(OUT_PARAM_NOT_SUPPORTED);
  }

  private void throwNamedParamNotSupportedVoid() throws SQLException {
    throw new DatabricksSQLFeatureNotSupportedException(NAMED_PARAM_NOT_SUPPORTED);
  }

  // ---------------------------------------------------------------------------
  // registerOutParameter — all overloads
  // ---------------------------------------------------------------------------

  @Override
  public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
    LOGGER.debug("public void registerOutParameter(int parameterIndex, int sqlType)");
    throwOutParamNotSupportedVoid();
  }

  @Override
  public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
    LOGGER.debug("public void registerOutParameter(int parameterIndex, int sqlType, int scale)");
    throwOutParamNotSupportedVoid();
  }

  @Override
  public void registerOutParameter(int parameterIndex, int sqlType, String typeName)
      throws SQLException {
    LOGGER.debug(
        "public void registerOutParameter(int parameterIndex, int sqlType, String typeName)");
    throwOutParamNotSupportedVoid();
  }

  @Override
  public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
    LOGGER.debug("public void registerOutParameter(String parameterName, int sqlType)");
    throwOutParamNotSupportedVoid();
  }

  @Override
  public void registerOutParameter(String parameterName, int sqlType, int scale)
      throws SQLException {
    LOGGER.debug("public void registerOutParameter(String parameterName, int sqlType, int scale)");
    throwOutParamNotSupportedVoid();
  }

  @Override
  public void registerOutParameter(String parameterName, int sqlType, String typeName)
      throws SQLException {
    LOGGER.debug(
        "public void registerOutParameter(String parameterName, int sqlType, String typeName)");
    throwOutParamNotSupportedVoid();
  }

  // Java 8+ SQLType-based overrides — explicit overrides to ensure consistent
  // Databricks-specific error messages rather than relying on default method delegation.

  @Override
  public void registerOutParameter(int parameterIndex, SQLType sqlType) throws SQLException {
    LOGGER.debug("public void registerOutParameter(int parameterIndex, SQLType sqlType)");
    throwOutParamNotSupportedVoid();
  }

  @Override
  public void registerOutParameter(int parameterIndex, SQLType sqlType, int scale)
      throws SQLException {
    LOGGER.debug(
        "public void registerOutParameter(int parameterIndex, SQLType sqlType, int scale)");
    throwOutParamNotSupportedVoid();
  }

  @Override
  public void registerOutParameter(int parameterIndex, SQLType sqlType, String typeName)
      throws SQLException {
    LOGGER.debug(
        "public void registerOutParameter(int parameterIndex, SQLType sqlType, String typeName)");
    throwOutParamNotSupportedVoid();
  }

  @Override
  public void registerOutParameter(String parameterName, SQLType sqlType) throws SQLException {
    LOGGER.debug("public void registerOutParameter(String parameterName, SQLType sqlType)");
    throwOutParamNotSupportedVoid();
  }

  @Override
  public void registerOutParameter(String parameterName, SQLType sqlType, int scale)
      throws SQLException {
    LOGGER.debug(
        "public void registerOutParameter(String parameterName, SQLType sqlType, int scale)");
    throwOutParamNotSupportedVoid();
  }

  @Override
  public void registerOutParameter(String parameterName, SQLType sqlType, String typeName)
      throws SQLException {
    LOGGER.debug(
        "public void registerOutParameter(String parameterName, SQLType sqlType, String typeName)");
    throwOutParamNotSupportedVoid();
  }

  // ---------------------------------------------------------------------------
  // wasNull
  // ---------------------------------------------------------------------------

  @Override
  public boolean wasNull() throws SQLException {
    LOGGER.debug("public boolean wasNull()");
    return throwOutParamNotSupported();
  }

  // ---------------------------------------------------------------------------
  // Output parameter retrieval by index — getXXX(int)
  // ---------------------------------------------------------------------------

  @Override
  public String getString(int parameterIndex) throws SQLException {
    LOGGER.debug("public String getString(int parameterIndex)");
    return throwOutParamNotSupported();
  }

  @Override
  public boolean getBoolean(int parameterIndex) throws SQLException {
    LOGGER.debug("public boolean getBoolean(int parameterIndex)");
    return throwOutParamNotSupported();
  }

  @Override
  public byte getByte(int parameterIndex) throws SQLException {
    LOGGER.debug("public byte getByte(int parameterIndex)");
    return throwOutParamNotSupported();
  }

  @Override
  public short getShort(int parameterIndex) throws SQLException {
    LOGGER.debug("public short getShort(int parameterIndex)");
    return throwOutParamNotSupported();
  }

  @Override
  public int getInt(int parameterIndex) throws SQLException {
    LOGGER.debug("public int getInt(int parameterIndex)");
    return throwOutParamNotSupported();
  }

  @Override
  public long getLong(int parameterIndex) throws SQLException {
    LOGGER.debug("public long getLong(int parameterIndex)");
    return throwOutParamNotSupported();
  }

  @Override
  public float getFloat(int parameterIndex) throws SQLException {
    LOGGER.debug("public float getFloat(int parameterIndex)");
    return throwOutParamNotSupported();
  }

  @Override
  public double getDouble(int parameterIndex) throws SQLException {
    LOGGER.debug("public double getDouble(int parameterIndex)");
    return throwOutParamNotSupported();
  }

  @Override
  @SuppressWarnings("deprecation")
  public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
    LOGGER.debug("public BigDecimal getBigDecimal(int parameterIndex, int scale)");
    return throwOutParamNotSupported();
  }

  @Override
  public byte[] getBytes(int parameterIndex) throws SQLException {
    LOGGER.debug("public byte[] getBytes(int parameterIndex)");
    return throwOutParamNotSupported();
  }

  @Override
  public Date getDate(int parameterIndex) throws SQLException {
    LOGGER.debug("public Date getDate(int parameterIndex)");
    return throwOutParamNotSupported();
  }

  @Override
  public Time getTime(int parameterIndex) throws SQLException {
    LOGGER.debug("public Time getTime(int parameterIndex)");
    return throwOutParamNotSupported();
  }

  @Override
  public Timestamp getTimestamp(int parameterIndex) throws SQLException {
    LOGGER.debug("public Timestamp getTimestamp(int parameterIndex)");
    return throwOutParamNotSupported();
  }

  @Override
  public Object getObject(int parameterIndex) throws SQLException {
    LOGGER.debug("public Object getObject(int parameterIndex)");
    return throwOutParamNotSupported();
  }

  @Override
  public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
    LOGGER.debug("public BigDecimal getBigDecimal(int parameterIndex)");
    return throwOutParamNotSupported();
  }

  @Override
  public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
    LOGGER.debug("public Object getObject(int parameterIndex, Map map)");
    return throwOutParamNotSupported();
  }

  @Override
  public Ref getRef(int parameterIndex) throws SQLException {
    LOGGER.debug("public Ref getRef(int parameterIndex)");
    return throwOutParamNotSupported();
  }

  @Override
  public Blob getBlob(int parameterIndex) throws SQLException {
    LOGGER.debug("public Blob getBlob(int parameterIndex)");
    return throwOutParamNotSupported();
  }

  @Override
  public Clob getClob(int parameterIndex) throws SQLException {
    LOGGER.debug("public Clob getClob(int parameterIndex)");
    return throwOutParamNotSupported();
  }

  @Override
  public Array getArray(int parameterIndex) throws SQLException {
    LOGGER.debug("public Array getArray(int parameterIndex)");
    return throwOutParamNotSupported();
  }

  @Override
  public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
    LOGGER.debug("public Date getDate(int parameterIndex, Calendar cal)");
    return throwOutParamNotSupported();
  }

  @Override
  public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
    LOGGER.debug("public Time getTime(int parameterIndex, Calendar cal)");
    return throwOutParamNotSupported();
  }

  @Override
  public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
    LOGGER.debug("public Timestamp getTimestamp(int parameterIndex, Calendar cal)");
    return throwOutParamNotSupported();
  }

  @Override
  public URL getURL(int parameterIndex) throws SQLException {
    LOGGER.debug("public URL getURL(int parameterIndex)");
    return throwOutParamNotSupported();
  }

  @Override
  public RowId getRowId(int parameterIndex) throws SQLException {
    LOGGER.debug("public RowId getRowId(int parameterIndex)");
    return throwOutParamNotSupported();
  }

  @Override
  public NClob getNClob(int parameterIndex) throws SQLException {
    LOGGER.debug("public NClob getNClob(int parameterIndex)");
    return throwOutParamNotSupported();
  }

  @Override
  public SQLXML getSQLXML(int parameterIndex) throws SQLException {
    LOGGER.debug("public SQLXML getSQLXML(int parameterIndex)");
    return throwOutParamNotSupported();
  }

  @Override
  public String getNString(int parameterIndex) throws SQLException {
    LOGGER.debug("public String getNString(int parameterIndex)");
    return throwOutParamNotSupported();
  }

  @Override
  public Reader getNCharacterStream(int parameterIndex) throws SQLException {
    LOGGER.debug("public Reader getNCharacterStream(int parameterIndex)");
    return throwOutParamNotSupported();
  }

  @Override
  public Reader getCharacterStream(int parameterIndex) throws SQLException {
    LOGGER.debug("public Reader getCharacterStream(int parameterIndex)");
    return throwOutParamNotSupported();
  }

  @Override
  public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
    LOGGER.debug("public <T> T getObject(int parameterIndex, Class<T> type)");
    return throwOutParamNotSupported();
  }

  // ---------------------------------------------------------------------------
  // Output parameter retrieval by name — getXXX(String)
  // ---------------------------------------------------------------------------

  @Override
  public String getString(String parameterName) throws SQLException {
    LOGGER.debug("public String getString(String parameterName)");
    return throwOutParamNotSupported();
  }

  @Override
  public boolean getBoolean(String parameterName) throws SQLException {
    LOGGER.debug("public boolean getBoolean(String parameterName)");
    return throwOutParamNotSupported();
  }

  @Override
  public byte getByte(String parameterName) throws SQLException {
    LOGGER.debug("public byte getByte(String parameterName)");
    return throwOutParamNotSupported();
  }

  @Override
  public short getShort(String parameterName) throws SQLException {
    LOGGER.debug("public short getShort(String parameterName)");
    return throwOutParamNotSupported();
  }

  @Override
  public int getInt(String parameterName) throws SQLException {
    LOGGER.debug("public int getInt(String parameterName)");
    return throwOutParamNotSupported();
  }

  @Override
  public long getLong(String parameterName) throws SQLException {
    LOGGER.debug("public long getLong(String parameterName)");
    return throwOutParamNotSupported();
  }

  @Override
  public float getFloat(String parameterName) throws SQLException {
    LOGGER.debug("public float getFloat(String parameterName)");
    return throwOutParamNotSupported();
  }

  @Override
  public double getDouble(String parameterName) throws SQLException {
    LOGGER.debug("public double getDouble(String parameterName)");
    return throwOutParamNotSupported();
  }

  @Override
  public byte[] getBytes(String parameterName) throws SQLException {
    LOGGER.debug("public byte[] getBytes(String parameterName)");
    return throwOutParamNotSupported();
  }

  @Override
  public Date getDate(String parameterName) throws SQLException {
    LOGGER.debug("public Date getDate(String parameterName)");
    return throwOutParamNotSupported();
  }

  @Override
  public Time getTime(String parameterName) throws SQLException {
    LOGGER.debug("public Time getTime(String parameterName)");
    return throwOutParamNotSupported();
  }

  @Override
  public Timestamp getTimestamp(String parameterName) throws SQLException {
    LOGGER.debug("public Timestamp getTimestamp(String parameterName)");
    return throwOutParamNotSupported();
  }

  @Override
  public Object getObject(String parameterName) throws SQLException {
    LOGGER.debug("public Object getObject(String parameterName)");
    return throwOutParamNotSupported();
  }

  @Override
  public BigDecimal getBigDecimal(String parameterName) throws SQLException {
    LOGGER.debug("public BigDecimal getBigDecimal(String parameterName)");
    return throwOutParamNotSupported();
  }

  @Override
  public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
    LOGGER.debug("public Object getObject(String parameterName, Map map)");
    return throwOutParamNotSupported();
  }

  @Override
  public Ref getRef(String parameterName) throws SQLException {
    LOGGER.debug("public Ref getRef(String parameterName)");
    return throwOutParamNotSupported();
  }

  @Override
  public Blob getBlob(String parameterName) throws SQLException {
    LOGGER.debug("public Blob getBlob(String parameterName)");
    return throwOutParamNotSupported();
  }

  @Override
  public Clob getClob(String parameterName) throws SQLException {
    LOGGER.debug("public Clob getClob(String parameterName)");
    return throwOutParamNotSupported();
  }

  @Override
  public Array getArray(String parameterName) throws SQLException {
    LOGGER.debug("public Array getArray(String parameterName)");
    return throwOutParamNotSupported();
  }

  @Override
  public Date getDate(String parameterName, Calendar cal) throws SQLException {
    LOGGER.debug("public Date getDate(String parameterName, Calendar cal)");
    return throwOutParamNotSupported();
  }

  @Override
  public Time getTime(String parameterName, Calendar cal) throws SQLException {
    LOGGER.debug("public Time getTime(String parameterName, Calendar cal)");
    return throwOutParamNotSupported();
  }

  @Override
  public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
    LOGGER.debug("public Timestamp getTimestamp(String parameterName, Calendar cal)");
    return throwOutParamNotSupported();
  }

  @Override
  public URL getURL(String parameterName) throws SQLException {
    LOGGER.debug("public URL getURL(String parameterName)");
    return throwOutParamNotSupported();
  }

  @Override
  public RowId getRowId(String parameterName) throws SQLException {
    LOGGER.debug("public RowId getRowId(String parameterName)");
    return throwOutParamNotSupported();
  }

  @Override
  public NClob getNClob(String parameterName) throws SQLException {
    LOGGER.debug("public NClob getNClob(String parameterName)");
    return throwOutParamNotSupported();
  }

  @Override
  public SQLXML getSQLXML(String parameterName) throws SQLException {
    LOGGER.debug("public SQLXML getSQLXML(String parameterName)");
    return throwOutParamNotSupported();
  }

  @Override
  public String getNString(String parameterName) throws SQLException {
    LOGGER.debug("public String getNString(String parameterName)");
    return throwOutParamNotSupported();
  }

  @Override
  public Reader getNCharacterStream(String parameterName) throws SQLException {
    LOGGER.debug("public Reader getNCharacterStream(String parameterName)");
    return throwOutParamNotSupported();
  }

  @Override
  public Reader getCharacterStream(String parameterName) throws SQLException {
    LOGGER.debug("public Reader getCharacterStream(String parameterName)");
    return throwOutParamNotSupported();
  }

  @Override
  public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
    LOGGER.debug("public <T> T getObject(String parameterName, Class<T> type)");
    return throwOutParamNotSupported();
  }

  // ---------------------------------------------------------------------------
  // Named parameter setXXX(String, ...) methods
  // ---------------------------------------------------------------------------

  @Override
  public void setURL(String parameterName, URL val) throws SQLException {
    LOGGER.debug("public void setURL(String parameterName, URL val)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setNull(String parameterName, int sqlType) throws SQLException {
    LOGGER.debug("public void setNull(String parameterName, int sqlType)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setBoolean(String parameterName, boolean x) throws SQLException {
    LOGGER.debug("public void setBoolean(String parameterName, boolean x)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setByte(String parameterName, byte x) throws SQLException {
    LOGGER.debug("public void setByte(String parameterName, byte x)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setShort(String parameterName, short x) throws SQLException {
    LOGGER.debug("public void setShort(String parameterName, short x)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setInt(String parameterName, int x) throws SQLException {
    LOGGER.debug("public void setInt(String parameterName, int x)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setLong(String parameterName, long x) throws SQLException {
    LOGGER.debug("public void setLong(String parameterName, long x)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setFloat(String parameterName, float x) throws SQLException {
    LOGGER.debug("public void setFloat(String parameterName, float x)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setDouble(String parameterName, double x) throws SQLException {
    LOGGER.debug("public void setDouble(String parameterName, double x)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
    LOGGER.debug("public void setBigDecimal(String parameterName, BigDecimal x)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setString(String parameterName, String x) throws SQLException {
    LOGGER.debug("public void setString(String parameterName, String x)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setBytes(String parameterName, byte[] x) throws SQLException {
    LOGGER.debug("public void setBytes(String parameterName, byte[] x)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setDate(String parameterName, Date x) throws SQLException {
    LOGGER.debug("public void setDate(String parameterName, Date x)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setTime(String parameterName, Time x) throws SQLException {
    LOGGER.debug("public void setTime(String parameterName, Time x)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
    LOGGER.debug("public void setTimestamp(String parameterName, Timestamp x)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
    LOGGER.debug("public void setAsciiStream(String parameterName, InputStream x, int length)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
    LOGGER.debug("public void setBinaryStream(String parameterName, InputStream x, int length)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setObject(String parameterName, Object x, int targetSqlType, int scale)
      throws SQLException {
    LOGGER.debug(
        "public void setObject(String parameterName, Object x, int targetSqlType, int scale)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
    LOGGER.debug("public void setObject(String parameterName, Object x, int targetSqlType)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setObject(String parameterName, Object x) throws SQLException {
    LOGGER.debug("public void setObject(String parameterName, Object x)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setCharacterStream(String parameterName, Reader reader, int length)
      throws SQLException {
    LOGGER.debug("public void setCharacterStream(String parameterName, Reader reader, int length)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
    LOGGER.debug("public void setDate(String parameterName, Date x, Calendar cal)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
    LOGGER.debug("public void setTime(String parameterName, Time x, Calendar cal)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
    LOGGER.debug("public void setTimestamp(String parameterName, Timestamp x, Calendar cal)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
    LOGGER.debug("public void setNull(String parameterName, int sqlType, String typeName)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setRowId(String parameterName, RowId x) throws SQLException {
    LOGGER.debug("public void setRowId(String parameterName, RowId x)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setNString(String parameterName, String value) throws SQLException {
    LOGGER.debug("public void setNString(String parameterName, String value)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setNCharacterStream(String parameterName, Reader value, long length)
      throws SQLException {
    LOGGER.debug(
        "public void setNCharacterStream(String parameterName, Reader value, long length)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setNClob(String parameterName, NClob value) throws SQLException {
    LOGGER.debug("public void setNClob(String parameterName, NClob value)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setClob(String parameterName, Reader reader, long length) throws SQLException {
    LOGGER.debug("public void setClob(String parameterName, Reader reader, long length)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setBlob(String parameterName, InputStream inputStream, long length)
      throws SQLException {
    LOGGER.debug("public void setBlob(String parameterName, InputStream inputStream, long length)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
    LOGGER.debug("public void setNClob(String parameterName, Reader reader, long length)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
    LOGGER.debug("public void setSQLXML(String parameterName, SQLXML xmlObject)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setBlob(String parameterName, Blob x) throws SQLException {
    LOGGER.debug("public void setBlob(String parameterName, Blob x)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setClob(String parameterName, Clob x) throws SQLException {
    LOGGER.debug("public void setClob(String parameterName, Clob x)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
    LOGGER.debug("public void setAsciiStream(String parameterName, InputStream x, long length)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setBinaryStream(String parameterName, InputStream x, long length)
      throws SQLException {
    LOGGER.debug("public void setBinaryStream(String parameterName, InputStream x, long length)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setCharacterStream(String parameterName, Reader reader, long length)
      throws SQLException {
    LOGGER.debug(
        "public void setCharacterStream(String parameterName, Reader reader, long length)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
    LOGGER.debug("public void setAsciiStream(String parameterName, InputStream x)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
    LOGGER.debug("public void setBinaryStream(String parameterName, InputStream x)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
    LOGGER.debug("public void setCharacterStream(String parameterName, Reader reader)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
    LOGGER.debug("public void setNCharacterStream(String parameterName, Reader value)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setClob(String parameterName, Reader reader) throws SQLException {
    LOGGER.debug("public void setClob(String parameterName, Reader reader)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
    LOGGER.debug("public void setBlob(String parameterName, InputStream inputStream)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setNClob(String parameterName, Reader reader) throws SQLException {
    LOGGER.debug("public void setNClob(String parameterName, Reader reader)");
    throwNamedParamNotSupportedVoid();
  }

  // Java 8+ SQLType-based named parameter overrides

  @Override
  public void setObject(String parameterName, Object x, SQLType targetSqlType, int scaleOrLength)
      throws SQLException {
    LOGGER.debug(
        "public void setObject(String parameterName, Object x, SQLType targetSqlType, int scaleOrLength)");
    throwNamedParamNotSupportedVoid();
  }

  @Override
  public void setObject(String parameterName, Object x, SQLType targetSqlType) throws SQLException {
    LOGGER.debug("public void setObject(String parameterName, Object x, SQLType targetSqlType)");
    throwNamedParamNotSupportedVoid();
  }
}
