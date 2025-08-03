package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.EMPTY_STRING;

import com.databricks.jdbc.common.util.DatabricksTypeUtil;
import com.databricks.jdbc.common.util.WrapperUtil;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

// TODO (PECO-1738): Implement ParameterMetaData
public class DatabricksParameterMetaData implements ParameterMetaData {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksParameterMetaData.class);
  private final Map<Integer, ImmutableSqlParameter> parameterBindings;
  private final int parameterCount;

  public DatabricksParameterMetaData() {
    this(null);
  }

  public DatabricksParameterMetaData(String sql) {
    this.parameterBindings = new HashMap<>();
    this.parameterCount = countParameters(sql);
  }

  public void put(int param, ImmutableSqlParameter value) {
    this.parameterBindings.put(param, value);
  }

  public Map<Integer, ImmutableSqlParameter> getParameterBindings() {
    return parameterBindings;
  }

  public void clear() {
    this.parameterBindings.clear();
  }

  @Override
  public int getParameterCount() throws SQLException {
    validateParameterBindings();
    return parameterCount;
  }

  /**
   * Validates that parameter bindings do not exceed the parameter count.
   *
   * @throws SQLException if there are more parameter bindings than expected parameters
   */
  private void validateParameterBindings() throws SQLException {
    if (parameterBindings.size() > parameterCount) {
      throw new SQLException(
          "Number of parameter bindings ("
              + parameterBindings.size()
              + ") exceeds parameter count ("
              + parameterCount
              + ")");
    }
  }

  @Override
  public int isNullable(int param) throws SQLException {
    // TODO (PECO-1711): Implement isNullable
    return ParameterMetaData.parameterNullableUnknown;
  }

  @Override
  public boolean isSigned(int param) throws SQLException {
    LOGGER.warn("This feature is not fully implemented in the driver yet.");
    return DatabricksTypeUtil.isSigned(getObject(param).type());
  }

  @Override
  public int getPrecision(int param) throws SQLException {
    LOGGER.warn("This feature is not fully implemented in the driver yet.");
    return DatabricksTypeUtil.getPrecision(
        DatabricksTypeUtil.getColumnType(getObject(param).type()));
  }

  @Override
  public int getScale(int param) throws SQLException {
    LOGGER.warn("This feature is not fully implemented in the driver yet.");
    return DatabricksTypeUtil.getScale(DatabricksTypeUtil.getColumnType(getObject(param).type()));
  }

  @Override
  public int getParameterType(int param) throws SQLException {
    LOGGER.warn("This feature is not fully implemented in the driver yet.");
    return DatabricksTypeUtil.getColumnType(getObject(param).type());
  }

  @Override
  public String getParameterTypeName(int param) throws SQLException {
    LOGGER.warn("This feature is not fully implemented in the driver yet.");
    return getObject(param).type().name();
  }

  @Override
  public String getParameterClassName(int param) throws SQLException {
    LOGGER.warn("This feature is not fully implemented in the driver yet.");
    return DatabricksTypeUtil.getColumnTypeClassName(getObject(param).type());
  }

  @Override
  public int getParameterMode(int param) throws SQLException {
    LOGGER.warn("This feature is not fully implemented in the driver yet.");
    return ParameterMetaData
        .parameterModeIn; // In context of prepared statement, only IN parameters are provided.
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return WrapperUtil.unwrap(iface, this);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return WrapperUtil.isWrapperFor(iface, this);
  }

  private ImmutableSqlParameter getObject(int param) {
    if (!parameterBindings.containsKey(param)) {
      LOGGER.info("Parameter not added in the prepared statement yet. Sending default value");
      return ImmutableSqlParameter.builder()
          .type(ColumnInfoTypeName.STRING)
          .cardinal(1)
          .value(EMPTY_STRING)
          .build();
    }
    return parameterBindings.get(param);
  }

  /**
   * Counts the number of parameter markers (?) in the SQL statement. This is currently a hacky
   * implementation that may need improvement for complex SQL.
   *
   * @param sql The SQL statement to analyze
   * @return The number of parameter markers in the SQL statement
   */
  private int countParameters(String sql) {
    if (sql == null || sql.isEmpty()) {
      return 0;
    }

    int count = 0;
    boolean inSingleQuote = false;
    boolean inDoubleQuote = false;
    boolean inLineComment = false;
    boolean inBlockComment = false;

    for (int i = 0; i < sql.length(); i++) {
      char c = sql.charAt(i);
      char next = (i < sql.length() - 1) ? sql.charAt(i + 1) : '\0';

      // Handle comments
      if (!inSingleQuote && !inDoubleQuote) {
        if (!inBlockComment && !inLineComment && c == '-' && next == '-') {
          inLineComment = true;
          i++; // Skip next dash
          continue;
        } else if (!inBlockComment && !inLineComment && c == '/' && next == '*') {
          inBlockComment = true;
          i++; // Skip next asterisk
          continue;
        } else if (inLineComment && (c == '\n' || c == '\r')) {
          inLineComment = false;
        } else if (inBlockComment && c == '*' && next == '/') {
          inBlockComment = false;
          i++; // Skip next slash
          continue;
        }
      }

      // Skip if in comment
      if (inLineComment || inBlockComment) {
        continue;
      }

      // Handle quotes with escaped quotes
      if (c == '\'') {
        if (!inDoubleQuote) {
          // Check for escaped single quote
          if (inSingleQuote && i < sql.length() - 1 && sql.charAt(i + 1) == '\'') {
            i++; // Skip the escaped quote
          } else {
            inSingleQuote = !inSingleQuote;
          }
        }
      } else if (c == '"') {
        if (!inSingleQuote) {
          // Check for escaped double quote
          if (inDoubleQuote && i < sql.length() - 1 && sql.charAt(i + 1) == '"') {
            i++; // Skip the escaped quote
          } else {
            inDoubleQuote = !inDoubleQuote;
          }
        }
      }

      // Count parameter markers only when not inside quotes or comments
      if (c == '?' && !inSingleQuote && !inDoubleQuote) {
        count++;
      }
    }

    return count;
  }
}
