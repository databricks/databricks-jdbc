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

public class DatabricksParameterMetaData implements ParameterMetaData {
  // TODO : this class is not fully implemented. Tracked in PECO-1738
  public static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksParameterMetaData.class);
  private final Map<Integer, ImmutableSqlParameter> parameterBindings;

  public DatabricksParameterMetaData() {
    this.parameterBindings = new HashMap<>();
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
    return parameterBindings.size();
  }

  @Override
  public int isNullable(int param) throws SQLException {
    return (DatabricksTypeUtil.isNullable(getObject(param).type()));
  }

  @Override
  public boolean isSigned(int param) throws SQLException {
    LOGGER.warn("This feature is not fully implemented in the driver yet.");
    return DatabricksTypeUtil.isSigned(getObject(param).type());
  }

  @Override
  public int getPrecision(int param) throws SQLException {
    LOGGER.warn("This feature is not fully implemented in the driver yet.");
    return DatabricksTypeUtil.getPrecision(getObject(param).type());
  }

  @Override
  public int getScale(int param) throws SQLException {
    LOGGER.warn("This feature is not fully implemented in the driver yet.");
    return DatabricksTypeUtil.getScale(getObject(param).type());
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
}
