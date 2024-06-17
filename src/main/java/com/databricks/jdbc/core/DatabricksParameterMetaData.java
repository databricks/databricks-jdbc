package com.databricks.jdbc.core;

import com.databricks.jdbc.commons.util.WrapperUtil;
import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class DatabricksParameterMetaData implements ParameterMetaData {

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
    return DatabricksTypeUtil.isSigned(getObject(param).type());
  }

  @Override
  public int getPrecision(int param) throws SQLException {
    return DatabricksTypeUtil.getPrecision(getObject(param).type());
  }

  @Override
  public int getScale(int param) throws SQLException {
    return DatabricksTypeUtil.getScale(getObject(param).type());
  }

  @Override
  public int getParameterType(int param) throws SQLException {
    return DatabricksTypeUtil.getColumnType(getObject(param).type());
  }

  @Override
  public String getParameterTypeName(int param) throws SQLException {
    return getObject(param).type().name();
  }

  @Override
  public String getParameterClassName(int param) throws SQLException {
    return DatabricksTypeUtil.getColumnTypeClassName(getObject(param).type());
  }

  @Override
  public int getParameterMode(int param) throws SQLException {
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

  private ImmutableSqlParameter getObject(int param) throws DatabricksValidationException {
    if (!parameterBindings.containsKey(param)) {
      throw new DatabricksValidationException("Invalid parameter index: " + param);
    }
    return parameterBindings.get(param);
  }
}
