package com.databricks.jdbc.model.core;

import java.sql.Types;

public class ResultColumn {

  /** The name that needs to be returned as part of the result. */
  private final String columnName;

  /** The corresponding column name in server. */
  private final String resultSetColumnName;

  private final Integer columnType;

  public ResultColumn(String columnName, String resultSetColumnName, Integer columnType) {
    this.columnName = columnName;
    this.resultSetColumnName = resultSetColumnName;
    this.columnType = columnType;
  }

  public String getColumnName() {
    return columnName;
  }

  public String getResultSetColumnName() {
    return resultSetColumnName;
  }

  public Integer getColumnTypeInt() {
    return columnType;
  }

  public String getColumnTypeString() {
    if (columnType.equals(Types.VARCHAR)) {
      return "VARCHAR";
    }
    return "INTEGER"; // Currently we have only Varchar and Int metadata fields.
  }

  public Integer getColumnPrecision() {
    return 128; // Todo : fix the precisions
  }
}
