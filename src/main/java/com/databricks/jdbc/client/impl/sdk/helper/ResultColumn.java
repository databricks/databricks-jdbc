package com.databricks.jdbc.client.impl.sdk.helper;

import java.sql.Types;

public class ResultColumn {
  private final String columnName; // This name needs to be returned as part of the result
  private final String resultSetColumnName; // This is the corresponding column name in server
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
    return "INT"; // Currently we have only Varchar and Int metadata fields.
  }

  public Integer getColumnPrecision() {
    return 128; // Todo : fix the precisions
  }
}
