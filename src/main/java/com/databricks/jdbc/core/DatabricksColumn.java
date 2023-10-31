package com.databricks.jdbc.core;

import org.immutables.value.Value;

@Value.Immutable
public interface DatabricksColumn {

  /** Name of the column in result set */
  String columnName();

  /** Type of the column in result set */
  int columnType();

  /** Full data type spec, SQL/catalogString text */
  String columnTypeText();

  /**
   * Precision is the maximum number of significant digits that can be stored in a column. For
   * string, it's 255.
   */
  int typePrecision();
}
