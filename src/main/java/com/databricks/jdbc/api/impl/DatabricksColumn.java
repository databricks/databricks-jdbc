package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.common.AccessType;
import com.databricks.jdbc.common.Nullable;
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

  int displaySize();

  boolean isSigned();

  @javax.annotation.Nullable
  String schemaName();

  boolean isCurrency();

  boolean isAutoIncrement();

  boolean isCaseSensitive();

  boolean isSearchable();

  Nullable nullable();

  int typeScale();

  AccessType accessType();

  boolean isDefinitelyWritable();

  String columnTypeClassName();

  @javax.annotation.Nullable
  String tableName();

  String catalogName();
}
