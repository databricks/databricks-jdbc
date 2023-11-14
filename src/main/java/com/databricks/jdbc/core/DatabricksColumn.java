package com.databricks.jdbc.core;

import com.databricks.jdbc.core.types.AccessType;
import com.databricks.jdbc.core.types.Nullable;
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

  String schemaName(); // TODO

  boolean isCurrency(); // TODO

  boolean isAutoIncrement(); // TODO

  boolean isCaseSensitive();

  boolean isSearchable(); // TODO

  Nullable nullable(); // TODO

  int scale(); // TODO

  AccessType accessType(); // TODO

  boolean isDefinitelyWritable(); // TODO

  String columnTypeClassName();

  String tableName(); // TODO

  String catalogName(); // TODO
}
