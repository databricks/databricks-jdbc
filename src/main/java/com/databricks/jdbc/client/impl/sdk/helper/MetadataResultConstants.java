package com.databricks.jdbc.client.impl.sdk.helper;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;

public class MetadataResultConstants {
  private static final ResultColumn CATALOG_COLUMN =
      new ResultColumn("TABLE_CAT", "catalogName", Types.VARCHAR);
  private static final ResultColumn TYPE_CATALOG_COLUMN =
      new ResultColumn("TYPE_CAT", "TYPE_CATALOG_COLUMN", Types.VARCHAR);
  private static final ResultColumn TYPE_SCHEMA_COLUMN =
      new ResultColumn("TYPE_SCHEM", "TYPE_SCHEMA_COLUMN", Types.VARCHAR);
  private static final ResultColumn SELF_REFERENCING_COLUMN_NAME =
      new ResultColumn("SELF_REFERENCING_COL_NAME", "SELF_REFERENCING_COLUMN_NAME", Types.VARCHAR);
  private static final ResultColumn REF_GENERATION_COLUMN =
      new ResultColumn("REF_GENERATION", "REF_GENERATION_COLUMN", Types.VARCHAR);
  private static final ResultColumn KEY_SEQUENCE_COLUMN =
      new ResultColumn("KEY_SEQ", "keySeq", Types.INTEGER);
  private static final ResultColumn PRIMARY_KEY_NAME_COLUMN =
      new ResultColumn("PK_NAME", "constraintName", Types.VARCHAR);
  private static final ResultColumn PRIMARY_KEY_TYPE_COLUMN =
      new ResultColumn("PK_TYPE", "constraintType", Types.VARCHAR);
  private static final ResultColumn TYPE_NAME_COLUMN =
      new ResultColumn("TYPE_NAME", "TYPE_NAME", Types.VARCHAR);
  private static final ResultColumn SCHEMA_COLUMN =
      new ResultColumn("TABLE_SCHEM", "namespace", Types.VARCHAR);
  private static final ResultColumn TABLE_NAME_COLUMN =
      new ResultColumn("TABLE_NAME", "tableName", Types.VARCHAR);
  private static final ResultColumn TABLE_TYPE_COLUMN =
      new ResultColumn("TABLE_TYPE", "tableType", Types.VARCHAR);
  private static final ResultColumn REMARKS_COLUMN =
      new ResultColumn("REMARKS", "remarks", Types.VARCHAR);
  private static final ResultColumn COLUMN_NAME_COLUMN =
      new ResultColumn("COLUMN_NAME", "columnName", Types.VARCHAR);
  private static final ResultColumn COLUMN_TYPE_COLUMN =
      new ResultColumn("DATA_TYPE", "columnType", Types.VARCHAR);
  private static final ResultColumn COLUMN_SIZE_COLUMN =
      new ResultColumn("COLUMN_SIZE", "columnType", Types.INTEGER);
  private static final ResultColumn DECIMAL_DIGITS_COLUMN =
      new ResultColumn("DECIMAL_DIGITS", "decimalDigits", Types.INTEGER);
  private static final ResultColumn COL_NAME_COLUMN =
      new ResultColumn("COLUMN_NAME", "col_name", Types.VARCHAR);
  private static final ResultColumn FUNCTION_CATALOG_COLUMN =
      new ResultColumn("FUNCTION_CAT", "catalogName", Types.VARCHAR);
  private static final ResultColumn FUNCTION_SCHEMA_COLUMN =
      new ResultColumn("FUNCTION_SCHEM", "namespace", Types.VARCHAR);
  private static final ResultColumn FUNCTION_NAME_COLUMN =
      new ResultColumn("FUNCTION_NAME", "functionName", Types.VARCHAR);
  private static final ResultColumn FUNCTION_TYPE_COLUMN =
      new ResultColumn("FUNCTION_TYPE", "functionType", Types.VARCHAR);
  private static final ResultColumn SPECIFIC_NAME_COLUMN =
      new ResultColumn("SPECIFIC_NAME", "specificName", Types.VARCHAR);
  private static final ResultColumn INFORMATION_NAME_COLUMN =
      new ResultColumn("INFO", "information", Types.VARCHAR);
  private static final ResultColumn RADIX_COLUMN =
      new ResultColumn("RADIX", "radix", Types.INTEGER);
  private static final ResultColumn IS_NULLABLE_COLUMN =
      new ResultColumn("IS_NULLABLE", "isNullable", Types.INTEGER);
  private static final ResultColumn ORDINAL_POSITION_COLUMN =
      new ResultColumn("ORDINAL_POSITION", "ordinalPosition", Types.INTEGER);
  private static final ResultColumn IS_AUTO_INCREMENT_COLUMN =
      new ResultColumn("IS_AUTOINCREMENT", "isAutoIncrement", Types.INTEGER);
  private static final ResultColumn IS_GENERATED_COLUMN =
      new ResultColumn("IS_GENERATEDCOLUMN", "isGenerated", Types.INTEGER);
  public static List<ResultColumn> FUNCTION_COLUMNS =
      List.of(
          FUNCTION_CATALOG_COLUMN,
          FUNCTION_SCHEMA_COLUMN,
          FUNCTION_NAME_COLUMN,
          REMARKS_COLUMN,
          FUNCTION_TYPE_COLUMN,
          SPECIFIC_NAME_COLUMN);
  public static List<ResultColumn> COLUMN_COLUMNS =
      List.of(
          CATALOG_COLUMN,
          SCHEMA_COLUMN,
          TABLE_NAME_COLUMN,
          COL_NAME_COLUMN,
          COLUMN_TYPE_COLUMN,
          COLUMN_SIZE_COLUMN,
          DECIMAL_DIGITS_COLUMN,
          RADIX_COLUMN,
          IS_NULLABLE_COLUMN,
          REMARKS_COLUMN,
          ORDINAL_POSITION_COLUMN,
          IS_AUTO_INCREMENT_COLUMN,
          IS_GENERATED_COLUMN);
  public static List<ResultColumn> CATALOG_COLUMNS = List.of(CATALOG_COLUMN);
  public static List<ResultColumn> SCHEMA_COLUMNS = List.of(SCHEMA_COLUMN, CATALOG_COLUMN);
  public static List<ResultColumn> TABLE_COLUMNS =
      List.of(
          CATALOG_COLUMN,
          SCHEMA_COLUMN,
          TABLE_NAME_COLUMN,
          TABLE_TYPE_COLUMN,
          REMARKS_COLUMN,
          TYPE_CATALOG_COLUMN,
          TYPE_SCHEMA_COLUMN,
          TYPE_NAME_COLUMN,
          /*Note that a few fields like the following is for backward compatibility with Simba*/
          SELF_REFERENCING_COLUMN_NAME,
          REF_GENERATION_COLUMN,
          INFORMATION_NAME_COLUMN);
  public static List<ResultColumn> PRIMARY_KEYS_COLUMNS =
      List.of(
          CATALOG_COLUMN,
          SCHEMA_COLUMN,
          TABLE_NAME_COLUMN,
          COLUMN_NAME_COLUMN,
          KEY_SEQUENCE_COLUMN,
          PRIMARY_KEY_NAME_COLUMN,
          PRIMARY_KEY_TYPE_COLUMN);
  public static List<List<Object>> TABLE_TYPES_ROWS =
      Arrays.asList(List.of("TABLE"), List.of("VIEW"), List.of("SYSTEM TABLES"));
  public static List<ResultColumn> TABLE_TYPE_COLUMNS = List.of(TABLE_TYPE_COLUMN);
  public static String NULL_STRING = "null";
}
