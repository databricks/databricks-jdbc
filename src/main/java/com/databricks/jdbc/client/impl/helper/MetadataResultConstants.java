package com.databricks.jdbc.client.impl.helper;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;

public class MetadataResultConstants {
  public static final String[] DEFAULT_TABLE_TYPES = {"TABLE", "VIEW", "SYSTEM TABLE"};
  private static final ResultColumn CATALOG_COLUMN =
      new ResultColumn("TABLE_CAT", "catalogName", Types.VARCHAR);

  private static final ResultColumn CATALOG_COLUMN_FOR_GET_CATALOGS =
      new ResultColumn("TABLE_CAT", "catalog", Types.VARCHAR);
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

  private static final ResultColumn SCHEMA_COLUMN_FOR_GET_SCHEMA =
      new ResultColumn("TABLE_SCHEM", "databaseName", Types.VARCHAR);

  private static final ResultColumn TABLE_NAME_COLUMN =
      new ResultColumn("TABLE_NAME", "tableName", Types.VARCHAR);
  private static final ResultColumn TABLE_TYPE_COLUMN =
      new ResultColumn("TABLE_TYPE", "tableType", Types.VARCHAR);
  private static final ResultColumn REMARKS_COLUMN =
      new ResultColumn("REMARKS", "remarks", Types.VARCHAR);
  private static final ResultColumn COLUMN_NAME_COLUMN =
      new ResultColumn("COLUMN_NAME", "columnName", Types.VARCHAR);

  private static final ResultColumn PROCEDURE_TYPE_COLUMN =
      new ResultColumn("PROCEDURE_TYPE", "procedureType", Types.SMALLINT);
  private static final ResultColumn COLUMN_TYPE_COLUMN =
      new ResultColumn("DATA_TYPE", "columnType", Types.VARCHAR);
  private static final ResultColumn BUFFER_LENGTH_COLUMN =
      new ResultColumn("BUFFER_LENGTH", "bufferLength", Types.INTEGER);
  private static final ResultColumn COLUMN_SIZE_COLUMN =
      new ResultColumn("COLUMN_SIZE", "columnType", Types.INTEGER);
  private static final ResultColumn PRECISION_COLUMN =
      new ResultColumn("PRECISION", "precision", Types.INTEGER);
  private static final ResultColumn COLUMN_DEF_COLUMN =
      new ResultColumn("COLUMN_DEF", "columnType", Types.VARCHAR);
  private static final ResultColumn DECIMAL_DIGITS_COLUMN =
      new ResultColumn("DECIMAL_DIGITS", "decimalDigits", Types.SMALLINT);
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
  private static final ResultColumn NUM_PREC_RADIX_COLUMN =
      new ResultColumn("NUM_PREC_RADIX", "numPrecRadix", Types.SMALLINT);
  private static final ResultColumn RADIX_COLUMN =
      new ResultColumn("RADIX", "radix", Types.INTEGER);
  private static final ResultColumn IS_NULLABLE_COLUMN =
      new ResultColumn("IS_NULLABLE", "isNullable", Types.INTEGER);
  private static final ResultColumn SQL_DATA_TYPE_COLUMN =
      new ResultColumn("SQL_DATA_TYPE", "SQLDataType", Types.SMALLINT);
  private static final ResultColumn DATA_TYPE_COLUMN =
      new ResultColumn("DATA_TYPE", "dataType", Types.INTEGER);
  private static final ResultColumn LITERAL_PREFIX_COLUMN =
      new ResultColumn("LITERAL_PREFIX", "literalPrefix", Types.VARCHAR);
  private static final ResultColumn LITERAL_SUFFIX_COLUMN =
      new ResultColumn("LITERAL_SUFFIX", "literalSuffix", Types.VARCHAR);
  private static final ResultColumn CREATE_PARAMS_COLUMN =
      new ResultColumn("CREATE_PARAMS", "createParams", Types.VARCHAR);
  private static final ResultColumn SQL_DATETIME_SUB_COLUMN =
      new ResultColumn("SQL_DATETIME_SUB", "SQLDateTimeSub", Types.SMALLINT);
  private static final ResultColumn CHAR_OCTET_LENGTH_COLUMN =
      new ResultColumn("CHAR_OCTET_LENGTH", "CharOctetLength", Types.INTEGER);
  private static final ResultColumn USER_DATA_TYPE_COLUMN =
      new ResultColumn("USER_DATA_TYPE", "UserDataType", Types.SMALLINT);
  private static final ResultColumn NULLABLE_COLUMN =
      new ResultColumn("NULLABLE", "isNullable", Types.SMALLINT);
  private static final ResultColumn ORDINAL_POSITION_COLUMN =
      new ResultColumn("ORDINAL_POSITION", "ordinalPosition", Types.INTEGER);
  private static final ResultColumn IS_AUTO_INCREMENT_COLUMN =
      new ResultColumn("IS_AUTOINCREMENT", "isAutoIncrement", Types.INTEGER);
  private static final ResultColumn IS_GENERATED_COLUMN =
      new ResultColumn("IS_GENERATEDCOLUMN", "isGenerated", Types.VARCHAR);
  private static final ResultColumn CASE_SENSITIVE_COLUMN =
      new ResultColumn("CASE_SENSITIVE", "caseSensitive", Types.BIT);
  private static final ResultColumn SEARCHABLE_COLUMN =
      new ResultColumn("SEARCHABLE", "searchable", Types.SMALLINT);
  private static final ResultColumn UNSIGNED_ATTRIBUTE_COLUMN =
      new ResultColumn("UNSIGNED_ATTRIBUTE", "unsignedAttribute", Types.BIT);
  private static final ResultColumn FIXED_PREC_SCALE_COLUMN =
      new ResultColumn("FIXED_PREC_SCALE", "fixedPrecScale", Types.BIT);

  private static final ResultColumn AUTO_INCREMENT_COLUMN =
      new ResultColumn("AUTO_INCREMENT", "autoIncrement", Types.BIT);
  private static final ResultColumn LOCAL_TYPE_NAME_COLUMN =
      new ResultColumn("LOCAL_TYPE_NAME", "localTypeName", Types.VARCHAR);
  private static final ResultColumn MINIMUM_SCALE_COLUMN =
      new ResultColumn("MINIMUM_SCALE", "minimumScale", Types.SMALLINT);
  private static final ResultColumn MAXIMUM_SCALE_COLUMN =
      new ResultColumn("MAXIMUM_SCALE", "maximumScale", Types.SMALLINT);
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
  public static List<ResultColumn> CATALOG_COLUMNS = List.of(CATALOG_COLUMN_FOR_GET_CATALOGS);
  public static List<ResultColumn> SCHEMA_COLUMNS =
      List.of(SCHEMA_COLUMN_FOR_GET_SCHEMA, CATALOG_COLUMN);
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
          /*Note that a few fields like the following is for backward compatibility*/
          SELF_REFERENCING_COLUMN_NAME,
          REF_GENERATION_COLUMN,
          INFORMATION_NAME_COLUMN);

  public static List<ResultColumn> TABLE_COLUMNS_ALL_PURPOSE =
      List.of(CATALOG_COLUMN, SCHEMA_COLUMN, TABLE_NAME_COLUMN, TABLE_TYPE_COLUMN, REMARKS_COLUMN);
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
  public static List<ResultColumn> PRIMARY_KEYS_COLUMNS_ALL_PURPOSE =
      List.of(
          CATALOG_COLUMN,
          SCHEMA_COLUMN,
          TABLE_NAME_COLUMN,
          COLUMN_NAME_COLUMN,
          KEY_SEQUENCE_COLUMN,
          PRIMARY_KEY_NAME_COLUMN);

  public static List<ResultColumn> FUNCTION_COLUMNS_ALL_PURPOSE =
      List.of(
          FUNCTION_CATALOG_COLUMN,
          FUNCTION_SCHEMA_COLUMN,
          FUNCTION_NAME_COLUMN,
          REMARKS_COLUMN,
          PROCEDURE_TYPE_COLUMN,
          SPECIFIC_NAME_COLUMN);

  public static List<ResultColumn> COLUMN_COLUMNS_ALL_PURPOSE =
      List.of(
          CATALOG_COLUMN,
          SCHEMA_COLUMN,
          TABLE_NAME_COLUMN,
          COLUMN_NAME_COLUMN,
          COLUMN_TYPE_COLUMN,
          TYPE_NAME_COLUMN,
          COLUMN_SIZE_COLUMN,
          BUFFER_LENGTH_COLUMN,
          DECIMAL_DIGITS_COLUMN,
          NUM_PREC_RADIX_COLUMN,
          NULLABLE_COLUMN,
          REMARKS_COLUMN,
          COLUMN_DEF_COLUMN,
          SQL_DATA_TYPE_COLUMN,
          SQL_DATETIME_SUB_COLUMN,
          CHAR_OCTET_LENGTH_COLUMN,
          ORDINAL_POSITION_COLUMN,
          IS_NULLABLE_COLUMN,
          IS_AUTO_INCREMENT_COLUMN,
          USER_DATA_TYPE_COLUMN,
          IS_GENERATED_COLUMN);
  public static String NULL_STRING = "null";

  public static List<ResultColumn> TYPE_INFO_COLUMNS =
      List.of(
          TYPE_NAME_COLUMN,
          DATA_TYPE_COLUMN,
          PRECISION_COLUMN,
          LITERAL_PREFIX_COLUMN,
          LITERAL_SUFFIX_COLUMN,
          CREATE_PARAMS_COLUMN,
          NULLABLE_COLUMN,
          CASE_SENSITIVE_COLUMN,
          SEARCHABLE_COLUMN,
          UNSIGNED_ATTRIBUTE_COLUMN,
          FIXED_PREC_SCALE_COLUMN,
          AUTO_INCREMENT_COLUMN,
          LOCAL_TYPE_NAME_COLUMN,
          MINIMUM_SCALE_COLUMN,
          MAXIMUM_SCALE_COLUMN,
          SQL_DATA_TYPE_COLUMN,
          SQL_DATETIME_SUB_COLUMN,
          NUM_PREC_RADIX_COLUMN);
}
