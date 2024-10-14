package com.databricks.jdbc.common;

import com.databricks.jdbc.model.core.ResultColumn;
import java.sql.Types;
import java.util.*;

public class MetadataResultConstants {
  public static final String[] DEFAULT_TABLE_TYPES = {"TABLE", "VIEW", "SYSTEM TABLE"};
  public static final ResultColumn CATALOG_COLUMN =
      new ResultColumn("TABLE_CAT", "catalogName", Types.VARCHAR);
  private static final ResultColumn CATALOG_FULL_COLUMN =
      new ResultColumn("TABLE_CATALOG", "catalogName", Types.VARCHAR);
  public static final ResultColumn CATALOG_COLUMN_FOR_GET_CATALOGS =
      new ResultColumn("TABLE_CAT", "catalog", Types.VARCHAR);
  public static final ResultColumn TYPE_CATALOG_COLUMN =
      new ResultColumn("TYPE_CAT", "TYPE_CATALOG_COLUMN", Types.VARCHAR);
  public static final ResultColumn TYPE_SCHEMA_COLUMN =
      new ResultColumn("TYPE_SCHEM", "TYPE_SCHEMA_COLUMN", Types.VARCHAR);
  public static final ResultColumn SELF_REFERENCING_COLUMN_NAME =
      new ResultColumn("SELF_REFERENCING_COL_NAME", "SELF_REFERENCING_COLUMN_NAME", Types.VARCHAR);
  public static final ResultColumn REF_GENERATION_COLUMN =
      new ResultColumn("REF_GENERATION", "REF_GENERATION_COLUMN", Types.VARCHAR);
  public static final ResultColumn KEY_SEQUENCE_COLUMN =
      new ResultColumn("KEY_SEQ", "keySeq", Types.SMALLINT);
  public static final ResultColumn PRIMARY_KEY_NAME_COLUMN =
      new ResultColumn("PK_NAME", "constraintName", Types.VARCHAR);
  public static final ResultColumn TYPE_NAME_COLUMN =
      new ResultColumn("TYPE_NAME", "TYPE_NAME", Types.VARCHAR);
  public static final ResultColumn SCHEMA_COLUMN =
      new ResultColumn("TABLE_SCHEM", "namespace", Types.VARCHAR);
  private static final ResultColumn SCHEMA_COLUMN_FOR_GET_SCHEMA =
      new ResultColumn("TABLE_SCHEM", "databaseName", Types.VARCHAR);
  public static final ResultColumn TABLE_NAME_COLUMN =
      new ResultColumn("TABLE_NAME", "tableName", Types.VARCHAR);
  public static final ResultColumn TABLE_TYPE_COLUMN =
      new ResultColumn("TABLE_TYPE", "tableType", Types.VARCHAR);
  public static final ResultColumn REMARKS_COLUMN =
      new ResultColumn("REMARKS", "remarks", Types.VARCHAR);
  private static final ResultColumn COLUMN_NAME_COLUMN =
      new ResultColumn("COLUMN_NAME", "columnName", Types.VARCHAR);
  public static final ResultColumn COLUMN_TYPE_COLUMN =
      new ResultColumn("TYPE_NAME", "columnType", Types.VARCHAR);
  public static final ResultColumn BUFFER_LENGTH_COLUMN =
      new ResultColumn("BUFFER_LENGTH", "bufferLength", Types.INTEGER);
  public static final ResultColumn COLUMN_SIZE_COLUMN =
      new ResultColumn("COLUMN_SIZE", "columnSize", Types.INTEGER);
  private static final ResultColumn PRECISION_COLUMN =
      new ResultColumn("PRECISION", "precision", Types.INTEGER);
  public static final ResultColumn COLUMN_DEF_COLUMN =
      new ResultColumn("COLUMN_DEF", "columnType", Types.VARCHAR);
  public static final ResultColumn DECIMAL_DIGITS_COLUMN =
      new ResultColumn("DECIMAL_DIGITS", "decimalDigits", Types.INTEGER);
  public static final ResultColumn COL_NAME_COLUMN =
      new ResultColumn("COLUMN_NAME", "col_name", Types.VARCHAR);
  public static final ResultColumn FUNCTION_CATALOG_COLUMN =
      new ResultColumn("FUNCTION_CAT", "catalogName", Types.VARCHAR);
  public static final ResultColumn FUNCTION_SCHEMA_COLUMN =
      new ResultColumn("FUNCTION_SCHEM", "namespace", Types.VARCHAR);
  public static final ResultColumn FUNCTION_NAME_COLUMN =
      new ResultColumn("FUNCTION_NAME", "functionName", Types.VARCHAR);
  public static final ResultColumn FUNCTION_TYPE_COLUMN =
      new ResultColumn("FUNCTION_TYPE", "functionType", Types.SMALLINT);
  public static final ResultColumn SPECIFIC_NAME_COLUMN =
      new ResultColumn("SPECIFIC_NAME", "specificName", Types.VARCHAR);
  public static final ResultColumn NUM_PREC_RADIX_COLUMN =
      new ResultColumn("NUM_PREC_RADIX", "radix", Types.INTEGER);
  public static final ResultColumn IS_NULLABLE_COLUMN =
      new ResultColumn("IS_NULLABLE", "isNullable", Types.VARCHAR);
  public static final ResultColumn SQL_DATA_TYPE_COLUMN =
      new ResultColumn("SQL_DATA_TYPE", "SQLDataType", Types.INTEGER);
  public static final ResultColumn DATA_TYPE_COLUMN =
      new ResultColumn("DATA_TYPE", "dataType", Types.INTEGER);
  private static final ResultColumn LITERAL_PREFIX_COLUMN =
      new ResultColumn("LITERAL_PREFIX", "literalPrefix", Types.VARCHAR);
  private static final ResultColumn LITERAL_SUFFIX_COLUMN =
      new ResultColumn("LITERAL_SUFFIX", "literalSuffix", Types.VARCHAR);
  private static final ResultColumn CREATE_PARAMS_COLUMN =
      new ResultColumn("CREATE_PARAMS", "createParams", Types.VARCHAR);
  public static final ResultColumn SQL_DATETIME_SUB_COLUMN =
      new ResultColumn("SQL_DATETIME_SUB", "SQLDateTimeSub", Types.INTEGER);
  public static final ResultColumn CHAR_OCTET_LENGTH_COLUMN =
      new ResultColumn("CHAR_OCTET_LENGTH", "CharOctetLength", Types.INTEGER);
  public static final ResultColumn SCOPE_CATALOG_COLUMN =
      new ResultColumn("SCOPE_CATALOG", "ScopeCatalog", Types.VARCHAR);
  public static final ResultColumn SCOPE_SCHEMA_COLUMN =
      new ResultColumn("SCOPE_SCHEMA", "ScopeSchema", Types.VARCHAR);
  public static final ResultColumn SCOPE_TABLE_COLUMN =
      new ResultColumn("SCOPE_TABLE", "ScopeTable", Types.VARCHAR);
  public static final ResultColumn SOURCE_DATA_TYPE_COLUMN =
      new ResultColumn("SOURCE_DATA_TYPE", "SourceDataType", Types.INTEGER);
  public static final ResultColumn NULLABLE_COLUMN =
      new ResultColumn("NULLABLE", "Nullable", Types.INTEGER);
  public static final ResultColumn ORDINAL_POSITION_COLUMN =
      new ResultColumn("ORDINAL_POSITION", "ordinalPosition", Types.INTEGER);
  public static final ResultColumn IS_AUTO_INCREMENT_COLUMN =
      new ResultColumn("IS_AUTOINCREMENT", "isAutoIncrement", Types.VARCHAR);
  public static final ResultColumn IS_GENERATED_COLUMN =
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
      Arrays.asList(
          FUNCTION_CATALOG_COLUMN,
          FUNCTION_SCHEMA_COLUMN,
          FUNCTION_NAME_COLUMN,
          REMARKS_COLUMN,
          FUNCTION_TYPE_COLUMN,
          SPECIFIC_NAME_COLUMN);
  public static List<ResultColumn> COLUMN_COLUMNS =
      Arrays.asList(
          CATALOG_COLUMN,
          SCHEMA_COLUMN,
          TABLE_NAME_COLUMN,
          COL_NAME_COLUMN,
          DATA_TYPE_COLUMN,
          COLUMN_TYPE_COLUMN,
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
          SCOPE_CATALOG_COLUMN,
          SCOPE_SCHEMA_COLUMN,
          SCOPE_TABLE_COLUMN,
          SOURCE_DATA_TYPE_COLUMN,
          IS_AUTO_INCREMENT_COLUMN,
          IS_GENERATED_COLUMN);
  public static List<ResultColumn> CATALOG_COLUMNS = Arrays.asList(CATALOG_COLUMN_FOR_GET_CATALOGS);
  public static List<ResultColumn> SCHEMA_COLUMNS =
      Arrays.asList(SCHEMA_COLUMN_FOR_GET_SCHEMA, CATALOG_FULL_COLUMN);
  public static List<ResultColumn> TABLE_COLUMNS =
      Arrays.asList(
          CATALOG_COLUMN,
          SCHEMA_COLUMN,
          TABLE_NAME_COLUMN,
          TABLE_TYPE_COLUMN,
          REMARKS_COLUMN,
          TYPE_CATALOG_COLUMN,
          TYPE_SCHEMA_COLUMN,
          TYPE_NAME_COLUMN,
          // Note that a few fields like the following is for backward compatibility
          SELF_REFERENCING_COLUMN_NAME,
          REF_GENERATION_COLUMN);
  public static List<ResultColumn> PRIMARY_KEYS_COLUMNS =
      Arrays.asList(
          CATALOG_COLUMN,
          SCHEMA_COLUMN,
          TABLE_NAME_COLUMN,
          COLUMN_NAME_COLUMN,
          KEY_SEQUENCE_COLUMN,
          PRIMARY_KEY_NAME_COLUMN);
  public static List<List<Object>> TABLE_TYPES_ROWS =
      Arrays.asList(Arrays.asList("TABLE"), Arrays.asList("VIEW"), Arrays.asList("SYSTEM TABLE"));
  public static List<ResultColumn> TABLE_TYPE_COLUMNS = Arrays.asList(TABLE_TYPE_COLUMN);
  public static String NULL_STRING = "NULL";
  public static List<ResultColumn> TYPE_INFO_COLUMNS =
      Arrays.asList(
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
  public static final Map<CommandName, List<ResultColumn>> NON_NULLABLE_COLUMNS_MAP;

  static {
    Map<CommandName, List<ResultColumn>> map = new HashMap<>();
    map.put(
        CommandName.LIST_TYPE_INFO,
        Arrays.asList(
            MetadataResultConstants.TYPE_NAME_COLUMN,
            MetadataResultConstants.DATA_TYPE_COLUMN,
            MetadataResultConstants
                .PRECISION_COLUMN // Assuming COLUMN_SIZE_COLUMN maps to precision
            ));
    map.put(
        CommandName.LIST_CATALOGS,
        Arrays.asList(MetadataResultConstants.CATALOG_COLUMN_FOR_GET_CATALOGS));
    map.put(CommandName.LIST_TABLES, Arrays.asList(MetadataResultConstants.TABLE_NAME_COLUMN));
    map.put(
        CommandName.LIST_PRIMARY_KEYS,
        Arrays.asList(
            MetadataResultConstants.TABLE_NAME_COLUMN, MetadataResultConstants.COLUMN_NAME_COLUMN));
    map.put(
        CommandName.LIST_SCHEMAS,
        Arrays.asList(MetadataResultConstants.SCHEMA_COLUMN_FOR_GET_SCHEMA));
    map.put(CommandName.LIST_TABLE_TYPES, Arrays.asList(MetadataResultConstants.TABLE_TYPE_COLUMN));
    map.put(
        CommandName.LIST_COLUMNS,
        Arrays.asList(
            MetadataResultConstants.TABLE_NAME_COLUMN,
            MetadataResultConstants.COL_NAME_COLUMN,
            MetadataResultConstants.DATA_TYPE_COLUMN,
            MetadataResultConstants.COLUMN_TYPE_COLUMN,
            MetadataResultConstants.NULLABLE_COLUMN,
            MetadataResultConstants.SQL_DATA_TYPE_COLUMN,
            MetadataResultConstants.ORDINAL_POSITION_COLUMN,
            MetadataResultConstants.IS_NULLABLE_COLUMN));
    map.put(
        CommandName.LIST_FUNCTIONS,
        Arrays.asList(
            MetadataResultConstants.FUNCTION_NAME_COLUMN,
            MetadataResultConstants.SPECIFIC_NAME_COLUMN));
    NON_NULLABLE_COLUMNS_MAP = Collections.unmodifiableMap(map);
  }
}
