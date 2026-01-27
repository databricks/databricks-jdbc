package com.databricks.jdbc.common;

import com.databricks.jdbc.model.core.ResultColumn;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetadataResultConstants {
  public static String NULL_STRING = "NULL";
  public static final String PARSE_SYNTAX_ERROR_SQL_STATE = "42601";
  public static final String[] DEFAULT_TABLE_TYPES = {
    "TABLE", "VIEW", "SYSTEM TABLE", "METRIC_VIEW"
  };
  public static final ResultColumn CATALOG_COLUMN =
      new ResultColumn("TABLE_CAT", "catalogName", Types.VARCHAR);
  public static final ResultColumn CATALOG_FULL_COLUMN =
      new ResultColumn("TABLE_CATALOG", "catalogName", Types.VARCHAR);
  public static final ResultColumn CATALOG_RESULT_COLUMN =
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
  private static final ResultColumn ATTR_NAME =
      new ResultColumn("ATTR_NAME", "attrName", Types.VARCHAR);
  private static final ResultColumn ATTR_TYPE_NAME =
      new ResultColumn("ATTR_TYPE_NAME", "attrTypeName", Types.VARCHAR);
  private static final ResultColumn ATTR_SIZE =
      new ResultColumn("ATTR_SIZE", "attrSize", Types.INTEGER);
  private static final ResultColumn ATTR_DEF =
      new ResultColumn("ATTR_DEF", "attrDef", Types.VARCHAR);
  private static final ResultColumn SOURCE_DATA_TYPE =
      new ResultColumn("SOURCE_DATA_TYPE", "SourceDataType", Types.SMALLINT);
  private static final ResultColumn GRANTOR = new ResultColumn("GRANTOR", "grantor", Types.VARCHAR);
  private static final ResultColumn GRANTEE = new ResultColumn("GRANTEE", "grantee", Types.VARCHAR);
  private static final ResultColumn PRIVILEGE =
      new ResultColumn("PRIVILEGE", "privilege", Types.VARCHAR);
  private static final ResultColumn IS_GRANTABLE =
      new ResultColumn("IS_GRANTABLE", "isGrantable", Types.VARCHAR);
  private static final ResultColumn SCOPE = new ResultColumn("SCOPE", "scope", Types.SMALLINT);
  private static final ResultColumn DECIMAL_DIGITS_SHORT =
      new ResultColumn("DECIMAL_DIGITS", "decimalDigits", Types.SMALLINT);
  private static final ResultColumn PSEUDO_COLUMN =
      new ResultColumn("PSEUDO_COLUMN", "pseudoColumn", Types.SMALLINT);
  public static final ResultColumn PKTABLE_CAT =
      new ResultColumn("PKTABLE_CAT", "pktableCat", Types.VARCHAR);
  public static final ResultColumn PKTABLE_SCHEM =
      new ResultColumn("PKTABLE_SCHEM", "pktableSchem", Types.VARCHAR);
  public static final ResultColumn PKTABLE_NAME =
      new ResultColumn("PKTABLE_NAME", "pktableName", Types.VARCHAR);
  public static final ResultColumn PKCOLUMN_NAME =
      new ResultColumn("PKCOLUMN_NAME", "pkcolumnName", Types.VARCHAR);
  public static final ResultColumn FKTABLE_CAT =
      new ResultColumn("FKTABLE_CAT", "fktableCat", Types.VARCHAR);
  public static final ResultColumn FKTABLE_SCHEM =
      new ResultColumn("FKTABLE_SCHEM", "fktableSchem", Types.VARCHAR);
  public static final ResultColumn FKTABLE_NAME =
      new ResultColumn("FKTABLE_NAME", "fktableName", Types.VARCHAR);
  public static final ResultColumn FKCOLUMN_NAME =
      new ResultColumn("FKCOLUMN_NAME", "fkcolumnName", Types.VARCHAR);
  public static final ResultColumn UPDATE_RULE =
      new ResultColumn("UPDATE_RULE", "updateRule", Types.SMALLINT);
  public static final ResultColumn DELETE_RULE =
      new ResultColumn("DELETE_RULE", "deleteRule", Types.SMALLINT);
  public static final ResultColumn FK_NAME = new ResultColumn("FK_NAME", "fkName", Types.VARCHAR);
  public static final ResultColumn PK_NAME = new ResultColumn("PK_NAME", "pkName", Types.VARCHAR);
  public static final ResultColumn DEFERRABILITY =
      new ResultColumn("DEFERRABILITY", "deferrability", Types.SMALLINT);
  private static final ResultColumn SUPERTYPE_CAT =
      new ResultColumn("SUPERTYPE_CAT", "supertypeCat", Types.VARCHAR);
  private static final ResultColumn SUPERTYPE_SCHEM =
      new ResultColumn("SUPERTYPE_SCHEM", "supertypeSchem", Types.VARCHAR);
  private static final ResultColumn SUPERTYPE_NAME =
      new ResultColumn("SUPERTYPE_NAME", "supertypeName", Types.VARCHAR);
  private static final ResultColumn PROCEDURE_CAT =
      new ResultColumn("PROCEDURE_CAT", "procedureCat", Types.VARCHAR);
  private static final ResultColumn PROCEDURE_SCHEM =
      new ResultColumn("PROCEDURE_SCHEM", "procedureSchem", Types.VARCHAR);
  private static final ResultColumn PROCEDURE_NAME =
      new ResultColumn("PROCEDURE_NAME", "procedureName", Types.VARCHAR);
  private static final ResultColumn COLUMN_TYPE =
      new ResultColumn("COLUMN_TYPE", "columnType", Types.SMALLINT);
  private static final ResultColumn LENGTH = new ResultColumn("LENGTH", "length", Types.INTEGER);
  private static final ResultColumn SCALE = new ResultColumn("SCALE", "scale", Types.SMALLINT);
  private static final ResultColumn RADIX = new ResultColumn("RADIX", "radix", Types.SMALLINT);
  private static final ResultColumn NULLABLE_SHORT =
      new ResultColumn("NULLABLE", "nullable", Types.SMALLINT);
  private static final ResultColumn NON_UNIQUE =
      new ResultColumn("NON_UNIQUE", "nonUnique", Types.BOOLEAN);
  private static final ResultColumn INDEX_QUALIFIER =
      new ResultColumn("INDEX_QUALIFIER", "indexQualifier", Types.VARCHAR);
  private static final ResultColumn INDEX_NAME =
      new ResultColumn("INDEX_NAME", "indexName", Types.VARCHAR);
  private static final ResultColumn TYPE = new ResultColumn("TYPE", "type", Types.SMALLINT);
  private static final ResultColumn ORDINAL_POSITION_SHORT =
      new ResultColumn("ORDINAL_POSITION", "ordinalPosition", Types.SMALLINT);
  private static final ResultColumn ASC_OR_DESC =
      new ResultColumn("ASC_OR_DESC", "ascOrDesc", Types.VARCHAR);
  private static final ResultColumn CARDINALITY =
      new ResultColumn("CARDINALITY", "cardinality", Types.BIGINT);
  private static final ResultColumn PAGES = new ResultColumn("PAGES", "pages", Types.BIGINT);
  private static final ResultColumn FILTER_CONDITION =
      new ResultColumn("FILTER_CONDITION", "filterCondition", Types.VARCHAR);
  private static final ResultColumn SUPERTABLE_NAME =
      new ResultColumn("SUPERTABLE_NAME", "supertableName", Types.VARCHAR);
  private static final ResultColumn COLUMN_USAGE =
      new ResultColumn("COLUMN_USAGE", "columnUsage", Types.VARCHAR);

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

  public static List<ResultColumn> CATALOG_COLUMNS = List.of(CATALOG_RESULT_COLUMN);

  public static List<ResultColumn> SCHEMA_COLUMNS =
      List.of(SCHEMA_COLUMN_FOR_GET_SCHEMA, CATALOG_FULL_COLUMN);

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
          // Note that a few fields like the following is for backward compatibility
          SELF_REFERENCING_COLUMN_NAME,
          REF_GENERATION_COLUMN);

  public static List<ResultColumn> PRIMARY_KEYS_COLUMNS =
      List.of(
          CATALOG_COLUMN,
          SCHEMA_COLUMN,
          TABLE_NAME_COLUMN,
          COL_NAME_COLUMN,
          KEY_SEQUENCE_COLUMN,
          PRIMARY_KEY_NAME_COLUMN);

  public static List<List<Object>> TABLE_TYPES_ROWS =
      Arrays.asList(
          List.of("SYSTEM TABLE"), List.of("TABLE"), List.of("VIEW"), List.of("METRIC_VIEW"));

  public static List<ResultColumn> TABLE_TYPE_COLUMNS = List.of(TABLE_TYPE_COLUMN);

  public static final List<ResultColumn> NULL_COLUMN_COLUMNS =
      List.of(
          SCOPE_CATALOG_COLUMN,
          SCOPE_SCHEMA_COLUMN,
          SCOPE_TABLE_COLUMN,
          SOURCE_DATA_TYPE_COLUMN,
          IS_AUTO_INCREMENT_COLUMN,
          IS_GENERATED_COLUMN);

  public static final List<ResultColumn> NULL_TABLE_COLUMNS =
      List.of(
          TYPE_CATALOG_COLUMN,
          TYPE_SCHEMA_COLUMN,
          TYPE_NAME_COLUMN,
          SELF_REFERENCING_COLUMN_NAME,
          REF_GENERATION_COLUMN);

  public static final List<ResultColumn> LARGE_DISPLAY_COLUMNS =
      List.of(
          REMARKS_COLUMN,
          SPECIFIC_NAME_COLUMN,
          COLUMN_DEF_COLUMN,
          IS_NULLABLE_COLUMN,
          SCOPE_CATALOG_COLUMN,
          SCOPE_SCHEMA_COLUMN,
          SCOPE_TABLE_COLUMN,
          ATTR_DEF,
          COLUMN_USAGE);

  public static final List<ResultColumn> ATTRIBUTES_COLUMNS =
      List.of(
          TYPE_CATALOG_COLUMN,
          TYPE_SCHEMA_COLUMN,
          TYPE_NAME_COLUMN,
          ATTR_NAME,
          DATA_TYPE_COLUMN,
          ATTR_TYPE_NAME,
          ATTR_SIZE,
          DECIMAL_DIGITS_COLUMN,
          NUM_PREC_RADIX_COLUMN,
          NULLABLE_COLUMN,
          REMARKS_COLUMN,
          ATTR_DEF,
          SQL_DATA_TYPE_COLUMN,
          SQL_DATETIME_SUB_COLUMN,
          CHAR_OCTET_LENGTH_COLUMN,
          ORDINAL_POSITION_COLUMN,
          IS_NULLABLE_COLUMN,
          SCOPE_CATALOG_COLUMN,
          SCOPE_SCHEMA_COLUMN,
          SCOPE_TABLE_COLUMN,
          SOURCE_DATA_TYPE);

  public static final List<ResultColumn> COLUMN_PRIVILEGES_COLUMNS =
      List.of(
          CATALOG_COLUMN,
          SCHEMA_COLUMN,
          TABLE_NAME_COLUMN,
          COLUMN_NAME_COLUMN,
          GRANTOR,
          GRANTEE,
          PRIVILEGE,
          IS_GRANTABLE);

  public static final List<ResultColumn> BEST_ROW_IDENTIFIER_COLUMNS =
      List.of(
          SCOPE,
          COL_NAME_COLUMN,
          DATA_TYPE_COLUMN,
          COLUMN_TYPE_COLUMN,
          COLUMN_SIZE_COLUMN,
          BUFFER_LENGTH_COLUMN,
          DECIMAL_DIGITS_SHORT,
          PSEUDO_COLUMN);

  public static List<ResultColumn> CROSS_REFERENCE_COLUMNS =
      List.of(
          PKTABLE_CAT,
          PKTABLE_SCHEM,
          PKTABLE_NAME,
          PKCOLUMN_NAME,
          FKTABLE_CAT,
          FKTABLE_SCHEM,
          FKTABLE_NAME,
          FKCOLUMN_NAME,
          KEY_SEQUENCE_COLUMN,
          UPDATE_RULE,
          DELETE_RULE,
          FK_NAME,
          PK_NAME,
          DEFERRABILITY);

  public static final List<ResultColumn> TABLE_PRIVILEGES_COLUMNS =
      List.of(
          CATALOG_COLUMN,
          SCHEMA_COLUMN,
          TABLE_NAME_COLUMN,
          GRANTOR,
          GRANTEE,
          PRIVILEGE,
          IS_GRANTABLE);

  public static final List<ResultColumn> VERSION_COLUMNS_COLUMNS =
      List.of(
          SCOPE,
          COL_NAME_COLUMN,
          DATA_TYPE_COLUMN,
          TYPE_NAME_COLUMN,
          COLUMN_SIZE_COLUMN,
          BUFFER_LENGTH_COLUMN,
          DECIMAL_DIGITS_SHORT,
          PSEUDO_COLUMN);

  public static final List<ResultColumn> SUPER_TYPES_COLUMNS =
      List.of(
          TYPE_CATALOG_COLUMN,
          TYPE_SCHEMA_COLUMN,
          TYPE_NAME_COLUMN,
          SUPERTYPE_CAT,
          SUPERTYPE_SCHEM,
          SUPERTYPE_NAME);

  public static final List<ResultColumn> PROCEDURE_COLUMNS_COLUMNS =
      List.of(
          PROCEDURE_CAT,
          PROCEDURE_SCHEM,
          PROCEDURE_NAME,
          COLUMN_NAME_COLUMN,
          COLUMN_TYPE,
          DATA_TYPE_COLUMN,
          TYPE_NAME_COLUMN,
          PRECISION_COLUMN,
          LENGTH,
          SCALE,
          RADIX,
          NULLABLE_SHORT,
          REMARKS_COLUMN,
          COLUMN_DEF_COLUMN,
          SQL_DATA_TYPE_COLUMN,
          SQL_DATETIME_SUB_COLUMN,
          CHAR_OCTET_LENGTH_COLUMN,
          ORDINAL_POSITION_COLUMN,
          IS_NULLABLE_COLUMN,
          SPECIFIC_NAME_COLUMN);

  public static final List<ResultColumn> INDEX_INFO_COLUMNS =
      List.of(
          CATALOG_COLUMN,
          SCHEMA_COLUMN,
          TABLE_NAME_COLUMN,
          NON_UNIQUE,
          INDEX_QUALIFIER,
          INDEX_NAME,
          TYPE,
          ORDINAL_POSITION_SHORT,
          COLUMN_NAME_COLUMN,
          ASC_OR_DESC,
          CARDINALITY,
          PAGES,
          FILTER_CONDITION);

  public static final List<ResultColumn> SUPER_TABLES_COLUMNS =
      List.of(CATALOG_COLUMN, SCHEMA_COLUMN, TABLE_NAME_COLUMN, SUPERTABLE_NAME);

  public static final List<ResultColumn> FUNCTION_COLUMNS_COLUMNS =
      List.of(
          FUNCTION_CATALOG_COLUMN,
          FUNCTION_SCHEMA_COLUMN,
          FUNCTION_NAME_COLUMN,
          COLUMN_NAME_COLUMN,
          COLUMN_TYPE,
          DATA_TYPE_COLUMN,
          TYPE_NAME_COLUMN,
          PRECISION_COLUMN,
          LENGTH,
          SCALE,
          RADIX,
          NULLABLE_SHORT,
          REMARKS_COLUMN,
          CHAR_OCTET_LENGTH_COLUMN,
          ORDINAL_POSITION_COLUMN,
          IS_NULLABLE_COLUMN,
          SPECIFIC_NAME_COLUMN);

  public static final List<ResultColumn> PSEUDO_COLUMNS_COLUMNS =
      List.of(
          CATALOG_COLUMN,
          SCHEMA_COLUMN,
          TABLE_NAME_COLUMN,
          COLUMN_NAME_COLUMN,
          DATA_TYPE_COLUMN,
          COLUMN_SIZE_COLUMN,
          DECIMAL_DIGITS_COLUMN,
          NUM_PREC_RADIX_COLUMN,
          COLUMN_USAGE,
          REMARKS_COLUMN,
          CHAR_OCTET_LENGTH_COLUMN,
          IS_NULLABLE_COLUMN);

  public static final List<ResultColumn> IMPORTED_KEYS_COLUMNS =
      List.of(
          PKTABLE_CAT,
          PKTABLE_SCHEM,
          PKTABLE_NAME,
          PKCOLUMN_NAME,
          FKTABLE_CAT,
          FKTABLE_SCHEM,
          FKTABLE_NAME,
          FKCOLUMN_NAME,
          KEY_SEQUENCE_COLUMN,
          UPDATE_RULE,
          DELETE_RULE,
          FK_NAME,
          PK_NAME,
          DEFERRABILITY);

  public static final List<ResultColumn> EXPORTED_KEYS_COLUMNS =
      List.of(
          PKTABLE_CAT,
          PKTABLE_SCHEM,
          PKTABLE_NAME,
          PKCOLUMN_NAME,
          FKTABLE_CAT,
          FKTABLE_SCHEM,
          FKTABLE_NAME,
          FKCOLUMN_NAME,
          KEY_SEQUENCE_COLUMN,
          UPDATE_RULE,
          DELETE_RULE,
          FK_NAME,
          PK_NAME,
          DEFERRABILITY);

  public static final Map<CommandName, List<ResultColumn>> NON_NULLABLE_COLUMNS_MAP =
      new HashMap<>() {
        {
          put(
              CommandName.LIST_TYPE_INFO,
              List.of(
                  MetadataResultConstants.TYPE_NAME_COLUMN,
                  MetadataResultConstants.DATA_TYPE_COLUMN,
                  MetadataResultConstants.PRECISION_COLUMN));
          put(CommandName.LIST_CATALOGS, List.of(MetadataResultConstants.CATALOG_RESULT_COLUMN));
          put(
              CommandName.LIST_TABLES,
              List.of(MetadataResultConstants.TABLE_NAME_COLUMN, TABLE_TYPE_COLUMN));
          put(
              CommandName.LIST_PRIMARY_KEYS,
              List.of(
                  MetadataResultConstants.TABLE_NAME_COLUMN,
                  MetadataResultConstants.COL_NAME_COLUMN,
                  MetadataResultConstants.KEY_SEQUENCE_COLUMN));
          put(
              CommandName.LIST_SCHEMAS,
              List.of(MetadataResultConstants.SCHEMA_COLUMN_FOR_GET_SCHEMA));
          put(CommandName.LIST_TABLE_TYPES, List.of(TABLE_TYPE_COLUMN));
          put(
              CommandName.LIST_COLUMNS,
              List.of(
                  MetadataResultConstants.TABLE_NAME_COLUMN,
                  MetadataResultConstants.COL_NAME_COLUMN,
                  MetadataResultConstants.DATA_TYPE_COLUMN,
                  MetadataResultConstants.COLUMN_TYPE_COLUMN,
                  MetadataResultConstants.NULLABLE_COLUMN,
                  MetadataResultConstants.SQL_DATA_TYPE_COLUMN,
                  MetadataResultConstants.ORDINAL_POSITION_COLUMN,
                  MetadataResultConstants.IS_NULLABLE_COLUMN));
          put(
              CommandName.LIST_FUNCTIONS,
              List.of(
                  MetadataResultConstants.FUNCTION_NAME_COLUMN,
                  MetadataResultConstants.SPECIFIC_NAME_COLUMN));
          put(
              CommandName.GET_COLUMN_PRIVILEGES,
              List.of(TABLE_NAME_COLUMN, COLUMN_NAME_COLUMN, GRANTEE, PRIVILEGE));
          put(
              CommandName.GET_BEST_ROW_IDENTIFIER,
              List.of(
                  MetadataResultConstants.SCOPE,
                  MetadataResultConstants.COL_NAME_COLUMN,
                  MetadataResultConstants.DATA_TYPE_COLUMN,
                  MetadataResultConstants.COLUMN_TYPE_COLUMN,
                  MetadataResultConstants.PSEUDO_COLUMN));
          put(
              CommandName.GET_CROSS_REFERENCE,
              List.of(
                  MetadataResultConstants.PKTABLE_NAME,
                  MetadataResultConstants.PKCOLUMN_NAME,
                  MetadataResultConstants.FKTABLE_NAME,
                  MetadataResultConstants.FKCOLUMN_NAME,
                  MetadataResultConstants.KEY_SEQUENCE_COLUMN,
                  MetadataResultConstants.DEFERRABILITY));
          put(
              CommandName.GET_ATTRIBUTES,
              List.of(
                  TYPE_NAME_COLUMN,
                  ATTR_NAME,
                  DATA_TYPE_COLUMN,
                  ATTR_TYPE_NAME,
                  NULLABLE_COLUMN,
                  SQL_DATA_TYPE_COLUMN,
                  ORDINAL_POSITION_COLUMN,
                  IS_NULLABLE_COLUMN));
          put(CommandName.GET_TABLE_PRIVILEGES, List.of(TABLE_NAME_COLUMN, GRANTEE, PRIVILEGE));
          put(
              CommandName.GET_VERSION_COLUMNS,
              List.of(SCOPE, COL_NAME_COLUMN, DATA_TYPE_COLUMN, TYPE_NAME_COLUMN, PSEUDO_COLUMN));
          put(CommandName.GET_SUPER_TYPES, List.of(TYPE_NAME_COLUMN, SUPERTYPE_NAME));
          put(
              CommandName.GET_PROCEDURES_COLUMNS,
              List.of(
                  PROCEDURE_NAME,
                  COLUMN_NAME_COLUMN,
                  COLUMN_TYPE,
                  DATA_TYPE_COLUMN,
                  TYPE_NAME_COLUMN,
                  NULLABLE_SHORT,
                  SQL_DATA_TYPE_COLUMN,
                  ORDINAL_POSITION_COLUMN,
                  IS_NULLABLE_COLUMN,
                  SPECIFIC_NAME_COLUMN));
          put(
              CommandName.GET_INDEX_INFO,
              List.of(
                  TABLE_NAME_COLUMN, NON_UNIQUE, TYPE, ORDINAL_POSITION_SHORT, CARDINALITY, PAGES));
          put(CommandName.GET_SUPER_TABLES, List.of(TABLE_NAME_COLUMN, SUPERTABLE_NAME));
          put(
              CommandName.GET_FUNCTION_COLUMNS,
              List.of(
                  FUNCTION_NAME_COLUMN,
                  COLUMN_NAME_COLUMN,
                  COLUMN_TYPE,
                  DATA_TYPE_COLUMN,
                  TYPE_NAME_COLUMN,
                  NULLABLE_SHORT,
                  ORDINAL_POSITION_COLUMN,
                  IS_NULLABLE_COLUMN,
                  SPECIFIC_NAME_COLUMN));
          put(
              CommandName.GET_PSEUDO_COLUMNS,
              List.of(
                  TABLE_NAME_COLUMN,
                  COLUMN_NAME_COLUMN,
                  DATA_TYPE_COLUMN,
                  COLUMN_USAGE,
                  IS_NULLABLE_COLUMN));
          put(
              CommandName.GET_IMPORTED_KEYS,
              List.of(
                  PKTABLE_NAME,
                  PKCOLUMN_NAME,
                  FKTABLE_NAME,
                  FKCOLUMN_NAME,
                  KEY_SEQUENCE_COLUMN,
                  DEFERRABILITY));
          put(
              CommandName.GET_EXPORTED_KEYS,
              List.of(
                  PKTABLE_NAME,
                  PKCOLUMN_NAME,
                  FKTABLE_NAME,
                  FKCOLUMN_NAME,
                  KEY_SEQUENCE_COLUMN,
                  DEFERRABILITY));
        }
      };
}
