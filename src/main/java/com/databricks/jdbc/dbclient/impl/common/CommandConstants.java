package com.databricks.jdbc.dbclient.impl.common;

import com.databricks.jdbc.api.impl.ImmutableSqlParameter;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import java.util.Map;

public class CommandConstants {
  public static final String METADATA_STATEMENT_ID = "metadata-statement";
  public static final String GET_TABLES_STATEMENT_ID = "gettables-metadata";
  public static final String GET_CATALOGS_STATEMENT_ID = "getcatalogs-metadata";
  public static final String GET_TABLE_TYPE_STATEMENT_ID = "gettabletype-metadata";
  public static final String GET_FUNCTIONS_STATEMENT_ID = "getfunctions-metadata";
  public static final String GET_PROCEDURES_STATEMENT_ID = "getprocedures-metadata";
  public static final String GET_PROCEDURE_COLUMNS_STATEMENT_ID = "getprocedurecolumns-metadata";
  public static final String SHOW_CATALOGS_SQL = "SHOW CATALOGS";
  public static final String SHOW_TABLE_TYPES_SQL = "SHOW TABLE_TYPES";
  public static final String IN_CATALOG_SQL = " IN CATALOG `%s`";
  public static final String IN_ABSOLUTE_SCHEMA_SQL = " IN SCHEMA `%s`";
  public static final String IN_ABSOLUTE_TABLE_SQL = " IN TABLE `%s`";
  public static final String IN_ALL_CATALOGS_SQL = " IN ALL CATALOGS";
  public static final String SHOW_SCHEMAS_IN_CATALOG_SQL = "SHOW SCHEMAS IN `%s`";
  public static final String LIKE_SQL = " LIKE '%s'";
  public static final String SCHEMA_LIKE_SQL = " SCHEMA" + LIKE_SQL;
  public static final String TABLE_LIKE_SQL = " TABLE" + LIKE_SQL;
  public static final String SHOW_TABLES_SQL = "SHOW TABLES" + IN_CATALOG_SQL;
  public static final String SHOW_TABLES_IN_ALL_CATALOGS_SQL = "SHOW TABLES" + IN_ALL_CATALOGS_SQL;
  public static final String SHOW_COLUMNS_SQL = "SHOW COLUMNS" + IN_CATALOG_SQL;
  public static final String SHOW_FUNCTIONS_SQL = "SHOW FUNCTIONS" + IN_CATALOG_SQL;
  public static final String SHOW_SCHEMAS_IN_ALL_CATALOGS_SQL =
      "SHOW SCHEMAS" + IN_ALL_CATALOGS_SQL;
  public static final String SHOW_PRIMARY_KEYS_SQL =
      "SHOW KEYS" + IN_CATALOG_SQL + IN_ABSOLUTE_SCHEMA_SQL + IN_ABSOLUTE_TABLE_SQL;
  public static final String SHOW_FOREIGN_KEYS_SQL =
      "SHOW FOREIGN KEYS" + IN_CATALOG_SQL + IN_ABSOLUTE_SCHEMA_SQL + IN_ABSOLUTE_TABLE_SQL;

  private static final String INFORMATION_SCHEMA_ROUTINES = "information_schema.routines";
  private static final String INFORMATION_SCHEMA_PARAMETERS = "information_schema.parameters";
  private static final String PROCEDURE_TYPE_FILTER = "routine_type = 'PROCEDURE'";

  private static final String ROUTINES_SELECT_COLUMNS =
      "routine_catalog, routine_schema, routine_name, comment, specific_name";

  private static final String PARAMETERS_SELECT_COLUMNS =
      "p.specific_catalog, p.specific_schema, p.specific_name,"
          + " p.parameter_name, p.parameter_mode, p.is_result,"
          + " p.data_type,"
          + " p.numeric_precision, p.numeric_precision_radix, p.numeric_scale,"
          + " p.character_maximum_length, p.character_octet_length,"
          + " p.ordinal_position, p.parameter_default, p.comment";

  /**
   * Builds a parameterized SQL query to fetch procedures from information_schema.routines. LIKE
   * clause values use ? placeholders with parameters populated in the provided map for server-side
   * binding.
   */
  public static String buildProceduresSQL(
      String catalog,
      String schemaPattern,
      String procedureNamePattern,
      Map<Integer, ImmutableSqlParameter> params) {
    String catalogPrefix = getCatalogPrefix(catalog);
    String routinesTable = catalogPrefix + "." + INFORMATION_SCHEMA_ROUTINES;
    int paramIndex = 1;

    StringBuilder sql = new StringBuilder();
    sql.append("SELECT ").append(ROUTINES_SELECT_COLUMNS);
    sql.append(" FROM ").append(routinesTable);
    sql.append(" WHERE ").append(PROCEDURE_TYPE_FILTER);
    if (schemaPattern != null) {
      sql.append(" AND routine_schema LIKE ?");
      params.put(paramIndex, buildStringParam(paramIndex, schemaPattern));
      paramIndex++;
    }
    if (procedureNamePattern != null) {
      sql.append(" AND routine_name LIKE ?");
      params.put(paramIndex, buildStringParam(paramIndex, procedureNamePattern));
      paramIndex++;
    }
    sql.append(" ORDER BY routine_catalog, routine_schema, routine_name");
    return sql.toString();
  }

  /**
   * Builds a parameterized SQL query to fetch procedure columns from information_schema.parameters.
   * LIKE clause values use ? placeholders with parameters populated in the provided map for
   * server-side binding.
   */
  public static String buildProcedureColumnsSQL(
      String catalog,
      String schemaPattern,
      String procedureNamePattern,
      String columnNamePattern,
      Map<Integer, ImmutableSqlParameter> params) {
    String catalogPrefix = getCatalogPrefix(catalog);
    String parametersTable = catalogPrefix + "." + INFORMATION_SCHEMA_PARAMETERS + " p";
    String routinesTable = catalogPrefix + "." + INFORMATION_SCHEMA_ROUTINES + " r";
    int paramIndex = 1;

    StringBuilder sql = new StringBuilder();
    sql.append("SELECT ").append(PARAMETERS_SELECT_COLUMNS);
    sql.append(" FROM ").append(parametersTable);
    sql.append(" JOIN ").append(routinesTable);
    sql.append(" ON p.specific_catalog = r.specific_catalog");
    sql.append(" AND p.specific_schema = r.specific_schema");
    sql.append(" AND p.specific_name = r.specific_name");
    sql.append(" WHERE r.").append(PROCEDURE_TYPE_FILTER);
    if (schemaPattern != null) {
      sql.append(" AND p.specific_schema LIKE ?");
      params.put(paramIndex, buildStringParam(paramIndex, schemaPattern));
      paramIndex++;
    }
    if (procedureNamePattern != null) {
      sql.append(" AND p.specific_name LIKE ?");
      params.put(paramIndex, buildStringParam(paramIndex, procedureNamePattern));
      paramIndex++;
    }
    if (columnNamePattern != null) {
      sql.append(" AND p.parameter_name LIKE ?");
      params.put(paramIndex, buildStringParam(paramIndex, columnNamePattern));
      paramIndex++;
    }
    sql.append(
        " ORDER BY p.specific_catalog, p.specific_schema, p.specific_name, p.ordinal_position");
    return sql.toString();
  }

  private static ImmutableSqlParameter buildStringParam(int index, String value) {
    return ImmutableSqlParameter.builder()
        .type(ColumnInfoTypeName.STRING)
        .value(value)
        .cardinal(index)
        .build();
  }

  private static String getCatalogPrefix(String catalog) {
    return (catalog == null) ? "system" : "`" + catalog + "`";
  }
}
