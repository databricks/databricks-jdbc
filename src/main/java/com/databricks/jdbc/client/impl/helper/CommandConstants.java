package com.databricks.jdbc.client.impl.helper;

public class CommandConstants {
  public static final String METADATA_STATEMENT_ID = "metadata-statement";
  public static final String GET_TABLES_STATEMENT_ID = "gettables-metadata";
  public static final String GET_CATALOGS_STATEMENT_ID = "getcatalogs-metadata";
  public static final String GET_TYPE_INFO_STATEMENT_ID = "typeinfo-metadata";
  public static final String GET_TABLE_TYPE_STATEMENT_ID = "gettabletype-metadata";
  public static final String GET_FUNCTIONS_STATEMENT_ID = "getfunctions-metadata";
  public static final String SHOW_CATALOGS_SQL = "SHOW CATALOGS";
  public static final String SHOW_TABLE_TYPES_SQL = "SHOW TABLE_TYPES";
  public static final String IN_CATALOG_SQL = " IN CATALOG %s";
  public static final String IN_ABSOLUTE_SCHEMA_SQL = " IN SCHEMA %s";
  public static final String IN_ABSOLUTE_TABLE_SQL = " IN TABLE %s";
  public static final String SHOW_SCHEMA_IN_CATALOG_SQL = "SHOW SCHEMAS IN %s";
  public static final String LIKE_SQL = " LIKE '%s'";
  public static final String SCHEMA_LIKE_SQL = " SCHEMA" + LIKE_SQL;
  public static final String TABLE_LIKE_SQL = " TABLE" + LIKE_SQL;
  public static final String SHOW_TABLES_SQL = "SHOW TABLES" + IN_CATALOG_SQL;
  public static final String SHOW_COLUMNS_SQL = "SHOW COLUMNS" + IN_CATALOG_SQL;
  public static final String SHOW_FUNCTIONS_SQL = "SHOW FUNCTIONS" + IN_CATALOG_SQL;
  public static final String SHOW_PRIMARY_KEYS_SQL =
      "SHOW KEYS" + IN_CATALOG_SQL + IN_ABSOLUTE_SCHEMA_SQL + IN_ABSOLUTE_TABLE_SQL;
}
