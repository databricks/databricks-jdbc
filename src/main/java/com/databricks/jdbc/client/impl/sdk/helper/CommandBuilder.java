package com.databricks.jdbc.client.impl.sdk.helper;

import static com.databricks.jdbc.client.impl.sdk.helper.CommandConstants.*;
import static com.databricks.jdbc.commons.util.ValidationUtil.throwErrorIfNull;
import static com.databricks.jdbc.driver.DatabricksJdbcConstants.*;

import com.databricks.jdbc.commons.util.WildcardUtil;
import com.databricks.jdbc.core.DatabricksSQLFeatureNotSupportedException;
import com.databricks.jdbc.core.IDatabricksSession;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;

public class CommandBuilder {
  private String catalogName = null;
  private String schemaName = null;
  private String tableName = null;
  private String schemaPattern = null;
  private String tablePattern = null;
  private String columnPattern = null;
  private String functionNamePattern = null;

  private final String sessionContext;

  public CommandBuilder(String catalogName, IDatabricksSession session) throws SQLException {
    this.sessionContext = session.toString();
    throwErrorIfNull(
        Collections.singletonMap(CATALOG, catalogName), sessionContext); // Validating input
    this.catalogName = catalogName;
  }

  public CommandBuilder(IDatabricksSession session) {
    this.sessionContext = session.toString();
  }

  public CommandBuilder setSchema(String schemaName) {
    this.schemaName = schemaName;
    return this;
  }

  public CommandBuilder setTable(String tableName) {
    this.tableName = tableName;
    return this;
  }

  public CommandBuilder setSchemaPattern(String pattern) {
    this.schemaPattern = WildcardUtil.jdbcPatternToHive(pattern);
    return this;
  }

  public CommandBuilder setTablePattern(String pattern) {
    this.tablePattern = WildcardUtil.jdbcPatternToHive(pattern);
    return this;
  }

  public CommandBuilder setColumnPattern(String pattern) {
    this.columnPattern = WildcardUtil.jdbcPatternToHive(pattern);
    return this;
  }

  public CommandBuilder setFunctionPattern(String pattern) {
    this.functionNamePattern = WildcardUtil.jdbcPatternToHive(pattern);
    return this;
  }

  private String fetchCatalogSQL() {
    return SHOW_CATALOGS_SQL;
  }

  private String fetchSchemaSQL() throws SQLException {
    String showSchemaSQL = String.format(SHOW_SCHEMA_IN_CATALOG_SQL, catalogName);
    if (!WildcardUtil.isNullOrEmpty(schemaPattern)) {
      showSchemaSQL += String.format(LIKE_SQL, schemaPattern);
    }
    return showSchemaSQL;
  }

  private String fetchTablesSQL() throws SQLException {
    String showTablesSQL = String.format(SHOW_TABLES_SQL, catalogName);
    if (!WildcardUtil.isNullOrEmpty(schemaPattern)) {
      showTablesSQL += String.format(SCHEMA_LIKE_SQL, schemaPattern);
    }
    if (!WildcardUtil.isNullOrEmpty(tablePattern)) {
      showTablesSQL += String.format(LIKE_SQL, tablePattern);
    }
    return showTablesSQL;
  }

  private String fetchColumnsSQL() throws SQLException {
    String showColumnsSQL = String.format(SHOW_COLUMNS_SQL, catalogName);

    if (!WildcardUtil.isNullOrEmpty(schemaPattern)) {
      showColumnsSQL += String.format(SCHEMA_LIKE_SQL, schemaPattern);
    }

    if (!WildcardUtil.isNullOrEmpty(tablePattern)) {
      showColumnsSQL += String.format(TABLE_LIKE_SQL, tablePattern);
    }

    if (!WildcardUtil.isNullOrEmpty(columnPattern)) {
      showColumnsSQL += String.format(LIKE_SQL, columnPattern);
    }
    return showColumnsSQL;
  }

  private String fetchFunctionsSQL() throws SQLException {
    String showFunctionsSQL = String.format(SHOW_FUNCTIONS_SQL, catalogName);
    if (!WildcardUtil.isNullOrEmpty(schemaPattern)) {
      showFunctionsSQL += String.format(SCHEMA_LIKE_SQL, schemaPattern);
    }
    if (!WildcardUtil.isNullOrEmpty(functionNamePattern)) {
      showFunctionsSQL += String.format(LIKE_SQL, functionNamePattern);
    }
    return showFunctionsSQL;
  }

  private String fetchTableTypesSQL() {
    return SHOW_TABLE_TYPES_SQL;
  }

  private String fetchPrimaryKeysSQL() throws SQLException {
    HashMap<String, String> hashMap = new HashMap<>();
    hashMap.put(SCHEMA, schemaName);
    hashMap.put(TABLE, tableName);
    throwErrorIfNull(hashMap, sessionContext);
    return String.format(SHOW_PRIMARY_KEYS_SQL, catalogName, schemaName, tableName);
  }

  public String getSQLString(CommandName command) throws SQLException {
    switch (command) {
      case LIST_CATALOGS:
        return fetchCatalogSQL();
      case LIST_PRIMARY_KEYS:
        return fetchPrimaryKeysSQL();
      case LIST_SCHEMAS:
        return fetchSchemaSQL();
      case LIST_TABLE_TYPES:
        return fetchTableTypesSQL();
      case LIST_TABLES:
        return fetchTablesSQL();
      case LIST_FUNCTIONS:
        return fetchFunctionsSQL();
      case LIST_COLUMNS:
        return fetchColumnsSQL();
    }
    throw new DatabricksSQLFeatureNotSupportedException(
        String.format("Invalid command issued %s. Context: %s", command, sessionContext));
  }
}
