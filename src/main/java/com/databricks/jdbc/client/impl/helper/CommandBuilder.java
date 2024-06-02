package com.databricks.jdbc.client.impl.helper;

import static com.databricks.jdbc.client.impl.helper.CommandConstants.*;
import static com.databricks.jdbc.commons.util.ValidationUtil.throwErrorIfNull;
import static com.databricks.jdbc.driver.DatabricksJdbcConstants.*;

import com.databricks.jdbc.commons.util.WildcardUtil;
import com.databricks.jdbc.core.DatabricksSQLFeatureNotSupportedException;
import com.databricks.jdbc.core.IDatabricksSession;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CommandBuilder {
  private static final Logger LOGGER = LogManager.getLogger(CommandBuilder.class);
  private String catalogName = null;
  private String schemaName = null;
  private String tableName = null;
  private String schemaPattern = null;
  private String tablePattern = null;
  private String columnPattern = null;
  private String functionPattern = null;

  private final String sessionContext;

  public CommandBuilder(String catalogName, IDatabricksSession session) throws SQLException {
    this.sessionContext = session.toString();
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
    LOGGER.debug("Schema pattern conversion {} -> {}", pattern, schemaPattern);
    return this;
  }

  public CommandBuilder setTablePattern(String pattern) {
    this.tablePattern = WildcardUtil.jdbcPatternToHive(pattern);
    LOGGER.debug("Table pattern conversion {} -> {}", pattern, tablePattern);
    return this;
  }

  public CommandBuilder setColumnPattern(String pattern) {
    this.columnPattern = WildcardUtil.jdbcPatternToHive(pattern);
    LOGGER.debug("Column pattern conversion {} -> {}", pattern, columnPattern);
    return this;
  }

  public CommandBuilder setFunctionPattern(String pattern) {
    this.functionPattern = WildcardUtil.jdbcPatternToHive(pattern);
    LOGGER.debug("Function pattern conversion {} -> {}", pattern, functionPattern);
    return this;
  }

  private String fetchCatalogSQL() {
    return SHOW_CATALOGS_SQL;
  }

  private String fetchSchemaSQL() throws SQLException {
    String contextString =
        String.format(
            "Building command for fetching schema. Catalog %s, SchemaPattern %s and session context %s",
            catalogName, schemaPattern, sessionContext);
    LOGGER.debug(contextString);
    throwErrorIfNull(Collections.singletonMap(CATALOG, catalogName), contextString);
    String showSchemaSQL = String.format(SHOW_SCHEMA_IN_CATALOG_SQL, catalogName);
    if (!WildcardUtil.isNullOrEmpty(schemaPattern)) {
      showSchemaSQL += String.format(LIKE_SQL, schemaPattern);
    }
    return showSchemaSQL;
  }

  private String fetchTablesSQL() throws SQLException {
    String contextString =
        String.format(
            "Building command for fetching tables. Catalog %s, SchemaPattern %s, TablePattern %s and session context %s",
            catalogName, schemaPattern, tablePattern, sessionContext);
    LOGGER.debug(contextString);
    throwErrorIfNull(Collections.singletonMap(CATALOG, catalogName), contextString);
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
    String contextString =
        String.format(
            "Building command for fetching columns. Catalog %s, SchemaPattern %s, TablePattern %s, ColumnPattern %s and session context : %s",
            catalogName, schemaPattern, tablePattern, columnPattern, sessionContext);
    LOGGER.debug(contextString);
    throwErrorIfNull(Collections.singletonMap(CATALOG, catalogName), contextString);
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
    String contextString =
        String.format(
            "Building command for fetching functions. Catalog %s, SchemaPattern %s, FunctionPattern %s. With session context %s",
            catalogName, schemaPattern, functionPattern, sessionContext);

    LOGGER.debug(contextString);
    throwErrorIfNull(Collections.singletonMap(CATALOG, catalogName), contextString);
    String showFunctionsSQL = String.format(SHOW_FUNCTIONS_SQL, catalogName);
    if (!WildcardUtil.isNullOrEmpty(schemaPattern)) {
      showFunctionsSQL += String.format(SCHEMA_LIKE_SQL, schemaPattern);
    }
    if (!WildcardUtil.isNullOrEmpty(functionPattern)) {
      showFunctionsSQL += String.format(LIKE_SQL, functionPattern);
    }
    return showFunctionsSQL;
  }

  private String fetchTableTypesSQL() {
    return SHOW_TABLE_TYPES_SQL;
  }

  private String fetchPrimaryKeysSQL() throws SQLException {
    String contextString =
        String.format(
            "Building command for fetching primary keys. Catalog %s, Schema %s, Table %s. With session context: %s",
            catalogName, schemaName, tableName, sessionContext);
    LOGGER.debug(contextString);
    HashMap<String, String> hashMap = new HashMap<>();
    hashMap.put(CATALOG, catalogName);
    hashMap.put(SCHEMA, schemaName);
    hashMap.put(TABLE, tableName);
    throwErrorIfNull(hashMap, contextString);
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
