package com.databricks.jdbc.client.impl.sdk.helper;

import static com.databricks.jdbc.client.impl.sdk.helper.CommandConstants.*;
import static com.databricks.jdbc.commons.util.ValidationUtil.throwErrorIfEmptyOrWildcard;
import static com.databricks.jdbc.driver.DatabricksJdbcConstants.*;

import com.databricks.jdbc.commons.util.WildcardUtil;
import com.databricks.jdbc.core.DatabricksSQLFeatureNotSupportedException;
import com.databricks.jdbc.core.IDatabricksSession;
import java.sql.SQLException;
import java.util.Map;

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
    throwErrorIfEmptyOrWildcard(Map.of(CATALOG, catalogName), sessionContext); // Validating input
    this.catalogName = catalogName;
  }

  public CommandBuilder(IDatabricksSession session) throws SQLException {
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
    // TODO : add pattern conversion
    this.schemaPattern = pattern;
    return this;
  }

  public CommandBuilder setTablePattern(String pattern) {
    // TODO : add pattern conversion
    this.tablePattern = pattern;
    return this;
  }

  public CommandBuilder setColumnPattern(String pattern) {
    // TODO : add pattern conversion
    this.columnPattern = pattern;
    return this;
  }

  public CommandBuilder setFunctionPattern(String pattern) {
    // TODO : add pattern conversion
    this.functionNamePattern = pattern;
    return this;
  }

  private String fetchCatalogSQL() {
    return SHOW_CATALOGS_SQL;
  }

  private String fetchSchemaSQL() throws SQLException {
    if (this.catalogName == null) {
      throw new DatabricksSQLFeatureNotSupportedException(
          String.format(
              "Catalog name has to be set for fetching schemas. Context: %s", sessionContext));
    }
    String showSchemaSQL = String.format(SHOW_SCHEMA_IN_CATALOG_SQL, catalogName);
    if (!WildcardUtil.isNullOrEmpty(schemaPattern)) {
      showSchemaSQL += String.format(LIKE_SQL, schemaPattern);
    }
    return showSchemaSQL;
  }

  private String fetchTablesSQL() throws SQLException {
    if (this.catalogName == null) {
      throw new DatabricksSQLFeatureNotSupportedException(
          String.format(
              "Catalog name has to be set for fetching tables. Context: %s", sessionContext));
    }
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
    if (this.catalogName == null) {
      throw new DatabricksSQLFeatureNotSupportedException(
          String.format(
              "Catalog name has to be set for fetching columns. Context: %s", sessionContext));
    }
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
    if (this.catalogName == null) {
      throw new DatabricksSQLFeatureNotSupportedException(
          String.format(
              "Catalog name has to be set for fetching columns. Context: %s", sessionContext));
    }
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

  public String getSQLString(CommandName command) throws SQLException {
    switch (command) {
      case LIST_CATALOGS:
        return fetchCatalogSQL();
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
