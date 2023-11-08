package com.databricks.jdbc.client;

import com.databricks.jdbc.core.DatabricksResultSet;
import com.databricks.jdbc.core.IDatabricksSession;
import java.sql.SQLException;

public interface DatabricksMetadataClient {

  /** Returns information about types supported by Databricks server */
  DatabricksResultSet listTypeInfo(IDatabricksSession session);

  /** Returns the list of catalogs */
  DatabricksResultSet listCatalogs(IDatabricksSession session) throws SQLException;

  /**
   * Returns the list of schemas
   *
   * @param session underlying session
   * @param catalog catalogName which must match to catalog in database
   * @param schemaNamePattern must match to schema name in database (can be a regex pattern or absolute name)
   * @return a DatabricksResultSet representing list of schemas
   */
  DatabricksResultSet listSchemas(
      IDatabricksSession session, String catalog, String schemaNamePattern) throws SQLException;

  /**
   * Returns the list of tables
   *
   * @param session underlying session
   * @param catalog catalogName which must match to catalog in database
   * @param schemaNamePattern must match to schema name in database (can be a regex pattern or absolute name)
   * @param tableNamePattern must match to table name in database (can be a regex pattern or absolute name)
   * @return a DatabricksResultSet representing list of tables
   */
  DatabricksResultSet listTables(
      IDatabricksSession session, String catalog, String schemaNamePattern, String tableNamePattern)
      throws SQLException;

  /** Returns list of table types */
  DatabricksResultSet listTableTypes(IDatabricksSession session);

  /**
   * Returns the list of columns
   *
   * @param session underlying session
   * @param catalog catalogName which must match to catalog in database
   * @param schemaNamePattern must match to schema name in database (can be a regex pattern or absolute name)
   * @param tableNamePattern must match to table name in database (can be a regex pattern or absolute name)
   * @param columnNamePattern must match to column name in database (can be a regex pattern or absolute name)
   * @return a DatabricksResultSet representing list of columns
   */
  DatabricksResultSet listColumns(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String tableNamePattern,
      String columnNamePattern)
      throws SQLException;

  /**
   * Returns the list of functions
   *
   * @param session underlying session
   * @param catalog catalogName which must match to catalog in database
   * @param schemaNamePattern must match to schema name in database (can be a regex pattern or absolute name)
   * @param functionNamePattern must match to function name in database (can be a regex pattern or absolute name)
   * @return a DatabricksResultSet representing list of functions
   */
  DatabricksResultSet listFunctions(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String functionNamePattern)
      throws SQLException;

  /**
   * Returns the list of primary keys
   *
   * @param session underlying session
   * @param catalog catalogName which must match to catalog in database
   * @param schema must match to a schema in database
   * @param table must match to a table in database
   * @return a DatabricksResultSet representing list of functions
   */
  DatabricksResultSet listPrimaryKeys(
      IDatabricksSession session, String catalog, String schema, String table) throws SQLException;
}
