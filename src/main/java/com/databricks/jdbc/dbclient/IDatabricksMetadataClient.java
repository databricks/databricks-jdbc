package com.databricks.jdbc.dbclient;

import com.databricks.jdbc.api.impl.DatabricksResultSet;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import java.sql.SQLException;

public interface IDatabricksMetadataClient {

  /** Returns information about types supported by Databricks server */
  DatabricksResultSet listTypeInfo(IDatabricksSession session) throws SQLException;

  /** Returns the list of catalogs */
  DatabricksResultSet listCatalogs(IDatabricksSession session) throws SQLException;

  /**
   * Returns the list of schemas
   *
   * @param session underlying session
   * @param catalog catalogName which must match to catalog in database
   * @param schemaNamePattern must match to schema name in database (can be a regex pattern or
   *     absolute name)
   * @return a DatabricksResultSet representing list of schemas
   */
  DatabricksResultSet listSchemas(
      IDatabricksSession session, String catalog, String schemaNamePattern) throws SQLException;

  /**
   * Returns the list of tables
   *
   * @param session underlying session
   * @param catalog catalogName which must match to catalog in database
   * @param schemaNamePattern must match to schema name in database (can be a regex pattern or
   *     absolute name)
   * @param tableNamePattern must match to table name in database (can be a regex pattern or
   *     absolute name)
   * @return a DatabricksResultSet representing list of tables
   */
  DatabricksResultSet listTables(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String tableNamePattern,
      String[] tableTypes)
      throws SQLException;

  /** Returns list of table types */
  DatabricksResultSet listTableTypes(IDatabricksSession session) throws SQLException;

  /**
   * Returns the list of columns
   *
   * @param session underlying session
   * @param catalog catalogName which must match to catalog in database
   * @param schemaNamePattern must match to schema name in database (can be a regex pattern or
   *     absolute name)
   * @param tableNamePattern must match to table name in database (can be a regex pattern or
   *     absolute name)
   * @param columnNamePattern must match to column name in database (can be a regex pattern or
   *     absolute name)
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
   * @param schemaNamePattern must match to schema name in database (can be a regex pattern or
   *     absolute name)
   * @param functionNamePattern must match to function name in database (can be a regex pattern or
   *     absolute name)
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

  /**
   * Returns the list of imported keys
   *
   * @param session underlying session
   * @param catalog catalogName which must match to catalog in database
   * @param schema must match to a schema in database
   * @param table must match to a table in database
   * @return a DatabricksResultSet representing list of imported keys
   */
  DatabricksResultSet listImportedKeys(
      IDatabricksSession session, String catalog, String schema, String table) throws SQLException;

  /**
   * Returns the list of imported keys
   *
   * @param session underlying session
   * @param catalog catalogName which must match to catalog in database
   * @param schema must match to a schema in database
   * @param table must match to a table in database
   * @return a DatabricksResultSet representing list of imported keys
   */
  DatabricksResultSet listExportedKeys(
      IDatabricksSession session, String catalog, String schema, String table) throws SQLException;

  /**
   * Returns the list of cross references between a parent table and a foreign table
   *
   * @param session underlying session
   * @param parentCatalog catalogName which must match to catalog in database
   * @param parentSchema must match to a schema in database
   * @param parentTable must match to a table in database
   * @param foreignCatalog catalogName which must match to foreign catalog in database
   * @param foreignSchema must match to a foreign schema in database
   * @param foreignTable must match to a foreign table in database
   * @return a DatabricksResultSet representing list of cross references
   */
  DatabricksResultSet listCrossReferences(
      IDatabricksSession session,
      String parentCatalog,
      String parentSchema,
      String parentTable,
      String foreignCatalog,
      String foreignSchema,
      String foreignTable)
      throws SQLException;
}
