package com.databricks.jdbc.dbclient.impl.sqlexec;

import static com.databricks.jdbc.common.MetadataResultConstants.*;
import static com.databricks.jdbc.dbclient.impl.common.CommandConstants.GET_TABLES_STATEMENT_ID;
import static com.databricks.jdbc.dbclient.impl.common.CommandConstants.METADATA_STATEMENT_ID;
import static com.databricks.jdbc.dbclient.impl.sqlexec.ResultConstants.TYPE_INFO_RESULT;

import com.databricks.jdbc.api.impl.DatabricksResultSet;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.MetadataResultConstants;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.util.JdbcThreadUtils;
import com.databricks.jdbc.common.util.WildcardUtil;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.IDatabricksMetadataClient;
import com.databricks.jdbc.dbclient.impl.common.MetadataResultSetBuilder;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Implementation for {@link IDatabricksMetadataClient} using {@link IDatabricksClient}. */
public class DatabricksMetadataSdkClient implements IDatabricksMetadataClient {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksMetadataSdkClient.class);
  private static final int DEFAULT_MAX_THREADS_FETCH_SCHEMAS = 10;
  private static final int TASK_TIMEOUT_FETCH_SCHEMAS_SEC = 90;
  private static final Object THREAD_POOL_LOCK = new Object();
  private static ExecutorService schemasThreadPool = null;
  private final IDatabricksClient sdkClient;
  private final MetadataResultSetBuilder metadataResultSetBuilder;

  public DatabricksMetadataSdkClient(IDatabricksClient sdkClient) {
    this.sdkClient = sdkClient;
    this.metadataResultSetBuilder = new MetadataResultSetBuilder(sdkClient.getConnectionContext());
  }

  @Override
  public DatabricksResultSet listTypeInfo(IDatabricksSession session) {
    LOGGER.debug("public ResultSet getTypeInfo()");
    return TYPE_INFO_RESULT;
  }

  @Override
  public DatabricksResultSet listCatalogs(IDatabricksSession session) throws SQLException {
    // If multiple catalog support is disabled, return only the current catalog
    if (isMultipleCatalogSupportDisabled()) {
      String currentCatalog = session.getCurrentCatalog();
      if (currentCatalog == null) {
        currentCatalog = "";
      }
      String SQL = String.format("SELECT '%s' AS catalog", currentCatalog);
      LOGGER.debug("SQL command to fetch catalogs: {}", SQL);
      return metadataResultSetBuilder.getCatalogsResult(getResultSet(SQL, session));
    }

    CommandBuilder commandBuilder = new CommandBuilder(session);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_CATALOGS);
    LOGGER.debug("SQL command to fetch catalogs: {}", SQL);
    return metadataResultSetBuilder.getCatalogsResult(getResultSet(SQL, session));
  }

  @Override
  public DatabricksResultSet listSchemas(
      IDatabricksSession session, String catalog, String schemaNamePattern) throws SQLException {
    catalog = autoFillCatalog(catalog, session);
    CommandBuilder commandBuilder =
        new CommandBuilder(catalog, session).setSchemaPattern(schemaNamePattern);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_SCHEMAS);
    LOGGER.debug("SQL command to fetch schemas: {}", SQL);
    try {
      return metadataResultSetBuilder.getSchemasResult(getResultSet(SQL, session), catalog);
    } catch (SQLException e) {
      if (WildcardUtil.isNullOrWildcard(catalog)
          && PARSE_SYNTAX_ERROR_SQL_STATE.equals(e.getSQLState())) {
        // This is a fallback for the case where the SQL command fails with "syntax error at or near
        // "ALL CATALOGS""
        // This is a known issue for older DBR versions
        LOGGER.debug("SQL command failed with syntax error. Fetching schemas across all catalogs.");
        return fetchSchemasAcrossCatalogs(session, schemaNamePattern);
      } else {
        throw e;
      }
    }
  }

  @Override
  public DatabricksResultSet listTables(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String tableNamePattern,
      String[] tableTypes)
      throws SQLException {
    catalog = autoFillCatalog(catalog, session);
    String[] validatedTableTypes =
        Optional.ofNullable(tableTypes)
            .filter(types -> types.length > 0)
            .orElse(DEFAULT_TABLE_TYPES);
    CommandBuilder commandBuilder =
        new CommandBuilder(catalog, session)
            .setSchemaPattern(schemaNamePattern)
            .setTablePattern(tableNamePattern);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_TABLES);
    LOGGER.debug("SQL command to fetch tables: {}", SQL);
    LOGGER.debug(String.format("SQL command to fetch tables: {%s}", SQL));
    try {
      return metadataResultSetBuilder.getTablesResult(
          getResultSet(SQL, session), validatedTableTypes);
    } catch (SQLException e) {
      if (PARSE_SYNTAX_ERROR_SQL_STATE.equals(e.getSQLState())
          && (catalog == null || catalog.equals("*") || catalog.equals("%"))) {
        // Gracefully handles the case where an older DBSQL version doesn't support all catalogs in
        // the SHOW TABLES command.
        LOGGER.debug("SQL command failed with syntax error. Returning empty result set.");
        return metadataResultSetBuilder.getResultSetWithGivenRowsAndColumns(
            MetadataResultConstants.TABLE_COLUMNS,
            new ArrayList<>(),
            GET_TABLES_STATEMENT_ID,
            com.databricks.jdbc.common.CommandName.LIST_TABLES);
      } else {
        throw e;
      }
    }
  }

  @Override
  public DatabricksResultSet listTableTypes(IDatabricksSession session) throws SQLException {
    LOGGER.debug("Returning list of table types.");
    return metadataResultSetBuilder.getTableTypesResult();
  }

  @Override
  public DatabricksResultSet listColumns(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String tableNamePattern,
      String columnNamePattern)
      throws SQLException {
    catalog = autoFillCatalog(catalog, session);

    // Return empty result set if catalog is null
    if (catalog == null) {
      LOGGER.debug("Catalog is null, returning empty result set for listColumns");
      return metadataResultSetBuilder.getResultSetWithGivenRowsAndColumns(
          MetadataResultConstants.COLUMN_COLUMNS,
          new ArrayList<>(),
          METADATA_STATEMENT_ID,
          com.databricks.jdbc.common.CommandName.LIST_COLUMNS);
    }

    CommandBuilder commandBuilder =
        new CommandBuilder(catalog, session)
            .setSchemaPattern(schemaNamePattern)
            .setTablePattern(tableNamePattern)
            .setColumnPattern(columnNamePattern);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_COLUMNS);
    LOGGER.debug("SQL command to fetch columns: {}", SQL);
    return metadataResultSetBuilder.getColumnsResult(getResultSet(SQL, session));
  }

  @Override
  public DatabricksResultSet listFunctions(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String functionNamePattern)
      throws SQLException {
    catalog = autoFillCatalog(catalog, session);

    // Return empty result set if catalog is null
    if (catalog == null) {
      LOGGER.debug("Catalog is null, returning empty result set for listFunctions");
      return metadataResultSetBuilder.getResultSetWithGivenRowsAndColumns(
          MetadataResultConstants.FUNCTION_COLUMNS,
          new ArrayList<>(),
          METADATA_STATEMENT_ID,
          com.databricks.jdbc.common.CommandName.LIST_FUNCTIONS);
    }

    CommandBuilder commandBuilder =
        new CommandBuilder(catalog, session)
            .setSchemaPattern(schemaNamePattern)
            .setFunctionPattern(functionNamePattern);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_FUNCTIONS);
    LOGGER.debug("SQL command to fetch functions: {}", SQL);
    return metadataResultSetBuilder.getFunctionsResult(getResultSet(SQL, session), catalog);
  }

  @Override
  public DatabricksResultSet listPrimaryKeys(
      IDatabricksSession session, String catalog, String schema, String table) throws SQLException {
    catalog = autoFillCatalog(catalog, session);

    // Return empty result set if catalog, schema, or table is null
    if (catalog == null || schema == null || table == null) {
      LOGGER.debug(
          "Catalog, schema, or table is null (catalog={}, schema={}, table={}), returning empty result set for listPrimaryKeys",
          catalog,
          schema,
          table);
      return metadataResultSetBuilder.getResultSetWithGivenRowsAndColumns(
          MetadataResultConstants.PRIMARY_KEYS_COLUMNS,
          new ArrayList<>(),
          METADATA_STATEMENT_ID,
          com.databricks.jdbc.common.CommandName.LIST_PRIMARY_KEYS);
    }

    CommandBuilder commandBuilder =
        new CommandBuilder(catalog, session).setSchema(schema).setTable(table);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_PRIMARY_KEYS);
    LOGGER.debug("SQL command to fetch primary keys: {}", SQL);
    return metadataResultSetBuilder.getPrimaryKeysResult(getResultSet(SQL, session));
  }

  @Override
  public DatabricksResultSet listImportedKeys(
      IDatabricksSession session, String catalog, String schema, String table) throws SQLException {
    LOGGER.debug("public ResultSet listImportedKeys() using SDK");
    catalog = autoFillCatalog(catalog, session);

    // Return empty result set if catalog, schema, or table is null
    if (catalog == null || schema == null || table == null) {
      LOGGER.debug(
          "Catalog, schema, or table is null (catalog={}, schema={}, table={}), returning empty result set for listImportedKeys",
          catalog,
          schema,
          table);
      return metadataResultSetBuilder.getResultSetWithGivenRowsAndColumns(
          MetadataResultConstants.IMPORTED_KEYS_COLUMNS,
          new ArrayList<>(),
          METADATA_STATEMENT_ID,
          com.databricks.jdbc.common.CommandName.GET_IMPORTED_KEYS);
    }

    CommandBuilder commandBuilder =
        new CommandBuilder(catalog, session).setSchema(schema).setTable(table);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_FOREIGN_KEYS);
    try {
      return metadataResultSetBuilder.getImportedKeysResult(getResultSet(SQL, session));
    } catch (SQLException e) {
      if (PARSE_SYNTAX_ERROR_SQL_STATE.equals(e.getSQLState())) {
        // This is a workaround for the issue where the SQL command fails with "syntax error at or
        // near "foreign""
        LOGGER.debug("SQL command failed with syntax error. Returning empty result set.");
        return metadataResultSetBuilder.getResultSetWithGivenRowsAndColumns(
            MetadataResultConstants.IMPORTED_KEYS_COLUMNS,
            new ArrayList<>(),
            METADATA_STATEMENT_ID,
            com.databricks.jdbc.common.CommandName.GET_IMPORTED_KEYS);
      } else {
        throw e;
      }
    }
  }

  @Override
  public DatabricksResultSet listExportedKeys(
      IDatabricksSession session, String catalog, String schema, String table) throws SQLException {
    LOGGER.debug("public ResultSet listExportedKeys() using SDK");
    catalog = autoFillCatalog(catalog, session);
    // Exported keys not tracked in DBSQL. Returning empty result set
    return metadataResultSetBuilder.getResultSetWithGivenRowsAndColumns(
        MetadataResultConstants.EXPORTED_KEYS_COLUMNS,
        new ArrayList<>(),
        METADATA_STATEMENT_ID,
        com.databricks.jdbc.common.CommandName.GET_EXPORTED_KEYS);
  }

  @Override
  public DatabricksResultSet listCrossReferences(
      IDatabricksSession session,
      String parentCatalog,
      String parentSchema,
      String parentTable,
      String foreignCatalog,
      String foreignSchema,
      String foreignTable)
      throws SQLException {
    LOGGER.debug("public ResultSet listCrossReferences() using SDK");
    CommandBuilder commandBuilder =
        new CommandBuilder(foreignCatalog, session).setSchema(foreignSchema).setTable(foreignTable);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_FOREIGN_KEYS);
    try {
      return metadataResultSetBuilder.getCrossReferenceKeysResult(
          getResultSet(SQL, session), parentCatalog, parentSchema, parentTable);
    } catch (SQLException e) {
      if (PARSE_SYNTAX_ERROR_SQL_STATE.equals(e.getSQLState())) {
        // This is a workaround for the issue where the SQL command fails with "syntax error at or
        // near "foreign""
        // This is a known issue in Databricks for older DBSQL versions
        LOGGER.debug("SQL command failed with syntax error. Returning empty result set.");
        return metadataResultSetBuilder.getResultSetWithGivenRowsAndColumns(
            MetadataResultConstants.CROSS_REFERENCE_COLUMNS,
            new ArrayList<>(),
            METADATA_STATEMENT_ID,
            com.databricks.jdbc.common.CommandName.GET_CROSS_REFERENCE);
      } else {
        LOGGER.error(
            e,
            "Error while executing SQL command: %s, SQL state: %s",
            e.getMessage(),
            e.getSQLState());
        throw e;
      }
    }
  }

  private boolean isMultipleCatalogSupportDisabled() {
    return sdkClient.getConnectionContext() != null
        && !sdkClient.getConnectionContext().getEnableMultipleCatalogSupport();
  }

  private String autoFillCatalog(String catalog, IDatabricksSession session) throws SQLException {
    if (isMultipleCatalogSupportDisabled()) {
      String currentCatalog = session.getCurrentCatalog();
      return (currentCatalog != null && !currentCatalog.isEmpty()) ? currentCatalog : "";
    }
    return catalog;
  }

  private DatabricksResultSet getResultSet(String SQL, IDatabricksSession session)
      throws SQLException {
    return sdkClient.executeStatement(
        SQL,
        session.getComputeResource(),
        new HashMap<>(),
        StatementType.METADATA,
        session,
        null /* parentStatement */);
  }

  private DatabricksResultSet fetchSchemasAcrossCatalogs(
      IDatabricksSession session, String schemaPattern) throws SQLException {
    List<String> catalogList = new ArrayList<>();
    try (ResultSet catalogs = session.getDatabricksMetadataClient().listCatalogs(session)) {
      while (catalogs.next()) {
        String c = catalogs.getString(1);
        if (c != null && !c.isEmpty()) {
          catalogList.add(c);
        }
      }
    }

    // Process catalogs in parallel, gathering schema information
    List<List<Object>> schemaRows =
        JdbcThreadUtils.parallelFlatMap(
            catalogList,
            session.getConnectionContext(),
            DEFAULT_MAX_THREADS_FETCH_SCHEMAS, // Not significant since the executor is provided as
            // a parameter
            TASK_TIMEOUT_FETCH_SCHEMAS_SEC,
            c -> {
              List<List<Object>> rows = new ArrayList<>();
              try (ResultSet catalogSchemas =
                  session.getDatabricksMetadataClient().listSchemas(session, c, schemaPattern)) {
                while (catalogSchemas.next()) {
                  List<Object> schemaRow = new ArrayList<>();
                  schemaRow.add(catalogSchemas.getString(1)); // TABLE_SCHEM
                  schemaRow.add(catalogSchemas.getString(2)); // TABLE_CATALOG
                  rows.add(schemaRow);
                }
              } catch (SQLException e) {
                LOGGER.warn("Error fetching schemas for catalog {} {}", c, e.getMessage());
              }
              return rows;
            },
            getOrCreateSchemasThreadPool());

    // Convert combined data into a result set
    return metadataResultSetBuilder.getResultSetWithGivenRowsAndColumns(
        SCHEMA_COLUMNS,
        schemaRows,
        METADATA_STATEMENT_ID,
        com.databricks.jdbc.common.CommandName.LIST_SCHEMAS);
  }

  public static ExecutorService getOrCreateSchemasThreadPool() {
    synchronized (THREAD_POOL_LOCK) {
      if (schemasThreadPool == null || schemasThreadPool.isShutdown()) {
        // Could read max threads from a configuration property
        schemasThreadPool =
            Executors.newFixedThreadPool(
                DEFAULT_MAX_THREADS_FETCH_SCHEMAS,
                r -> {
                  Thread t = new Thread(r, "jdbc-schemas-fetcher");
                  t.setDaemon(true);
                  return t;
                });
      }
      return schemasThreadPool;
    }
  }
}
