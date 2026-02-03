package com.databricks.jdbc.dbclient.impl.sqlexec;

import static com.databricks.jdbc.common.MetadataResultConstants.*;
import static com.databricks.jdbc.dbclient.impl.common.CommandConstants.METADATA_STATEMENT_ID;

import com.databricks.jdbc.api.impl.DatabricksResultSet;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.util.JdbcThreadUtils;
import com.databricks.jdbc.common.util.WildcardUtil;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.IDatabricksMetadataClient;
import com.databricks.jdbc.dbclient.impl.common.MetadataResultSetBuilder;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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
  private static final int DEFAULT_MAX_THREADS_METADATA_FETCH = 10;
  private static final int TASK_TIMEOUT_METADATA_FETCH_SEC = 90;
  private static final Object THREAD_POOL_LOCK = new Object();
  private static ExecutorService metadataThreadPool = null;
  private final IDatabricksClient sdkClient;
  private final MetadataResultSetBuilder metadataResultSetBuilder;

  public DatabricksMetadataSdkClient(IDatabricksClient sdkClient) {
    this.sdkClient = sdkClient;
    this.metadataResultSetBuilder = new MetadataResultSetBuilder(sdkClient.getConnectionContext());
  }

  @Override
  public DatabricksResultSet listTypeInfo(IDatabricksSession session) {
    LOGGER.debug("public ResultSet getTypeInfo()");
    return metadataResultSetBuilder.getTypeInfoResult();
  }

  @Override
  public DatabricksResultSet listCatalogs(IDatabricksSession session) throws SQLException {
    // If multiple catalog support is disabled, return only the current catalog
    if (isMultipleCatalogSupportDisabled()) {
      String currentCatalog = session.getCurrentCatalog();
      if (currentCatalog == null || currentCatalog.isEmpty()) {
        currentCatalog = "spark";
        LOGGER.debug(
            "Current catalog is null or empty when multiple catalog support is disabled. Using default catalog: {}",
            currentCatalog);
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
    // Only fetch currentCatalog if multiple catalog support is disabled
    String currentCatalog = isMultipleCatalogSupportDisabled() ? session.getCurrentCatalog() : null;
    if (!metadataResultSetBuilder.shouldAllowCatalogAccess(catalog, currentCatalog, session)) {
      return metadataResultSetBuilder.getSchemasResult(new ArrayList<>());
    }

    catalog = autoFillCatalog(catalog, currentCatalog);

    // Return empty result set if catalog is an empty string
    if (catalog != null && catalog.isEmpty()) {
      LOGGER.debug("Catalog is empty string, returning empty result set for listSchemas");
      return metadataResultSetBuilder.getResultSetWithGivenRowsAndColumns(
          SCHEMA_COLUMNS,
          new ArrayList<>(),
          METADATA_STATEMENT_ID,
          com.databricks.jdbc.common.CommandName.LIST_SCHEMAS);
    }

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
    String[] validatedTableTypes =
        Optional.ofNullable(tableTypes)
            .filter(types -> types.length > 0)
            .orElse(DEFAULT_TABLE_TYPES);

    // Only fetch currentCatalog if multiple catalog support is disabled
    String currentCatalog = isMultipleCatalogSupportDisabled() ? session.getCurrentCatalog() : null;
    if (!metadataResultSetBuilder.shouldAllowCatalogAccess(catalog, currentCatalog, session)) {
      return metadataResultSetBuilder.getTablesResult(
          catalog, validatedTableTypes, new ArrayList<>());
    }

    catalog = autoFillCatalog(catalog, currentCatalog);
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
        return metadataResultSetBuilder.getTablesResult(
            catalog, validatedTableTypes, new ArrayList<>());
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
    // Only fetch currentCatalog if multiple catalog support is disabled
    String currentCatalog = isMultipleCatalogSupportDisabled() ? session.getCurrentCatalog() : null;
    if (!metadataResultSetBuilder.shouldAllowCatalogAccess(catalog, currentCatalog, session)) {
      return metadataResultSetBuilder.getColumnsResult(new ArrayList<>());
    }

    catalog = autoFillCatalog(catalog, currentCatalog);

    // Fetch columns from all catalogs if catalog is null
    if (catalog == null) {
      LOGGER.debug("Catalog is null, fetching columns across all catalogs");
      return fetchColumnsAcrossCatalogs(
          session, schemaNamePattern, tableNamePattern, columnNamePattern);
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
    // Only fetch currentCatalog if multiple catalog support is disabled
    String currentCatalog = isMultipleCatalogSupportDisabled() ? session.getCurrentCatalog() : null;
    if (!metadataResultSetBuilder.shouldAllowCatalogAccess(catalog, currentCatalog, session)) {
      return metadataResultSetBuilder.getFunctionsResult(catalog, new ArrayList<>());
    }

    catalog = autoFillCatalog(catalog, currentCatalog);

    // Fetch current catalog if catalog is null
    if (catalog == null) {
      // #TODO: Make server side changes
      LOGGER.debug("Catalog is null, fetching current catalog for listFunctions");
      catalog = session.getCurrentCatalog();

      // Use hive_metastore as fallback if current catalog is null
      if (catalog == null) {
        LOGGER.debug("Current catalog is null, using hive_metastore as fallback");
        catalog = "hive_metastore";
      } else {
        LOGGER.debug("Using current catalog: {}", catalog);
      }
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
    // Only fetch currentCatalog if multiple catalog support is disabled
    String currentCatalog = isMultipleCatalogSupportDisabled() ? session.getCurrentCatalog() : null;
    if (!metadataResultSetBuilder.shouldAllowCatalogAccess(catalog, currentCatalog, session)) {
      return metadataResultSetBuilder.getPrimaryKeysResult(new ArrayList<>());
    }

    catalog = autoFillCatalog(catalog, currentCatalog);

    // Return empty result set if catalog, schema, or table is null
    if (catalog == null || schema == null || table == null) {
      LOGGER.debug(
          "Catalog, schema, or table is null (catalog={}, schema={}, table={}), returning empty result set for listPrimaryKeys",
          catalog,
          schema,
          table);
      return metadataResultSetBuilder.getResultSetWithGivenRowsAndColumns(
          PRIMARY_KEYS_COLUMNS,
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

    // Only fetch currentCatalog if multiple catalog support is disabled
    String currentCatalog = isMultipleCatalogSupportDisabled() ? session.getCurrentCatalog() : null;
    if (!metadataResultSetBuilder.shouldAllowCatalogAccess(catalog, currentCatalog, session)) {
      return metadataResultSetBuilder.getImportedKeys(new ArrayList<>());
    }

    catalog = autoFillCatalog(catalog, currentCatalog);

    // Return empty result set if catalog, schema, or table is null
    if (catalog == null || schema == null || table == null) {
      LOGGER.debug(
          "Catalog, schema, or table is null (catalog={}, schema={}, table={}), returning empty result set for listImportedKeys",
          catalog,
          schema,
          table);
      return metadataResultSetBuilder.getResultSetWithGivenRowsAndColumns(
          IMPORTED_KEYS_COLUMNS,
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
        return metadataResultSetBuilder.getImportedKeys(new ArrayList<>());
      } else {
        throw e;
      }
    }
  }

  @Override
  public DatabricksResultSet listExportedKeys(
      IDatabricksSession session, String catalog, String schema, String table) throws SQLException {
    LOGGER.debug("public ResultSet listExportedKeys() using SDK");

    // Only fetch currentCatalog if multiple catalog support is disabled
    String currentCatalog = isMultipleCatalogSupportDisabled() ? session.getCurrentCatalog() : null;
    if (!metadataResultSetBuilder.shouldAllowCatalogAccess(catalog, currentCatalog, session)) {
      return metadataResultSetBuilder.getExportedKeys(new ArrayList<>());
    }

    catalog = autoFillCatalog(catalog, currentCatalog);

    // Exported keys not tracked in DBSQL. Returning empty result set
    return metadataResultSetBuilder.getExportedKeys(new ArrayList<>());
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

    // Only fetch currentCatalog if multiple catalog support is disabled
    String currentCatalog = isMultipleCatalogSupportDisabled() ? session.getCurrentCatalog() : null;
    if (!metadataResultSetBuilder.shouldAllowCatalogAccess(parentCatalog, currentCatalog, session)
        || !metadataResultSetBuilder.shouldAllowCatalogAccess(
            foreignCatalog, currentCatalog, session)) {
      return metadataResultSetBuilder.getCrossRefsResult(new ArrayList<>());
    }

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
        return metadataResultSetBuilder.getCrossRefsResult(new ArrayList<>());
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

  /**
   * Auto-fills the catalog parameter if multiple catalog support is disabled and catalog is null.
   *
   * @param catalog the catalog parameter to auto-fill
   * @param currentCatalog the current catalog from the session
   * @return the auto-filled catalog or the original catalog if no auto-fill is needed
   */
  private String autoFillCatalog(String catalog, String currentCatalog) {
    if (isMultipleCatalogSupportDisabled() && catalog == null) {
      String result =
          (currentCatalog != null && !currentCatalog.isEmpty()) ? currentCatalog : "spark";
      LOGGER.debug(
          "Auto-filling null catalog with '{}' when multiple catalog support is disabled", result);
      return result;
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
            DEFAULT_MAX_THREADS_METADATA_FETCH, // Not significant since the executor is provided as
            // a parameter
            TASK_TIMEOUT_METADATA_FETCH_SEC,
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
            getOrCreateMetadataThreadPool());

    // Convert combined data into a result set
    return metadataResultSetBuilder.getResultSetWithGivenRowsAndColumns(
        SCHEMA_COLUMNS,
        schemaRows,
        METADATA_STATEMENT_ID,
        com.databricks.jdbc.common.CommandName.LIST_SCHEMAS);
  }

  private DatabricksResultSet fetchColumnsAcrossCatalogs(
      IDatabricksSession session,
      String schemaNamePattern,
      String tableNamePattern,
      String columnNamePattern)
      throws SQLException {
    List<String> catalogList = new ArrayList<>();
    try (ResultSet catalogs = session.getDatabricksMetadataClient().listCatalogs(session)) {
      while (catalogs.next()) {
        String c = catalogs.getString(1);
        if (c != null && !c.isEmpty()) {
          catalogList.add(c);
        }
      }
    }

    // Process catalogs in parallel, gathering column information
    List<List<Object>> columnRows =
        JdbcThreadUtils.parallelFlatMap(
            catalogList,
            session.getConnectionContext(),
            DEFAULT_MAX_THREADS_METADATA_FETCH,
            TASK_TIMEOUT_METADATA_FETCH_SEC,
            c -> {
              List<List<Object>> rows = new ArrayList<>();
              try (ResultSet catalogColumns =
                  session
                      .getDatabricksMetadataClient()
                      .listColumns(
                          session, c, schemaNamePattern, tableNamePattern, columnNamePattern)) {
                ResultSetMetaData metaData = catalogColumns.getMetaData();
                int columnCount = metaData.getColumnCount();
                while (catalogColumns.next()) {
                  List<Object> columnRow = new ArrayList<>();
                  // Read all columns from the result set
                  for (int i = 1; i <= columnCount; i++) {
                    columnRow.add(catalogColumns.getObject(i));
                  }
                  rows.add(columnRow);
                }
              } catch (SQLException e) {
                LOGGER.warn("Error fetching columns for catalog {} {}", c, e.getMessage());
              }
              return rows;
            },
            getOrCreateMetadataThreadPool());

    // Convert combined data into a result set
    return metadataResultSetBuilder.getResultSetWithGivenRowsAndColumns(
        COLUMN_COLUMNS,
        columnRows,
        METADATA_STATEMENT_ID,
        com.databricks.jdbc.common.CommandName.LIST_COLUMNS);
  }

  public static ExecutorService getOrCreateMetadataThreadPool() {
    synchronized (THREAD_POOL_LOCK) {
      if (metadataThreadPool == null || metadataThreadPool.isShutdown()) {
        // Could read max threads from a configuration property
        metadataThreadPool =
            Executors.newFixedThreadPool(
                DEFAULT_MAX_THREADS_METADATA_FETCH,
                r -> {
                  Thread t = new Thread(r, "jdbc-metadata-fetcher");
                  t.setDaemon(true);
                  return t;
                });
      }
      return metadataThreadPool;
    }
  }
}
