package com.databricks.jdbc.dbclient.impl.sqlexec;

import static com.databricks.jdbc.common.MetadataResultConstants.*;
import static com.databricks.jdbc.dbclient.impl.common.CommandConstants.METADATA_STATEMENT_ID;

import com.databricks.jdbc.api.impl.DatabricksResultSet;
import com.databricks.jdbc.api.impl.ImmutableSqlParameter;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.MetadataOperationType;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.util.JdbcThreadUtils;
import com.databricks.jdbc.common.util.WildcardUtil;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.IDatabricksMetadataClient;
import com.databricks.jdbc.dbclient.impl.common.CommandConstants;
import com.databricks.jdbc.dbclient.impl.common.MetadataResultSetBuilder;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Implementation for {@link IDatabricksMetadataClient} using {@link IDatabricksClient}. */
public class DatabricksMetadataQueryClient implements IDatabricksMetadataClient {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksMetadataQueryClient.class);
  private static final int DEFAULT_MAX_THREADS_METADATA_FETCH = 10;
  private static final int TASK_TIMEOUT_METADATA_FETCH_SEC = 90;
  private static final Object THREAD_POOL_LOCK = new Object();
  private static ExecutorService metadataThreadPool = null;
  private final IDatabricksClient queryExecutionClient;
  private final MetadataResultSetBuilder metadataResultSetBuilder;

  public DatabricksMetadataQueryClient(IDatabricksClient queryExecutionClient) {
    this.queryExecutionClient = queryExecutionClient;
    this.metadataResultSetBuilder =
        new MetadataResultSetBuilder(queryExecutionClient.getConnectionContext());
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
      return metadataResultSetBuilder.getCatalogsResult(
          getResultSet(SQL, session, MetadataOperationType.GET_CATALOGS));
    }

    CommandBuilder commandBuilder = new CommandBuilder(session);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_CATALOGS);
    LOGGER.debug("SQL command to fetch catalogs: {}", SQL);
    return metadataResultSetBuilder.getCatalogsResult(
        getResultSet(SQL, session, MetadataOperationType.GET_CATALOGS));
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
    // Strip JDBC escape sequences from catalog for the result set TABLE_CATALOG column.
    // SHOW SCHEMAS IN `catalog` doesn't return a catalog column from the server,
    // so the client populates it from this parameter. Without stripping, JDBC-escaped
    // underscores (\_) would appear in the result (e.g., "comparator\_tests" instead
    // of "comparator_tests").
    String resultCatalog =
        catalog != null ? WildcardUtil.stripJdbcEscapes(catalog).toLowerCase() : null;
    try {
      return metadataResultSetBuilder.getSchemasResult(
          getResultSet(SQL, session, MetadataOperationType.GET_SCHEMAS), resultCatalog);
    } catch (SQLException e) {
      if (catalog == null && PARSE_SYNTAX_ERROR_SQL_STATE.equals(e.getSQLState())) {
        // This is a fallback for the case where the SQL command fails with "syntax error at or near
        // "ALL CATALOGS""
        // This is a known issue for older DBR versions
        LOGGER.debug("SQL command failed with syntax error. Fetching schemas across all catalogs.");
        return fetchSchemasAcrossCatalogs(session, schemaNamePattern);
      } else if (isObjectNotFoundException(e)) {
        LOGGER.debug("Object not found for getSchemas, returning empty result set.");
        return metadataResultSetBuilder.getSchemasResult(new ArrayList<>());
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
    // Per JDBC spec: null types = return all types; empty array = return nothing
    if (tableTypes != null && tableTypes.length == 0) {
      return metadataResultSetBuilder.getTablesResult(catalog, tableTypes, new ArrayList<>());
    }
    String[] validatedTableTypes = tableTypes != null ? tableTypes : DEFAULT_TABLE_TYPES;

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
          getResultSet(SQL, session, MetadataOperationType.GET_TABLES), validatedTableTypes);
    } catch (SQLException e) {
      if ((PARSE_SYNTAX_ERROR_SQL_STATE.equals(e.getSQLState()) && catalog == null)
          || isObjectNotFoundException(e)
          || isEmptyPatternError(schemaNamePattern, tableNamePattern)) {
        LOGGER.debug("SQL error for getTables ({}), returning empty result set.", e.getSQLState());
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
    try {
      return metadataResultSetBuilder.getColumnsResult(
          getResultSet(SQL, session, MetadataOperationType.GET_COLUMNS));
    } catch (SQLException e) {
      if (isObjectNotFoundException(e)
          || isEmptyPatternError(schemaNamePattern, tableNamePattern, columnNamePattern)) {
        LOGGER.debug("Error for getColumns ({}), returning empty result set.", e.getSQLState());
        return metadataResultSetBuilder.getColumnsResult(new ArrayList<>());
      }
      throw e;
    }
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
    try {
      return metadataResultSetBuilder.getFunctionsResult(
          getResultSet(SQL, session, MetadataOperationType.GET_FUNCTIONS), catalog);
    } catch (SQLException e) {
      if (isObjectNotFoundException(e)) {
        LOGGER.debug("Object not found for getFunctions, returning empty result set.");
        return metadataResultSetBuilder.getFunctionsResult(catalog, new ArrayList<>());
      }
      throw e;
    }
  }

  @Override
  public DatabricksResultSet listProcedures(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String procedureNamePattern)
      throws SQLException {
    String currentCatalog = isMultipleCatalogSupportDisabled() ? session.getCurrentCatalog() : null;
    if (!metadataResultSetBuilder.shouldAllowCatalogAccess(catalog, currentCatalog, session)) {
      return metadataResultSetBuilder.getProceduresResult(new ArrayList<>());
    }

    catalog = autoFillCatalog(catalog, currentCatalog);
    Map<Integer, ImmutableSqlParameter> params = new HashMap<>();
    String SQL =
        CommandConstants.buildProceduresSQL(
            catalog, schemaNamePattern, procedureNamePattern, params);
    LOGGER.debug("SQL command to fetch procedures: {}", SQL);
    return metadataResultSetBuilder.getProceduresResult(
        getResultSet(SQL, params, session, MetadataOperationType.GET_PROCEDURES));
  }

  @Override
  public DatabricksResultSet listProcedureColumns(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String procedureNamePattern,
      String columnNamePattern)
      throws SQLException {
    String currentCatalog = isMultipleCatalogSupportDisabled() ? session.getCurrentCatalog() : null;
    if (!metadataResultSetBuilder.shouldAllowCatalogAccess(catalog, currentCatalog, session)) {
      return metadataResultSetBuilder.getProcedureColumnsResult(new ArrayList<>());
    }

    catalog = autoFillCatalog(catalog, currentCatalog);
    Map<Integer, ImmutableSqlParameter> params = new HashMap<>();
    String SQL =
        CommandConstants.buildProcedureColumnsSQL(
            catalog, schemaNamePattern, procedureNamePattern, columnNamePattern, params);
    LOGGER.debug("SQL command to fetch procedure columns: {}", SQL);
    return metadataResultSetBuilder.getProcedureColumnsResult(
        getResultSet(SQL, params, session, MetadataOperationType.GET_PROCEDURE_COLUMNS));
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

    String[] resolvedParams = resolveKeyBasedParams(catalog, schema, table, session);
    String resolvedCatalog = resolvedParams[0];
    String resolvedSchema = resolvedParams[1];
    String resolvedTable = resolvedParams[2];

    CommandBuilder commandBuilder =
        new CommandBuilder(resolvedCatalog, session)
            .setSchema(resolvedSchema)
            .setTable(resolvedTable);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_PRIMARY_KEYS);
    LOGGER.debug("SQL command to fetch primary keys: {}", SQL);
    try {
      return metadataResultSetBuilder.getPrimaryKeysResult(
          getResultSet(SQL, session, MetadataOperationType.GET_PRIMARY_KEYS));
    } catch (SQLException e) {
      if (isObjectNotFoundException(e)) {
        LOGGER.debug("Object not found for getPrimaryKeys, returning empty result");
        return metadataResultSetBuilder.getPrimaryKeysResult(new ArrayList<>());
      }
      throw e;
    }
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

    String[] resolvedParams = resolveKeyBasedParams(catalog, schema, table, session);
    String resolvedCatalog = resolvedParams[0];
    String resolvedSchema = resolvedParams[1];
    String resolvedTable = resolvedParams[2];

    CommandBuilder commandBuilder =
        new CommandBuilder(resolvedCatalog, session)
            .setSchema(resolvedSchema)
            .setTable(resolvedTable);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_FOREIGN_KEYS);
    try {
      return metadataResultSetBuilder.getImportedKeysResult(
          getResultSet(SQL, session, MetadataOperationType.GET_CROSS_REFERENCE));
    } catch (SQLException e) {
      if (PARSE_SYNTAX_ERROR_SQL_STATE.equals(e.getSQLState()) || isObjectNotFoundException(e)) {
        LOGGER.debug(
            "SQL error for getImportedKeys ({}), returning empty result set.", e.getSQLState());
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

    if (table == null) {
      LOGGER.debug("listExportedKeys: table is null, throwing");
      throw new DatabricksSQLException(
          "Invalid argument: tableName may not be null", DatabricksDriverErrorCode.INVALID_STATE);
    }

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

    // Null foreignTable means "unspecified" — Thrift server returns empty ResultSet
    if (foreignTable == null) {
      LOGGER.debug("listCrossReferences: foreignTable is null, returning empty result set");
      return metadataResultSetBuilder.getCrossRefsResult(new ArrayList<>());
    }

    // Only fetch currentCatalog if multiple catalog support is disabled
    String currentCatalog = isMultipleCatalogSupportDisabled() ? session.getCurrentCatalog() : null;
    if (!metadataResultSetBuilder.shouldAllowCatalogAccess(parentCatalog, currentCatalog, session)
        || !metadataResultSetBuilder.shouldAllowCatalogAccess(
            foreignCatalog, currentCatalog, session)) {
      return metadataResultSetBuilder.getCrossRefsResult(new ArrayList<>());
    }

    // Resolve null catalog/schema for the foreign side (used to build the SQL query)
    String[] resolvedForeignParams =
        resolveKeyBasedParams(foreignCatalog, foreignSchema, foreignTable, session);
    // Resolve null catalog/schema for the parent side (used for filtering results)
    String[] resolvedParentParams =
        resolveKeyBasedParams(parentCatalog, parentSchema, parentTable, session);

    String resolvedForeignCatalog = resolvedForeignParams[0];
    String resolvedForeignSchema = resolvedForeignParams[1];
    String resolvedForeignTable = resolvedForeignParams[2];
    String resolvedParentCatalog = resolvedParentParams[0];
    String resolvedParentSchema = resolvedParentParams[1];
    String resolvedParentTable = resolvedParentParams[2];

    CommandBuilder commandBuilder =
        new CommandBuilder(resolvedForeignCatalog, session)
            .setSchema(resolvedForeignSchema)
            .setTable(resolvedForeignTable);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_FOREIGN_KEYS);
    try {
      return metadataResultSetBuilder.getCrossReferenceKeysResult(
          getResultSet(SQL, session, MetadataOperationType.GET_CROSS_REFERENCE),
          resolvedParentCatalog,
          resolvedParentSchema,
          resolvedParentTable);
    } catch (SQLException e) {
      if (PARSE_SYNTAX_ERROR_SQL_STATE.equals(e.getSQLState()) || isObjectNotFoundException(e)) {
        LOGGER.debug(
            "SQL error for getCrossReference ({}), returning empty result set.", e.getSQLState());
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
    return queryExecutionClient.getConnectionContext() != null
        && !queryExecutionClient.getConnectionContext().getEnableMultipleCatalogSupport();
  }

  /**
   * Returns true if any of the provided patterns is an empty string. Empty string patterns generate
   * invalid LIKE '' clauses that cause server errors. Per JDBC spec, empty string means "without a
   * name" which matches nothing in Unity Catalog.
   */
  private static boolean isEmptyPatternError(String... patterns) {
    for (String p : patterns) {
      if ("".equals(p)) {
        return true;
      }
    }
    return false;
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

  /**
   * Validates and resolves null catalog/schema/table for key-based metadata operations to match
   * Thrift server behavior. Throws DatabricksSQLException for invalid parameter combinations
   * (matching Thrift error behavior). When catalog is null, it is replaced with current_catalog and
   * (if schema is also null) schema is replaced with current_schema.
   *
   * @throws DatabricksSQLException if table is null/empty, or schema is null with an explicit
   *     catalog
   */
  private String[] resolveKeyBasedParams(
      String catalog, String schema, String table, IDatabricksSession session) throws SQLException {
    if (table == null || table.isEmpty()) {
      LOGGER.debug("resolveKeyBasedParams: table is null or empty, throwing");
      throw new DatabricksSQLException(
          "Invalid argument: tableName may not be null or empty",
          DatabricksDriverErrorCode.INVALID_STATE);
    }

    if (catalog == null) {
      String[] currentCatalogAndSchema = session.getCurrentCatalogAndSchema();
      catalog = currentCatalogAndSchema[0];
      if (schema == null) {
        schema = currentCatalogAndSchema[1];
      }
    } else if (schema == null) {
      LOGGER.debug(
          "resolveKeyBasedParams: schema is null with explicit catalog '{}', throwing", catalog);
      throw new DatabricksSQLException(
          "Invalid argument: schema may not be null when catalog is specified",
          DatabricksDriverErrorCode.INVALID_STATE);
    }

    // Safety net: getCurrentCatalogAndSchema() returned null values
    if (catalog == null || schema == null) {
      LOGGER.debug(
          "resolveKeyBasedParams: could not resolve catalog or schema (catalog={}, schema={})",
          catalog,
          schema);
      throw new DatabricksSQLException(
          "Invalid argument: could not resolve catalog or schema",
          DatabricksDriverErrorCode.INVALID_STATE);
    }

    return new String[] {catalog, schema, table};
  }

  private DatabricksResultSet getResultSet(
      String SQL, IDatabricksSession session, MetadataOperationType metadataOperationType)
      throws SQLException {
    return getResultSet(SQL, new HashMap<>(), session, metadataOperationType);
  }

  private DatabricksResultSet getResultSet(
      String SQL,
      Map<Integer, ImmutableSqlParameter> params,
      IDatabricksSession session,
      MetadataOperationType metadataOperationType)
      throws SQLException {
    return queryExecutionClient.executeStatement(
        SQL,
        session.getComputeResource(),
        params,
        StatementType.METADATA,
        session,
        null /* parentStatement */,
        metadataOperationType);
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
