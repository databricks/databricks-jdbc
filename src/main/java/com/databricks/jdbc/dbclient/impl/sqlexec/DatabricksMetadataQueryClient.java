package com.databricks.jdbc.dbclient.impl.sqlexec;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.OPERATION_ERROR_SQLSTATE;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.SYNTAX_OR_ACCESS_VIOLATION_SQLSTATE;
import static com.databricks.jdbc.common.MetadataResultConstants.*;
import static com.databricks.jdbc.dbclient.impl.common.CommandConstants.METADATA_STATEMENT_ID;

import com.databricks.jdbc.api.impl.ApcParameterCapabilities;
import com.databricks.jdbc.api.impl.DatabricksResultSet;
import com.databricks.jdbc.api.impl.ImmutableSqlParameter;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.MetadataOperationType;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.TelemetryLogLevel;
import com.databricks.jdbc.common.util.JdbcThreadUtils;
import com.databricks.jdbc.common.util.SQLInterpolator;
import com.databricks.jdbc.common.util.WildcardUtil;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.IDatabricksMetadataClient;
import com.databricks.jdbc.dbclient.MetadataAsyncHandle;
import com.databricks.jdbc.dbclient.MetadataAsyncRequest;
import com.databricks.jdbc.dbclient.impl.common.CommandConstants;
import com.databricks.jdbc.dbclient.impl.common.MetadataResultSetBuilder;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.dbclient.impl.common.TimeoutHandler;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.jdbc.telemetry.TelemetryHelper;
import com.databricks.sdk.service.sql.StatementState;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

/** Implementation for {@link IDatabricksMetadataClient} using {@link IDatabricksClient}. */
public class DatabricksMetadataQueryClient implements IDatabricksMetadataClient {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksMetadataQueryClient.class);
  private static final int DEFAULT_MAX_THREADS_METADATA_FETCH = 10;
  private static final int TASK_TIMEOUT_METADATA_FETCH_SEC = 90;
  private static final long DEFAULT_METADATA_POLL_INTERVAL_MILLIS = 50;
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
  public IDatabricksConnectionContext getConnectionContext() {
    return queryExecutionClient.getConnectionContext();
  }

  @Override
  public MetadataAsyncHandle executeMetadataAsync(
      MetadataAsyncRequest request, IDatabricksSession session) throws SQLException {
    if (request == null
        || request.getMethod() == null
        || !request.getOperation().isShowOperation()) {
      throw unsupportedAsyncMetadata();
    }
    String catalog = request.getCatalog();
    String[] parent = null;
    String sql;
    CommandBuilder builder;
    switch (request.getMethod()) {
      case CATALOGS:
        if (isMultipleCatalogSupportDisabled()) {
          return immediateHandle(request, listCatalogs(session));
        }
        sql = new CommandBuilder(session).getSQLString(CommandName.LIST_CATALOGS);
        break;
      case SCHEMAS:
        if (emptyCatalogResult(request, session)) {
          return immediateHandle(
              request, metadataResultSetBuilder.getSchemasResult(new ArrayList<>()));
        }
        catalog =
            autoFillCatalog(
                catalog, isMultipleCatalogSupportDisabled() ? session.getCurrentCatalog() : null);
        sql =
            new CommandBuilder(catalog, session)
                .setSchemaPattern(request.getSchema())
                .getSQLString(CommandName.LIST_SCHEMAS);
        break;
      case TABLES:
        if (request.getTableTypes() != null && request.getTableTypes().length == 0) {
          return immediateHandle(
              request,
              metadataResultSetBuilder.getTablesResult(
                  catalog, request.getTableTypes(), new ArrayList<>()));
        }
        if (emptyCatalogResult(request, session)) {
          return immediateHandle(
              request,
              metadataResultSetBuilder.getTablesResult(
                  catalog, request.getTableTypes(), new ArrayList<>()));
        }
        catalog =
            autoFillCatalog(
                catalog, isMultipleCatalogSupportDisabled() ? session.getCurrentCatalog() : null);
        sql =
            new CommandBuilder(catalog, session)
                .setSchemaPattern(request.getSchema())
                .setTablePattern(request.getTable())
                .getSQLString(CommandName.LIST_TABLES);
        break;
      case COLUMNS:
        if (emptyCatalogResult(request, session)) {
          return immediateHandle(
              request, metadataResultSetBuilder.getColumnsResult(new ArrayList<>()));
        }
        catalog =
            autoFillCatalog(
                catalog, isMultipleCatalogSupportDisabled() ? session.getCurrentCatalog() : null);
        sql =
            new CommandBuilder(catalog, session)
                .setSchemaPattern(request.getSchema())
                .setTablePattern(request.getTable())
                .setColumnPattern(request.getColumn())
                .getSQLString(CommandName.LIST_COLUMNS);
        break;
      case FUNCTIONS:
        if (emptyCatalogResult(request, session)) {
          return immediateHandle(
              request, metadataResultSetBuilder.getFunctionsResult(catalog, new ArrayList<>()));
        }
        catalog =
            autoFillCatalog(
                catalog, isMultipleCatalogSupportDisabled() ? session.getCurrentCatalog() : null);
        if (catalog == null) {
          catalog = session.getCurrentCatalog();
          if (catalog == null) {
            catalog = "hive_metastore";
          }
        }
        sql =
            new CommandBuilder(catalog, session)
                .setSchemaPattern(request.getSchema())
                .setFunctionPattern(request.getFunctionPattern())
                .getSQLString(CommandName.LIST_FUNCTIONS);
        break;
      case PRIMARY_KEYS:
      case IMPORTED_KEYS:
      case CROSS_REFERENCES:
        if (request.getMethod() == MetadataAsyncRequest.Method.CROSS_REFERENCES
            && request.getTable() == null) {
          return immediateHandle(
              request, metadataResultSetBuilder.getCrossRefsResult(new ArrayList<>()));
        }
        String[] key = null;
        // With multiple catalogs, synchronous key metadata validates these arguments before
        // building SQL, including when an empty catalog produces an empty result.
        if (!isMultipleCatalogSupportDisabled()) {
          key = resolveKeyBasedParams(catalog, request.getSchema(), request.getTable(), session);
          if (request.getMethod() == MetadataAsyncRequest.Method.CROSS_REFERENCES) {
            parent =
                resolveKeyBasedParams(
                    request.getParentCatalog(),
                    request.getParentSchema(),
                    request.getParentTable(),
                    session);
          }
        }
        if (emptyCatalogResult(request, session)) {
          return immediateHandle(request, emptyKeyResult(request));
        }
        // Synchronous listCrossReferences passes the raw foreign catalog so that null catalog and
        // schema are both resolved from the session; auto-filling here would make a null schema
        // throw.
        if (request.getMethod() != MetadataAsyncRequest.Method.CROSS_REFERENCES) {
          catalog =
              autoFillCatalog(
                  catalog, isMultipleCatalogSupportDisabled() ? session.getCurrentCatalog() : null);
        }
        if (key == null) {
          key = resolveKeyBasedParams(catalog, request.getSchema(), request.getTable(), session);
        }
        catalog = key[0];
        builder = new CommandBuilder(key[0], session).setSchema(key[1]).setTable(key[2]);
        sql =
            builder.getSQLString(
                request.getMethod() == MetadataAsyncRequest.Method.PRIMARY_KEYS
                    ? CommandName.LIST_PRIMARY_KEYS
                    : CommandName.LIST_FOREIGN_KEYS);
        if (request.getMethod() == MetadataAsyncRequest.Method.CROSS_REFERENCES && parent == null) {
          parent =
              resolveKeyBasedParams(
                  request.getParentCatalog(),
                  request.getParentSchema(),
                  request.getParentTable(),
                  session);
        }
        break;
      default:
        throw unsupportedAsyncMetadata();
    }
    try (DatabricksResultSet submitted =
        queryExecutionClient.executeStatementAsync(
            sql,
            session.getComputeResource(),
            java.util.Collections.emptyMap(),
            session,
            null,
            StatementType.METADATA,
            request.getOperation())) {
      StatementId id =
          StatementId.forCurrentSession(submitted.getStatementId(), session.getSessionInfo());
      return new MetadataAsyncHandle(id, request, catalog, parent);
    } catch (SQLException e) {
      // Match getMetadataExecutionResult: submission can fail before a statement_id exists, but
      // callers still expect parse-error fan-out and object-not-found empty results.
      if (isTerminalMetadataError(e)) {
        throw e;
      }
      if (catalog == null
          && PARSE_SYNTAX_ERROR_SQL_STATE.equals(e.getSQLState())
          && (request.getMethod() == MetadataAsyncRequest.Method.SCHEMAS
              || request.getMethod() == MetadataAsyncRequest.Method.COLUMNS)) {
        MetadataAsyncHandle failed = new MetadataAsyncHandle(null, request, catalog, parent);
        TimeoutHandler timeout =
            new TimeoutHandler(
                queryExecutionClient.getConnectionContext().getMetadataOperationTimeout(),
                "asynchronous metadata",
                () -> {},
                DatabricksDriverErrorCode.OPERATION_TIMEOUT_ERROR,
                true);
        return immediateHandle(
            request, fallbackAcrossCatalogs(failed, session, timeout, new AtomicReference<>()));
      }
      if (shouldReturnEmpty(request, catalog, e)) {
        return immediateHandle(request, emptyResult(request, catalog));
      }
      throw e;
    }
  }

  @Override
  public DatabricksResultSet getMetadataExecutionResult(
      MetadataAsyncHandle handle, IDatabricksSession session) throws SQLException {
    if (handle == null) {
      throw new DatabricksSQLException(
          "Metadata async handle is required",
          DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR.name(),
          DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR);
    }
    if (handle.getImmediateResult() != null) {
      return handle.getImmediateResult();
    }
    AtomicReference<StatementId> activeId = new AtomicReference<>(handle.getStatementId());
    TimeoutHandler timeout =
        new TimeoutHandler(
            queryExecutionClient.getConnectionContext().getMetadataOperationTimeout(),
            "asynchronous metadata",
            () -> {
              try {
                queryExecutionClient.cancelStatement(activeId.get());
              } catch (SQLException e) {
                LOGGER.warn("Could not cancel timed out metadata statement");
              }
            },
            DatabricksDriverErrorCode.OPERATION_TIMEOUT_ERROR,
            true);
    try {
      return convertCompletedMetadata(handle, session, timeout);
    } catch (SQLException e) {
      MetadataAsyncRequest request = handle.getRequest();
      if (isTerminalMetadataError(e)) {
        throw e;
      }
      if (handle.getResolvedCatalog() == null
          && PARSE_SYNTAX_ERROR_SQL_STATE.equals(e.getSQLState())
          && (request.getMethod() == MetadataAsyncRequest.Method.SCHEMAS
              || request.getMethod() == MetadataAsyncRequest.Method.COLUMNS)) {
        return fallbackAcrossCatalogs(handle, session, timeout, activeId);
      }
      if (shouldReturnEmpty(request, handle.getResolvedCatalog(), e)) {
        return emptyResult(request, handle.getResolvedCatalog());
      }
      throw e;
    } finally {
      closeMetadataOperation(handle.getStatementId());
      for (StatementId id : handle.getFallbackStatementIds()) {
        closeMetadataOperation(id);
      }
    }
  }

  private DatabricksResultSet convertCompletedMetadata(
      MetadataAsyncHandle handle, IDatabricksSession session, TimeoutHandler timeout)
      throws SQLException {
    DatabricksResultSet source = awaitMetadata(handle.getStatementId(), session, timeout);
    try {
      return convertMetadata(handle, source);
    } finally {
      closeMetadataSource(source);
    }
  }

  private void closeMetadataSource(DatabricksResultSet source) {
    try {
      source.close();
    } catch (SQLException e) {
      LOGGER.warn("Could not close metadata result");
    }
  }

  private void closeMetadataOperation(StatementId id) {
    // Warehouse metadata did not issue an explicit CloseStatement before APC support.
    if (id == null || id.computeResource() == null) {
      return;
    }
    try {
      queryExecutionClient.closeStatement(id);
    } catch (SQLException e) {
      LOGGER.warn("Could not close metadata operation");
    }
  }

  @FunctionalInterface
  private interface MetadataResultConverter {
    DatabricksResultSet convert(DatabricksResultSet source) throws SQLException;
  }

  private DatabricksResultSet convertSynchronousMetadata(
      DatabricksResultSet source, IDatabricksSession session, MetadataResultConverter converter)
      throws SQLException {
    StatementId id = null;
    try {
      String rawId = source.getStatementId();
      if (rawId != null) {
        id =
            session.getSessionInfo() == null
                ? new StatementId(rawId)
                : StatementId.forCurrentSession(rawId, session.getSessionInfo());
      }
      return converter.convert(source);
    } finally {
      closeMetadataSource(source);
      closeMetadataOperation(id);
    }
  }

  private DatabricksResultSet awaitMetadata(
      StatementId id, IDatabricksSession session, TimeoutHandler timeout) throws SQLException {
    long pollIntervalMillis = metadataPollIntervalMillis();
    while (true) {
      timeout.checkTimeout();
      DatabricksResultSet result =
          queryExecutionClient.getStatementResult(id, session, null, StatementType.METADATA);
      boolean completed = false;
      try {
        StatementState state = result.getStatementStatus().getState();
        if (state == StatementState.SUCCEEDED || state == StatementState.CLOSED) {
          completed = true;
          return result;
        }
        if (state == StatementState.CANCELED) {
          throw new DatabricksSQLException(
              "Metadata statement was cancelled",
              com.databricks.jdbc.common.DatabricksJdbcConstants.OPERATION_CANCELLED_SQLSTATE,
              DatabricksDriverErrorCode.EXECUTE_STATEMENT_CANCELLED);
        }
        if (state == StatementState.FAILED) {
          throw new DatabricksSQLException(
              "Metadata execution " + state,
              result.getStatementStatus().getSqlState(),
              DatabricksDriverErrorCode.EXECUTE_STATEMENT_FAILED);
        }
        if (state != StatementState.PENDING && state != StatementState.RUNNING) {
          throw new DatabricksSQLException(
              "Unexpected metadata execution state " + state,
              DatabricksDriverErrorCode.INVALID_STATE.name(),
              DatabricksDriverErrorCode.INVALID_STATE);
        }
      } finally {
        if (!completed) {
          closeMetadataSource(result);
        }
      }
      try {
        Thread.sleep(pollIntervalMillis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new DatabricksSQLException(
            "Interrupted while waiting for metadata",
            DatabricksDriverErrorCode.EXECUTE_STATEMENT_FAILED.name(),
            DatabricksDriverErrorCode.EXECUTE_STATEMENT_FAILED.getCode(),
            e);
      }
    }
  }

  /**
   * Honours the same poll interval as statement execution so catalog fan-out does not hammer the
   * server. Falls back to a short default when no positive interval is configured.
   */
  private long metadataPollIntervalMillis() throws SQLException {
    IDatabricksConnectionContext context = queryExecutionClient.getConnectionContext();
    int configured = context == null ? 0 : context.getAsyncExecPollInterval();
    return configured > 0 ? configured : DEFAULT_METADATA_POLL_INTERVAL_MILLIS;
  }

  private MetadataAsyncHandle immediateHandle(
      MetadataAsyncRequest request, DatabricksResultSet result) {
    return new MetadataAsyncHandle(null, request, request.getCatalog(), null, result);
  }

  private boolean emptyCatalogResult(MetadataAsyncRequest request, IDatabricksSession session)
      throws SQLException {
    String currentCatalog = isMultipleCatalogSupportDisabled() ? session.getCurrentCatalog() : null;
    return !metadataResultSetBuilder.shouldAllowCatalogAccess(
            request.getCatalog(), currentCatalog, session)
        || (request.getMethod() == MetadataAsyncRequest.Method.CROSS_REFERENCES
            && !metadataResultSetBuilder.shouldAllowCatalogAccess(
                request.getParentCatalog(), currentCatalog, session))
        || "".equals(request.getCatalog());
  }

  private DatabricksResultSet emptyKeyResult(MetadataAsyncRequest request) throws SQLException {
    switch (request.getMethod()) {
      case PRIMARY_KEYS:
        return metadataResultSetBuilder.getPrimaryKeysResult(new ArrayList<>());
      case IMPORTED_KEYS:
        return metadataResultSetBuilder.getImportedKeys(new ArrayList<>());
      case CROSS_REFERENCES:
        return metadataResultSetBuilder.getCrossRefsResult(new ArrayList<>());
      default:
        throw new SQLException("Not a key metadata operation");
    }
  }

  private DatabricksResultSet convertMetadata(
      MetadataAsyncHandle handle, DatabricksResultSet result) throws SQLException {
    MetadataAsyncRequest request = handle.getRequest();
    String catalog = handle.getResolvedCatalog();
    switch (request.getMethod()) {
      case CATALOGS:
        return metadataResultSetBuilder.getCatalogsResult(result);
      case SCHEMAS:
        return metadataResultSetBuilder.getSchemasResult(
            result, catalog == null ? null : WildcardUtil.stripJdbcEscapes(catalog).toLowerCase());
      case TABLES:
        String[] types = request.getTableTypes();
        return metadataResultSetBuilder.getTablesResult(
            result,
            request.getCatalog(),
            result.isThriftNativeMetadataResult()
                ? types
                : types == null ? DEFAULT_TABLE_TYPES : types);
      case COLUMNS:
        return metadataResultSetBuilder.getColumnsResult(result);
      case FUNCTIONS:
        return metadataResultSetBuilder.getFunctionsResult(
            result, result.isThriftNativeMetadataResult() ? request.getCatalog() : catalog);
      case PRIMARY_KEYS:
        return metadataResultSetBuilder.getPrimaryKeysResult(result);
      case IMPORTED_KEYS:
        return metadataResultSetBuilder.getImportedKeysResult(result);
      case CROSS_REFERENCES:
        String[] parent = handle.getResolvedParent();
        return metadataResultSetBuilder.getCrossReferenceKeysResult(
            result, parent[0], parent[1], parent[2]);
      default:
        throw unsupportedAsyncMetadata();
    }
  }

  private static SQLFeatureNotSupportedException unsupportedAsyncMetadata() {
    return new SQLFeatureNotSupportedException(
        "Only SHOW metadata operations support async execution",
        DatabricksDriverErrorCode.UNSUPPORTED_OPERATION.name(),
        DatabricksDriverErrorCode.UNSUPPORTED_OPERATION.getCode());
  }

  private boolean shouldReturnEmpty(
      MetadataAsyncRequest request, String resolvedCatalog, SQLException error) {
    boolean notFound = isObjectNotFoundException(error);
    switch (request.getMethod()) {
      case SCHEMAS:
      case FUNCTIONS:
      case PRIMARY_KEYS:
        return notFound;
      case TABLES:
        return notFound
            || (resolvedCatalog == null && PARSE_SYNTAX_ERROR_SQL_STATE.equals(error.getSQLState()))
            || isEmptyPatternError(request.getSchema(), request.getTable());
      case COLUMNS:
        return notFound
            || isEmptyPatternError(request.getSchema(), request.getTable(), request.getColumn());
      case IMPORTED_KEYS:
      case CROSS_REFERENCES:
        return notFound || PARSE_SYNTAX_ERROR_SQL_STATE.equals(error.getSQLState());
      default:
        return false;
    }
  }

  private DatabricksResultSet emptyResult(MetadataAsyncRequest request, String catalog)
      throws SQLException {
    switch (request.getMethod()) {
      case SCHEMAS:
        return metadataResultSetBuilder.getSchemasResult(new ArrayList<>());
      case TABLES:
        return metadataResultSetBuilder.getTablesResult(
            catalog,
            request.getTableTypes() == null ? DEFAULT_TABLE_TYPES : request.getTableTypes(),
            new ArrayList<>());
      case COLUMNS:
        return metadataResultSetBuilder.getColumnsResult(new ArrayList<>());
      case FUNCTIONS:
        return metadataResultSetBuilder.getFunctionsResult(catalog, new ArrayList<>());
      case PRIMARY_KEYS:
      case IMPORTED_KEYS:
      case CROSS_REFERENCES:
        return emptyKeyResult(request);
      default:
        throw new SQLException("No empty result for " + request.getOperation());
    }
  }

  private DatabricksResultSet fallbackAcrossCatalogs(
      MetadataAsyncHandle handle,
      IDatabricksSession session,
      TimeoutHandler timeout,
      AtomicReference<StatementId> activeId)
      throws SQLException {
    MetadataAsyncHandle catalogsHandle =
        executeMetadataAsync(MetadataAsyncRequest.forCatalogs(), session);
    activeId.set(catalogsHandle.getStatementId());
    handle.addFallbackStatementId(catalogsHandle.getStatementId());
    DatabricksResultSet catalogs = convertCompletedMetadata(catalogsHandle, session, timeout);
    List<List<Object>> rows = new ArrayList<>();
    try (ResultSet catalogRows = catalogs) {
      while (catalogRows.next()) {
        String catalog = catalogRows.getString(1);
        if (catalog == null || catalog.isEmpty()) {
          continue;
        }
        MetadataAsyncRequest original = handle.getRequest();
        MetadataAsyncRequest request =
            original.getMethod() == MetadataAsyncRequest.Method.SCHEMAS
                ? MetadataAsyncRequest.forSchemas(catalog, original.getSchema())
                : MetadataAsyncRequest.forColumns(
                    catalog, original.getSchema(), original.getTable(), original.getColumn());
        try {
          timeout.checkTimeout();
          MetadataAsyncHandle child = executeMetadataAsync(request, session);
          activeId.set(child.getStatementId());
          handle.addFallbackStatementId(child.getStatementId());
          try (ResultSet converted = convertCompletedMetadata(child, session, timeout)) {
            ResultSetMetaData metadata = converted.getMetaData();
            while (converted.next()) {
              List<Object> row = new ArrayList<>();
              for (int i = 1; i <= metadata.getColumnCount(); i++) {
                row.add(converted.getObject(i));
              }
              rows.add(row);
            }
          }
        } catch (SQLException e) {
          if (isTerminalMetadataError(e)) {
            throw e;
          }
          LOGGER.warn("Error fetching metadata for catalog {}", catalog);
        }
      }
    }
    boolean schemas = handle.getRequest().getMethod() == MetadataAsyncRequest.Method.SCHEMAS;
    return metadataResultSetBuilder.getResultSetWithGivenRowsAndColumns(
        schemas ? SCHEMA_COLUMNS : COLUMN_COLUMNS,
        rows,
        METADATA_STATEMENT_ID,
        schemas
            ? com.databricks.jdbc.common.CommandName.LIST_SCHEMAS
            : com.databricks.jdbc.common.CommandName.LIST_COLUMNS);
  }

  private static boolean isTerminalMetadataError(SQLException error) {
    return error instanceof com.databricks.jdbc.exception.DatabricksTimeoutException
        || com.databricks.jdbc.common.DatabricksJdbcConstants.OPERATION_CANCELLED_SQLSTATE.equals(
            error.getSQLState())
        || Thread.currentThread().isInterrupted()
        || hasInterruptedCause(error);
  }

  private static boolean hasInterruptedCause(Throwable error) {
    for (Throwable cause = error; cause != null; cause = cause.getCause()) {
      if (cause instanceof InterruptedException) {
        return true;
      }
    }
    return false;
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
      return metadataResultSetBuilder.getCatalogsResult(List.of(List.of(currentCatalog)));
    }

    CommandBuilder commandBuilder = new CommandBuilder(session);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_CATALOGS);
    LOGGER.debug("SQL command to fetch catalogs: {}", SQL);
    return convertSynchronousMetadata(
        getResultSet(SQL, session, MetadataOperationType.GET_CATALOGS),
        session,
        metadataResultSetBuilder::getCatalogsResult);
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
      return convertSynchronousMetadata(
          getResultSet(SQL, session, MetadataOperationType.GET_SCHEMAS),
          session,
          source -> metadataResultSetBuilder.getSchemasResult(source, resultCatalog));
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
    // Runtime does not reliably enforce empty or exact type filters. SHOW uses supported defaults;
    // native results are post-filtered with the original JDBC types below.
    String[] validatedTableTypes = tableTypes != null ? tableTypes : DEFAULT_TABLE_TYPES;
    // Runtime treats catalog as a pattern and can return temporary views outside it. Preserve the
    // original JDBC catalog for exact native-result filtering before resolving catalog below.
    String requestedCatalog = catalog;

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
      return convertSynchronousMetadata(
          getResultSet(SQL, session, MetadataOperationType.GET_TABLES),
          session,
          source ->
              metadataResultSetBuilder.getTablesResult(
                  source,
                  requestedCatalog,
                  source.isThriftNativeMetadataResult() ? tableTypes : validatedTableTypes));
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

    // When catalog is null, CommandBuilder emits "SHOW COLUMNS IN ALL CATALOGS" so the server
    // enumerates every catalog in a single call (mirroring listSchemas/listTables). Older DBR
    // versions that don't support that syntax are handled by the parse-error fallback below.
    CommandBuilder commandBuilder =
        new CommandBuilder(catalog, session)
            .setSchemaPattern(schemaNamePattern)
            .setTablePattern(tableNamePattern)
            .setColumnPattern(columnNamePattern);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_COLUMNS);
    LOGGER.debug("SQL command to fetch columns: {}", SQL);
    try {
      return convertSynchronousMetadata(
          getResultSet(SQL, session, MetadataOperationType.GET_COLUMNS),
          session,
          metadataResultSetBuilder::getColumnsResult);
    } catch (SQLException e) {
      if (catalog == null && PARSE_SYNTAX_ERROR_SQL_STATE.equals(e.getSQLState())) {
        // Fallback for older DBR versions that don't support "SHOW COLUMNS IN ALL CATALOGS":
        // enumerate the catalogs and issue a per-catalog SHOW COLUMNS.
        LOGGER.debug("SQL command failed with syntax error. Fetching columns across all catalogs.");
        return fetchColumnsAcrossCatalogs(
            session, schemaNamePattern, tableNamePattern, columnNamePattern);
      } else if (isObjectNotFoundException(e)
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
    // Native GetFunctions ignores catalog filtering and returns an empty FUNCTION_CAT. Preserve
    // the original JDBC argument (including null) for Thrift's native-only column override; the
    // SHOW path is catalog-aware and uses the resolved catalog below.
    String requestedCatalog = catalog;

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
      String resolvedCatalog = catalog;
      return convertSynchronousMetadata(
          getResultSet(SQL, session, MetadataOperationType.GET_FUNCTIONS),
          session,
          source ->
              metadataResultSetBuilder.getFunctionsResult(
                  source,
                  source.isThriftNativeMetadataResult() ? requestedCatalog : resolvedCatalog));
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
    return convertSynchronousMetadata(
        getResultSet(SQL, params, session, MetadataOperationType.GET_PROCEDURES),
        session,
        metadataResultSetBuilder::getProceduresResult);
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
    return convertSynchronousMetadata(
        getResultSet(SQL, params, session, MetadataOperationType.GET_PROCEDURE_COLUMNS),
        session,
        metadataResultSetBuilder::getProcedureColumnsResult);
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
      return convertSynchronousMetadata(
          getResultSet(SQL, session, MetadataOperationType.GET_PRIMARY_KEYS),
          session,
          metadataResultSetBuilder::getPrimaryKeysResult);
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
      return convertSynchronousMetadata(
          getResultSet(SQL, session, MetadataOperationType.GET_CROSS_REFERENCE),
          session,
          metadataResultSetBuilder::getImportedKeysResult);
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
          "Invalid argument: tableName may not be null",
          SYNTAX_OR_ACCESS_VIOLATION_SQLSTATE,
          DatabricksDriverErrorCode.EXECUTE_STATEMENT_FAILED);
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
      return convertSynchronousMetadata(
          getResultSet(SQL, session, MetadataOperationType.GET_CROSS_REFERENCE),
          session,
          source ->
              metadataResultSetBuilder.getCrossReferenceKeysResult(
                  source, resolvedParentCatalog, resolvedParentSchema, resolvedParentTable));
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
    if (table == null) {
      LOGGER.debug("resolveKeyBasedParams: table is null, throwing");
      throw new DatabricksSQLException(
          "Invalid argument: tableName may not be null or empty",
          SYNTAX_OR_ACCESS_VIOLATION_SQLSTATE,
          DatabricksDriverErrorCode.EXECUTE_STATEMENT_FAILED);
    }
    if (table.isEmpty()) {
      LOGGER.debug("resolveKeyBasedParams: table is empty, throwing");
      throw new DatabricksSQLException(
          "Invalid argument: tableName may not be null or empty",
          OPERATION_ERROR_SQLSTATE,
          DatabricksDriverErrorCode.EXECUTE_STATEMENT_FAILED);
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
          OPERATION_ERROR_SQLSTATE,
          DatabricksDriverErrorCode.EXECUTE_STATEMENT_FAILED);
    }

    // Safety net: getCurrentCatalogAndSchema() returned null values
    if (catalog == null || schema == null) {
      LOGGER.debug(
          "resolveKeyBasedParams: could not resolve catalog or schema (catalog={}, schema={})",
          catalog,
          schema);
      throw new DatabricksSQLException(
          "Invalid argument: could not resolve catalog or schema",
          OPERATION_ERROR_SQLSTATE,
          DatabricksDriverErrorCode.EXECUTE_STATEMENT_FAILED);
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
    if (!params.isEmpty()
        && (metadataOperationType == MetadataOperationType.GET_PROCEDURES
            || metadataOperationType == MetadataOperationType.GET_PROCEDURE_COLUMNS)
        && ApcParameterCapabilities.isApcSea(session)
        && !ApcParameterCapabilities.supportsNativeParameters(session)) {
      String reason = ApcParameterCapabilities.downgradeReason(session);
      LOGGER.warn("APC procedure metadata parameter downgrade: {}", reason);
      TelemetryHelper.exportFailureLog(
          queryExecutionClient.getConnectionContext(),
          "APC_PARAMETER_DOWNGRADE",
          reason,
          TelemetryLogLevel.WARN);
      SQL = SQLInterpolator.interpolateApcSQL(SQL, params);
      params = java.util.Collections.emptyMap();
    }
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
