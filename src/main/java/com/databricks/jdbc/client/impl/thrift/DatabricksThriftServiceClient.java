package com.databricks.jdbc.client.impl.thrift;

import static com.databricks.jdbc.client.impl.helper.MetadataResultSetBuilder.*;
import static com.databricks.jdbc.client.impl.thrift.commons.DatabricksThriftHelper.*;
import static com.databricks.jdbc.client.impl.thrift.commons.DatabricksThriftHelper.byteBufferToString;
import static com.databricks.jdbc.client.impl.thrift.commons.DatabricksThriftHelper.verifySuccessStatus;
import static com.databricks.jdbc.commons.EnvironmentVariables.JDBC_THRIFT_VERSION;

import com.databricks.jdbc.client.DatabricksClient;
import com.databricks.jdbc.client.DatabricksMetadataClient;
import com.databricks.jdbc.client.StatementType;
import com.databricks.jdbc.client.impl.helper.MetadataResultSetBuilder;
import com.databricks.jdbc.client.impl.thrift.commons.DatabricksThriftAccessor;
import com.databricks.jdbc.client.impl.thrift.generated.*;
import com.databricks.jdbc.client.sqlexec.ExternalLink;
import com.databricks.jdbc.commons.CommandName;
import com.databricks.jdbc.commons.MetricsList;
import com.databricks.jdbc.core.*;
import com.databricks.jdbc.core.types.ComputeResource;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.jdbc.telemetry.DatabricksMetrics;
import com.google.common.annotations.VisibleForTesting;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DatabricksThriftServiceClient implements DatabricksClient, DatabricksMetadataClient {

  private static final Logger LOGGER = LogManager.getLogger(DatabricksThriftServiceClient.class);

  private final DatabricksThriftAccessor thriftAccessor;
  private final IDatabricksConnectionContext connectionContext;

  public DatabricksThriftServiceClient(IDatabricksConnectionContext connectionContext)
      throws DatabricksParsingException {
    this.connectionContext = connectionContext;
    this.thriftAccessor = new DatabricksThriftAccessor(connectionContext);
  }

  @VisibleForTesting
  DatabricksThriftServiceClient(
      DatabricksThriftAccessor thriftAccessor, IDatabricksConnectionContext connectionContext) {
    this.thriftAccessor = thriftAccessor;
    this.connectionContext = connectionContext;
  }

  private TNamespace getNamespace(String catalog, String schema) {
    return new TNamespace().setCatalogName(catalog).setSchemaName(schema);
  }

  @Override
  public ImmutableSessionInfo createSession(
      ComputeResource cluster, String catalog, String schema, Map<String, String> sessionConf)
      throws DatabricksSQLException {
    LOGGER.debug(
        "public Session createSession(Compute cluster = {}, String catalog = {}, String schema = {}, Map<String, String> sessionConf = {})",
        cluster.toString(),
        catalog,
        schema,
        sessionConf);
    long startTime = System.currentTimeMillis();
    TOpenSessionReq openSessionReq =
        new TOpenSessionReq()
            .setInitialNamespace(getNamespace(catalog, schema))
            .setConfiguration(sessionConf)
            .setCanUseMultipleCatalogs(true)
            .setClient_protocol_i64(JDBC_THRIFT_VERSION.getValue());
    TOpenSessionResp response =
        (TOpenSessionResp)
            thriftAccessor.getThriftResponse(openSessionReq, CommandName.OPEN_SESSION, null);
    verifySuccessStatus(response.status.getStatusCode(), response.toString());
    String sessionId = byteBufferToString(response.sessionHandle.getSessionId().guid);
    LOGGER.info("Session created with ID {}", sessionId);

    ImmutableSessionInfo sessionInfo =
        ImmutableSessionInfo.builder()
            .sessionId(sessionId)
            .sessionHandle(response.sessionHandle)
            .computeResource(cluster)
            .build();
    DatabricksMetrics.record(
        MetricsList.CREATE_SESSION_THRIFT.name(),
        (double) (System.currentTimeMillis() - startTime));
    return sessionInfo;
  }

  @Override
  public void deleteSession(IDatabricksSession session, ComputeResource cluster)
      throws DatabricksSQLException {
    long startTime = System.currentTimeMillis();
    LOGGER.debug(
        "public void deleteSession(Session session = {}, Compute cluster = {})",
        session.toString(),
        cluster.toString());
    TCloseSessionReq closeSessionReq =
        new TCloseSessionReq().setSessionHandle(session.getSessionInfo().sessionHandle());
    TCloseSessionResp response =
        (TCloseSessionResp)
            thriftAccessor.getThriftResponse(closeSessionReq, CommandName.CLOSE_SESSION, null);
    verifySuccessStatus(response.status.getStatusCode(), response.toString());
    DatabricksMetrics.record(
        MetricsList.DELETE_SESSION_THRIFT.name(),
        (double) (System.currentTimeMillis() - startTime));
  }

  @Override
  public DatabricksResultSet executeStatement(
      String sql,
      ComputeResource computeResource,
      Map<Integer, ImmutableSqlParameter> parameters,
      StatementType statementType,
      IDatabricksSession session,
      IDatabricksStatement parentStatement)
      throws SQLException {
    // Note that prepared statement is not supported by SEA/Thrift flow.
    long startTime = System.currentTimeMillis();
    LOGGER.debug(
        "public DatabricksResultSet executeStatement(String sql = {}, Compute cluster = {}, Map<Integer, ImmutableSqlParameter> parameters = {}, StatementType statementType = {}, IDatabricksSession session)",
        sql,
        computeResource.toString(),
        parameters.toString(),
        statementType);
    TExecuteStatementReq request =
        new TExecuteStatementReq()
            .setStatement(sql)
            .setSessionHandle(session.getSessionInfo().sessionHandle())
            .setCanReadArrowResult(this.connectionContext.shouldEnableArrow())
            .setCanDownloadResult(true)
            .setRunAsync(true);
    DatabricksResultSet resultSet =
        thriftAccessor.execute(request, parentStatement, session, statementType);
    DatabricksMetrics.record(
        MetricsList.EXECUTE_STATEMENT_THRIFT.name(),
        (double) (System.currentTimeMillis() - startTime));
    return resultSet;
  }

  @Override
  public void closeStatement(String statementId) throws DatabricksSQLException {
    LOGGER.debug(
        "public void closeStatement(String statementId = {}) for all purpose cluster", statementId);
    throw new DatabricksSQLFeatureNotImplementedException(
        "closeStatement for all purpose cluster not implemented");
  }

  @Override
  public void cancelStatement(String statementId) throws DatabricksSQLException {
    LOGGER.debug(
        "public void cancelStatement(String statementId = {}) for all purpose cluster",
        statementId);
    throw new DatabricksSQLFeatureNotImplementedException(
        "abortStatement for all purpose cluster not implemented");
  }

  @Override
  public Collection<ExternalLink> getResultChunks(String statementId, long chunkIndex)
      throws DatabricksSQLException {
    long startTime = System.currentTimeMillis();
    String context =
        String.format(
            "public Optional<ExternalLink> getResultChunk(String statementId = {%s}, long chunkIndex = {%s}) for all purpose cluster",
            statementId, chunkIndex);
    LOGGER.debug(context);
    THandleIdentifier handleIdentifier = new THandleIdentifier().setGuid(statementId.getBytes());
    TOperationHandle operationHandle =
        new TOperationHandle().setOperationId(handleIdentifier).setHasResultSet(false);
    TFetchResultsResp fetchResultsResp = thriftAccessor.getResultSetResp(operationHandle, context);
    if (chunkIndex < 0 || fetchResultsResp.getResults().getResultLinksSize() <= chunkIndex) {
      String error = String.format("Out of bounds error for chunkIndex. Context: %s", context);
      LOGGER.error(error);
      throw new DatabricksSQLException(error);
    }
    AtomicInteger index = new AtomicInteger(0);
    List<ExternalLink> externalLinks = new ArrayList<>();
    fetchResultsResp
        .getResults()
        .getResultLinks()
        .forEach(
            resultLink -> {
              externalLinks.add(createExternalLink(resultLink, index.getAndIncrement()));
            });
    DatabricksMetrics.record(
        MetricsList.GET_RESULT_CHUNK_THRIFT.name(),
        (double) (System.currentTimeMillis() - startTime));
    return externalLinks;
  }

  @Override
  public DatabricksResultSet listTypeInfo(IDatabricksSession session)
      throws DatabricksSQLException {
    long startTime = System.currentTimeMillis();
    LOGGER.debug("public ResultSet getTypeInfo()");
    TGetTypeInfoReq request =
        new TGetTypeInfoReq().setSessionHandle(session.getSessionInfo().sessionHandle());
    TFetchResultsResp response =
        (TFetchResultsResp)
            thriftAccessor.getThriftResponse(request, CommandName.LIST_TYPE_INFO, null);
    DatabricksResultSet resultSet =
        getTypeInfoResult(extractValues(response.getResults().getColumns()));
    DatabricksMetrics.record(
        MetricsList.LIST_TYPE_INFO_THRIFT.name(),
        (double) (System.currentTimeMillis() - startTime));
    return resultSet;
  }

  @Override
  public DatabricksResultSet listCatalogs(IDatabricksSession session) throws SQLException {
    long startTime = System.currentTimeMillis();
    String context =
        String.format(
            "Fetching catalogs for all purpose cluster. Session {%s}", session.toString());
    LOGGER.debug(context);
    TGetCatalogsReq request =
        new TGetCatalogsReq().setSessionHandle(session.getSessionInfo().sessionHandle());
    TFetchResultsResp response =
        (TFetchResultsResp)
            thriftAccessor.getThriftResponse(request, CommandName.LIST_CATALOGS, null);
    DatabricksResultSet resultSet =
        getCatalogsResult(extractValuesColumnar(response.getResults().getColumns()));
    DatabricksMetrics.record(
        MetricsList.LIST_CATALOGS_THRIFT.name(), (double) (System.currentTimeMillis() - startTime));
    return resultSet;
  }

  @Override
  public DatabricksResultSet listSchemas(
      IDatabricksSession session, String catalog, String schemaNamePattern) throws SQLException {
    long startTime = System.currentTimeMillis();
    String context =
        String.format(
            "Fetching schemas for all purpose cluster. Session {%s}, catalog {%s}, schemaNamePattern {%s}",
            session.toString(), catalog, schemaNamePattern);
    LOGGER.debug(context);
    TGetSchemasReq request =
        new TGetSchemasReq()
            .setSessionHandle(session.getSessionInfo().sessionHandle())
            .setCatalogName(catalog);
    if (schemaNamePattern != null) {
      request.setSchemaName(schemaNamePattern);
    }
    TFetchResultsResp response =
        (TFetchResultsResp)
            thriftAccessor.getThriftResponse(request, CommandName.LIST_SCHEMAS, null);
    DatabricksResultSet resultSet =
        getSchemasResult(extractValuesColumnar(response.getResults().getColumns()));
    DatabricksMetrics.record(
        MetricsList.LIST_SCHEMAS_THRIFT.name(), (double) (System.currentTimeMillis() - startTime));
    return resultSet;
  }

  @Override
  public DatabricksResultSet listTables(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String tableNamePattern,
      String[] tableTypes)
      throws SQLException {
    long startTime = System.currentTimeMillis();
    String context =
        String.format(
            "Fetching tables for all purpose cluster. Session {%s}, catalog {%s}, schemaNamePattern {%s}, tableNamePattern {%s}",
            session.toString(), catalog, schemaNamePattern, tableNamePattern);
    LOGGER.debug(context);
    TGetTablesReq request =
        new TGetTablesReq()
            .setSessionHandle(session.getSessionInfo().sessionHandle())
            .setCatalogName(catalog)
            .setSchemaName(schemaNamePattern)
            .setTableName(tableNamePattern);
    if (tableTypes != null) {
      request.setTableTypes(Arrays.asList(tableTypes));
    }
    TFetchResultsResp response =
        (TFetchResultsResp)
            thriftAccessor.getThriftResponse(request, CommandName.LIST_TABLES, null);
    DatabricksResultSet resultSet =
        getTablesResult(extractValuesColumnar(response.getResults().getColumns()));
    DatabricksMetrics.record(
        MetricsList.LIST_TABLES_THRIFT.name(), (double) (System.currentTimeMillis() - startTime));
    return resultSet;
  }

  @Override
  public DatabricksResultSet listTableTypes(IDatabricksSession session)
      throws DatabricksSQLException {
    long startTime = System.currentTimeMillis();
    LOGGER.debug("Fetching table types for all purpose cluster. Session {}", session.toString());
    return MetadataResultSetBuilder.getTableTypesResult();
  }

  @Override
  public DatabricksResultSet listColumns(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String tableNamePattern,
      String columnNamePattern)
      throws DatabricksSQLException {
    long startTime = System.currentTimeMillis();
    String context =
        String.format(
            "Fetching columns for all purpose cluster. Session {%s}, catalog {%s}, schemaNamePattern {%s}, tableNamePattern {%s}, columnNamePattern {%s}",
            session.toString(), catalog, schemaNamePattern, tableNamePattern, columnNamePattern);
    LOGGER.debug(context);
    TGetColumnsReq request =
        new TGetColumnsReq()
            .setSessionHandle(session.getSessionInfo().sessionHandle())
            .setCatalogName(catalog)
            .setSchemaName(schemaNamePattern)
            .setTableName(tableNamePattern)
            .setColumnName(columnNamePattern);
    TFetchResultsResp response =
        (TFetchResultsResp)
            thriftAccessor.getThriftResponse(request, CommandName.LIST_COLUMNS, null);
    DatabricksResultSet resultSet =
        getColumnsResult(extractValuesColumnar(response.getResults().getColumns()));
    DatabricksMetrics.record(
        MetricsList.LIST_COLUMNS_THRIFT.name(), (double) (System.currentTimeMillis() - startTime));
    return resultSet;
  }

  @Override
  public DatabricksResultSet listFunctions(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String functionNamePattern)
      throws DatabricksSQLException {
    long startTime = System.currentTimeMillis();
    String context =
        String.format(
            "Fetching functions for all purpose cluster. Session {%s}, catalog {%s}, schemaNamePattern {%s}, functionNamePattern {%s}.",
            session.toString(), catalog, schemaNamePattern, functionNamePattern);
    LOGGER.debug(context);
    TGetFunctionsReq request =
        new TGetFunctionsReq()
            .setSessionHandle(session.getSessionInfo().sessionHandle())
            .setCatalogName(catalog)
            .setSchemaName(schemaNamePattern)
            .setFunctionName(functionNamePattern);
    TFetchResultsResp response =
        (TFetchResultsResp)
            thriftAccessor.getThriftResponse(request, CommandName.LIST_FUNCTIONS, null);
    DatabricksResultSet resultSet =
        getFunctionsResult(extractValues(response.getResults().getColumns()));
    DatabricksMetrics.record(
        MetricsList.LIST_FUNCTIONS_THRIFT.name(),
        (double) (System.currentTimeMillis() - startTime));
    return resultSet;
  }

  @Override
  public DatabricksResultSet listPrimaryKeys(
      IDatabricksSession session, String catalog, String schema, String table) throws SQLException {
    long startTime = System.currentTimeMillis();
    String context =
        String.format(
            "Fetching primary keys for all purpose cluster. session {%s}, catalog {%s}, schema {%s}, table {%s}",
            session.toString(), catalog, schema, table);
    LOGGER.debug(context);
    TGetPrimaryKeysReq request =
        new TGetPrimaryKeysReq()
            .setSessionHandle(session.getSessionInfo().sessionHandle())
            .setCatalogName(catalog)
            .setSchemaName(schema)
            .setTableName(table);
    TFetchResultsResp response =
        (TFetchResultsResp)
            thriftAccessor.getThriftResponse(request, CommandName.LIST_PRIMARY_KEYS, null);
    DatabricksResultSet resultSet =
        getPrimaryKeysResult(extractValues(response.getResults().getColumns()));
    DatabricksMetrics.record(
        MetricsList.LIST_PRIMARY_KEYS_THRIFT.name(),
        (double) (System.currentTimeMillis() - startTime));
    return resultSet;
  }
}
