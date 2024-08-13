package com.databricks.jdbc.client.impl.thrift;

import static com.databricks.jdbc.client.impl.helper.MetadataResultSetBuilder.*;
import static com.databricks.jdbc.client.impl.thrift.commons.DatabricksThriftHelper.*;
import static com.databricks.jdbc.commons.EnvironmentVariables.JDBC_THRIFT_VERSION;

import com.databricks.jdbc.client.DatabricksClient;
import com.databricks.jdbc.client.DatabricksMetadataClient;
import com.databricks.jdbc.client.StatementType;
import com.databricks.jdbc.client.impl.helper.MetadataResultSetBuilder;
import com.databricks.jdbc.client.impl.thrift.commons.DatabricksThriftAccessor;
import com.databricks.jdbc.client.impl.thrift.generated.*;
import com.databricks.jdbc.client.sqlexec.ExternalLink;
import com.databricks.jdbc.commons.CommandName;
import com.databricks.jdbc.commons.LogLevel;
import com.databricks.jdbc.commons.util.LoggingUtil;
import com.databricks.jdbc.core.*;
import com.databricks.jdbc.core.types.ComputeResource;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.google.common.annotations.VisibleForTesting;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DatabricksThriftServiceClient implements DatabricksClient, DatabricksMetadataClient {

  private final DatabricksThriftAccessor thriftAccessor;
  private final IDatabricksConnectionContext connectionContext;

  public DatabricksThriftServiceClient(IDatabricksConnectionContext connectionContext)
      throws DatabricksParsingException {
    this.connectionContext = connectionContext;
    this.thriftAccessor = new DatabricksThriftAccessor(connectionContext);
  }

  @Override
  public IDatabricksConnectionContext getConnectionContext() {
    return connectionContext;
  }

  @VisibleForTesting
  DatabricksThriftServiceClient(
      DatabricksThriftAccessor thriftAccessor, IDatabricksConnectionContext connectionContext) {
    this.thriftAccessor = thriftAccessor;
    this.connectionContext = connectionContext;
  }

  private TNamespace getNamespace(String catalog, String schema) {
    final TNamespace namespace = new TNamespace();
    if (catalog != null) {
      namespace.setCatalogName(catalog);
    }
    if (schema != null) {
      namespace.setSchemaName(schema);
    }

    return namespace;
  }

  @Override
  public ImmutableSessionInfo createSession(
      ComputeResource cluster, String catalog, String schema, Map<String, String> sessionConf)
      throws DatabricksSQLException {
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format(
            "public Session createSession(Compute cluster = {%s}, String catalog = {%s}, String schema = {%s}, Map<String, String> sessionConf = {%s})",
            cluster.toString(), catalog, schema, sessionConf));
    TOpenSessionReq openSessionReq =
        new TOpenSessionReq()
            .setConfiguration(sessionConf)
            .setCanUseMultipleCatalogs(true)
            .setClient_protocol_i64(JDBC_THRIFT_VERSION.getValue());
    if (catalog != null || schema != null) {
      openSessionReq.setInitialNamespace(getNamespace(catalog, schema));
    }
    TOpenSessionResp response =
        (TOpenSessionResp)
            thriftAccessor.getThriftResponse(openSessionReq, CommandName.OPEN_SESSION, null);
    verifySuccessStatus(response.status.getStatusCode(), response.toString());

    TProtocolVersion serverProtocol = response.getServerProtocolVersion();
    if (serverProtocol.getValue() <= TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10.getValue()) {
      throw new DatabricksSQLException(
          "Attempting to connect to a non Databricks cluster using the Databricks driver.");
    }

    String sessionId = byteBufferToString(response.sessionHandle.getSessionId().guid);
    LoggingUtil.log(LogLevel.DEBUG, String.format("Session created with ID {%s}", sessionId));
    return ImmutableSessionInfo.builder()
        .sessionId(sessionId)
        .sessionHandle(response.sessionHandle)
        .computeResource(cluster)
        .build();
  }

  @Override
  public void deleteSession(IDatabricksSession session, ComputeResource cluster)
      throws DatabricksSQLException {
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format(
            "public void deleteSession(Session session = {%s}, Compute cluster = {%s})",
            session.toString(), cluster.toString()));
    TCloseSessionReq closeSessionReq =
        new TCloseSessionReq().setSessionHandle(session.getSessionInfo().sessionHandle());
    TCloseSessionResp response =
        (TCloseSessionResp)
            thriftAccessor.getThriftResponse(closeSessionReq, CommandName.CLOSE_SESSION, null);
    verifySuccessStatus(response.status.getStatusCode(), response.toString());
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
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format(
            "public DatabricksResultSet executeStatement(String sql = {%s}, Compute cluster = {%s}, Map<Integer, ImmutableSqlParameter> parameters = {%s}, StatementType statementType = {%s}, IDatabricksSession session)",
            sql, computeResource.toString(), parameters.toString(), statementType));
    TExecuteStatementReq request =
        new TExecuteStatementReq()
            .setStatement(sql)
            .setSessionHandle(session.getSessionInfo().sessionHandle())
            .setCanReadArrowResult(this.connectionContext.shouldEnableArrow())
            .setCanDownloadResult(true);
    return thriftAccessor.execute(request, parentStatement, session, statementType);
  }

  @Override
  public void closeStatement(String statementId) throws DatabricksSQLException {
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format(
            "public void closeStatement(String statementId = {%s}) for all purpose cluster",
            statementId));
    throw new DatabricksSQLFeatureNotImplementedException(
        "closeStatement for all purpose cluster not implemented");
  }

  @Override
  public void cancelStatement(String statementId) throws DatabricksSQLException {
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format(
            "public void cancelStatement(String statementId = {%s}) for all purpose cluster",
            statementId));
    throw new DatabricksSQLFeatureNotImplementedException(
        "abortStatement for all purpose cluster not implemented");
  }

  @Override
  public Collection<ExternalLink> getResultChunks(String statementId, long chunkIndex)
      throws DatabricksSQLException {
    String context =
        String.format(
            "public Optional<ExternalLink> getResultChunk(String statementId = {%s}, long chunkIndex = {%s}) for all purpose cluster",
            statementId, chunkIndex);
    LoggingUtil.log(LogLevel.DEBUG, context);
    THandleIdentifier handleIdentifier = new THandleIdentifier().setGuid(statementId.getBytes());
    TOperationHandle operationHandle =
        new TOperationHandle().setOperationId(handleIdentifier).setHasResultSet(false);
    TFetchResultsResp fetchResultsResp = thriftAccessor.getResultSetResp(operationHandle, context);
    if (chunkIndex < 0 || fetchResultsResp.getResults().getResultLinksSize() <= chunkIndex) {
      String error = String.format("Out of bounds error for chunkIndex. Context: %s", context);
      LoggingUtil.log(LogLevel.ERROR, error);
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
    return externalLinks;
  }

  @Override
  public DatabricksResultSet listTypeInfo(IDatabricksSession session)
      throws DatabricksSQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public ResultSet getTypeInfo()");
    TGetTypeInfoReq request =
        new TGetTypeInfoReq().setSessionHandle(session.getSessionInfo().sessionHandle());
    TFetchResultsResp response =
        (TFetchResultsResp)
            thriftAccessor.getThriftResponse(request, CommandName.LIST_TYPE_INFO, null);
    return getTypeInfoResult(extractValues(response.getResults().getColumns()));
  }

  @Override
  public DatabricksResultSet listCatalogs(IDatabricksSession session) throws SQLException {
    String context =
        String.format(
            "Fetching catalogs for all purpose cluster. Session {%s}", session.toString());
    LoggingUtil.log(LogLevel.DEBUG, context);
    TGetCatalogsReq request =
        new TGetCatalogsReq().setSessionHandle(session.getSessionInfo().sessionHandle());
    TFetchResultsResp response =
        (TFetchResultsResp)
            thriftAccessor.getThriftResponse(request, CommandName.LIST_CATALOGS, null);
    return getCatalogsResult(extractValuesColumnar(response.getResults().getColumns()));
  }

  @Override
  public DatabricksResultSet listSchemas(
      IDatabricksSession session, String catalog, String schemaNamePattern) throws SQLException {
    String context =
        String.format(
            "Fetching schemas for all purpose cluster. Session {%s}, catalog {%s}, schemaNamePattern {%s}",
            session.toString(), catalog, schemaNamePattern);
    LoggingUtil.log(LogLevel.DEBUG, context);
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
    return getSchemasResult(extractValuesColumnar(response.getResults().getColumns()));
  }

  @Override
  public DatabricksResultSet listTables(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String tableNamePattern,
      String[] tableTypes)
      throws SQLException {
    String context =
        String.format(
            "Fetching tables for all purpose cluster. Session {%s}, catalog {%s}, schemaNamePattern {%s}, tableNamePattern {%s}",
            session.toString(), catalog, schemaNamePattern, tableNamePattern);
    LoggingUtil.log(LogLevel.DEBUG, context);
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
    return getTablesResult(extractValuesColumnar(response.getResults().getColumns()));
  }

  @Override
  public DatabricksResultSet listTableTypes(IDatabricksSession session) {
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format(
            "Fetching table types for all purpose cluster. Session {%s}", session.toString()));
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
    System.out.println("public ResultSet getColumns()");
    String context =
        String.format(
            "Fetching columns for all purpose cluster. Session {%s}, catalog {%s}, schemaNamePattern {%s}, tableNamePattern {%s}, columnNamePattern {%s}",
            session.toString(), catalog, schemaNamePattern, tableNamePattern, columnNamePattern);
    LoggingUtil.log(LogLevel.DEBUG, context);
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
    return getColumnsResult(extractValuesColumnar(response.getResults().getColumns()));
  }

  @Override
  public DatabricksResultSet listFunctions(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String functionNamePattern)
      throws DatabricksSQLException {
    String context =
        String.format(
            "Fetching functions for all purpose cluster. Session {%s}, catalog {%s}, schemaNamePattern {%s}, functionNamePattern {%s}.",
            session.toString(), catalog, schemaNamePattern, functionNamePattern);
    LoggingUtil.log(LogLevel.DEBUG, context);
    TGetFunctionsReq request =
        new TGetFunctionsReq()
            .setSessionHandle(session.getSessionInfo().sessionHandle())
            .setCatalogName(catalog)
            .setSchemaName(schemaNamePattern)
            .setFunctionName(functionNamePattern);
    TFetchResultsResp response =
        (TFetchResultsResp)
            thriftAccessor.getThriftResponse(request, CommandName.LIST_FUNCTIONS, null);
    return getFunctionsResult(extractValues(response.getResults().getColumns()));
  }

  @Override
  public DatabricksResultSet listPrimaryKeys(
      IDatabricksSession session, String catalog, String schema, String table) throws SQLException {
    String context =
        String.format(
            "Fetching primary keys for all purpose cluster. session {%s}, catalog {%s}, schema {%s}, table {%s}",
            session.toString(), catalog, schema, table);
    LoggingUtil.log(LogLevel.DEBUG, context);
    TGetPrimaryKeysReq request =
        new TGetPrimaryKeysReq()
            .setSessionHandle(session.getSessionInfo().sessionHandle())
            .setCatalogName(catalog)
            .setSchemaName(schema)
            .setTableName(table);
    TFetchResultsResp response =
        (TFetchResultsResp)
            thriftAccessor.getThriftResponse(request, CommandName.LIST_PRIMARY_KEYS, null);
    return getPrimaryKeysResult(extractValues(response.getResults().getColumns()));
  }
}
