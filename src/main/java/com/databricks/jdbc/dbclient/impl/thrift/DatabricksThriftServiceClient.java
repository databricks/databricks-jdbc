package com.databricks.jdbc.dbclient.impl.thrift;

import static com.databricks.jdbc.common.EnvironmentVariables.JDBC_THRIFT_VERSION;
import static com.databricks.jdbc.common.util.DatabricksThriftUtil.*;
import static com.databricks.jdbc.dbclient.impl.common.MetadataResultSetBuilder.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.api.IDatabricksStatement;
import com.databricks.jdbc.api.impl.*;
import com.databricks.jdbc.common.CommandName;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.IDatabricksMetadataClient;
import com.databricks.jdbc.dbclient.impl.common.MetadataResultSetBuilder;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksSQLFeatureNotImplementedException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.*;
import com.databricks.jdbc.model.core.ExternalLink;
import com.google.common.annotations.VisibleForTesting;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DatabricksThriftServiceClient implements IDatabricksClient, IDatabricksMetadataClient {

  public static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksThriftServiceClient.class);
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

  @Override
  public void resetAccessToken(String newAccessToken) {
    this.thriftAccessor.resetAccessToken(newAccessToken);
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
      IDatabricksComputeResource cluster,
      String catalog,
      String schema,
      Map<String, String> sessionConf)
      throws DatabricksSQLException {
    LOGGER.debug(
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
    LOGGER.debug(String.format("Session created with ID {%s}", sessionId));
    return ImmutableSessionInfo.builder()
        .sessionId(sessionId)
        .sessionHandle(response.sessionHandle)
        .computeResource(cluster)
        .build();
  }

  @Override
  public void deleteSession(IDatabricksSession session, IDatabricksComputeResource cluster)
      throws DatabricksSQLException {
    LOGGER.debug(
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
      IDatabricksComputeResource computeResource,
      Map<Integer, ImmutableSqlParameter> parameters,
      StatementType statementType,
      IDatabricksSession session,
      IDatabricksStatement parentStatement)
      throws SQLException {
    // Note that prepared statement is not supported by SEA/Thrift flow.
    LOGGER.debug(
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
    LOGGER.debug(
        String.format(
            "public void closeStatement(String statementId = {%s}) for all purpose cluster",
            statementId));
    throw new DatabricksSQLFeatureNotImplementedException(
        "closeStatement for all purpose cluster not implemented");
  }

  @Override
  public void cancelStatement(String statementId) throws DatabricksSQLException {
    LOGGER.debug(
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
    return externalLinks;
  }

  @Override
  public DatabricksResultSet listTypeInfo(IDatabricksSession session)
      throws DatabricksSQLException {
    LOGGER.debug("public ResultSet getTypeInfo()");
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
    LOGGER.debug(context);
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
    return getTablesResult(catalog, extractValuesColumnar(response.getResults().getColumns()));
  }

  @Override
  public DatabricksResultSet listTableTypes(IDatabricksSession session) {
    LOGGER.debug(
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
    return getFunctionsResult(extractValuesColumnar(response.getResults().getColumns()));
  }

  @Override
  public DatabricksResultSet listPrimaryKeys(
      IDatabricksSession session, String catalog, String schema, String table) throws SQLException {
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
    return getPrimaryKeysResult(extractValues(response.getResults().getColumns()));
  }
}
