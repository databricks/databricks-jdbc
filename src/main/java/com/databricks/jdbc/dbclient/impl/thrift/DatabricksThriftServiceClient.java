package com.databricks.jdbc.dbclient.impl.thrift;

import static com.databricks.jdbc.common.EnvironmentVariables.JDBC_THRIFT_VERSION;
import static com.databricks.jdbc.common.util.DatabricksThriftUtil.*;
import static com.databricks.jdbc.dbclient.impl.common.MetadataResultSetBuilder.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.api.impl.*;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.util.DriverUtil;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.IDatabricksMetadataClient;
import com.databricks.jdbc.dbclient.impl.common.MetadataResultSetBuilder;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.*;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.google.common.annotations.VisibleForTesting;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DatabricksThriftServiceClient implements IDatabricksClient, IDatabricksMetadataClient {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksThriftServiceClient.class);
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

  @Override
  public IDatabricksConnectionContext getConnectionContext() {
    return connectionContext;
  }

  @Override
  public void resetAccessToken(String newAccessToken) {
    ((DatabricksHttpTTransport) thriftAccessor.getThriftClient().getInputProtocol().getTransport())
        .resetAccessToken(newAccessToken);
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
    TOpenSessionResp response = (TOpenSessionResp) thriftAccessor.getThriftResponse(openSessionReq);
    verifySuccessStatus(response.status, response.toString());

    TProtocolVersion serverProtocol = response.getServerProtocolVersion();
    if (serverProtocol.getValue() <= TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10.getValue()) {
      throw new DatabricksSQLException(
          "Attempting to connect to a non Databricks compute using the Databricks driver.",
          DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
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
  public void deleteSession(ImmutableSessionInfo sessionInfo) throws DatabricksSQLException {
    LOGGER.debug(
        String.format(
            "public void deleteSession(Session session = {%s}))", sessionInfo.toString()));
    TCloseSessionReq closeSessionReq =
        new TCloseSessionReq().setSessionHandle(sessionInfo.sessionHandle());
    TCloseSessionResp response =
        (TCloseSessionResp) thriftAccessor.getThriftResponse(closeSessionReq);
    verifySuccessStatus(response.status, response.toString());
  }

  @Override
  public DatabricksResultSet executeStatement(
      String sql,
      IDatabricksComputeResource computeResource,
      Map<Integer, ImmutableSqlParameter> parameters,
      StatementType statementType,
      IDatabricksSession session,
      IDatabricksStatementInternal parentStatement)
      throws SQLException {
    // Note that prepared statement is not supported by SEA/Thrift flow.
    LOGGER.debug(
        String.format(
            "public DatabricksResultSet executeStatement(String sql = {%s}, Compute cluster = {%s}, Map<Integer, ImmutableSqlParameter> parameters = {%s}, StatementType statementType = {%s}, IDatabricksSession session)",
            sql, computeResource.toString(), parameters.toString(), statementType));
    TExecuteStatementReq request =
        new TExecuteStatementReq()
            .setStatement(sql)
            .setSessionHandle(Objects.requireNonNull(session.getSessionInfo()).sessionHandle())
            .setCanDecompressLZ4Result(true)
            .setQueryTimeout(parentStatement.getStatement().getQueryTimeout())
            .setCanReadArrowResult(this.connectionContext.shouldEnableArrow())
            .setCanDownloadResult(true);
    if (!DriverUtil.isRunningAgainstFake()) {
      // run queries in async mode if not using fake services
      request.setRunAsync(true);
    }
    return thriftAccessor.execute(request, parentStatement, session, statementType);
  }

  @Override
  public DatabricksResultSet executeStatementAsync(
      String sql,
      IDatabricksComputeResource computeResource,
      Map<Integer, ImmutableSqlParameter> parameters,
      IDatabricksSession session,
      IDatabricksStatementInternal parentStatement)
      throws SQLException {
    LOGGER.debug(
        String.format(
            "public DatabricksResultSet executeStatementAsync(String sql = {%s}, Compute cluster = {%s}, Map<Integer, ImmutableSqlParameter> parameters = {%s})",
            sql, computeResource.toString(), parameters.toString()));
    TExecuteStatementReq request =
        new TExecuteStatementReq()
            .setStatement(sql)
            .setQueryTimeout(parentStatement.getStatement().getQueryTimeout())
            .setSessionHandle(Objects.requireNonNull(session.getSessionInfo()).sessionHandle())
            .setCanDecompressLZ4Result(true)
            .setCanReadArrowResult(this.connectionContext.shouldEnableArrow())
            .setRunAsync(true)
            .setCanDownloadResult(true);
    return thriftAccessor.executeAsync(request, parentStatement, session, StatementType.SQL);
  }

  @Override
  public void closeStatement(StatementId statementId) throws DatabricksSQLException {
    LOGGER.debug(
        String.format(
            "public void closeStatement(String statementId = {%s}) for all purpose cluster",
            statementId));
    TCloseOperationReq request =
        new TCloseOperationReq().setOperationHandle(getOperationHandle(statementId));
    TCloseOperationResp resp = thriftAccessor.closeOperation(request);
    LOGGER.debug("Statement {%s} closed with status {%s}", statementId, resp.getStatus());
  }

  @Override
  public void cancelStatement(StatementId statementId) throws DatabricksSQLException {
    LOGGER.debug(
        String.format(
            "public void cancelStatement(String statementId = {%s}) for all purpose cluster",
            statementId));
    TCancelOperationReq request =
        new TCancelOperationReq().setOperationHandle(getOperationHandle(statementId));
    TCancelOperationResp resp = thriftAccessor.cancelOperation(request);
    LOGGER.debug("Statement {%s} cancelled with status {%s}", statementId, resp.getStatus());
  }

  @Override
  public DatabricksResultSet getStatementResult(
      StatementId statementId,
      IDatabricksSession session,
      IDatabricksStatementInternal parentStatement)
      throws SQLException {
    LOGGER.debug(
        String.format(
            "public DatabricksResultSet getStatementResult(String statementId = {%s}) for all purpose cluster",
            statementId));
    return thriftAccessor.getStatementResult(
        getOperationHandle(statementId), parentStatement, session);
  }

  @Override
  public Collection<ExternalLink> getResultChunks(StatementId statementId, long chunkIndex)
      throws DatabricksSQLException {
    String context =
        String.format(
            "public Optional<ExternalLink> getResultChunk(String statementId = {%s}, long chunkIndex = {%s}) for all purpose cluster",
            statementId, chunkIndex);
    LOGGER.debug(context);
    TFetchResultsResp fetchResultsResp;
    List<ExternalLink> externalLinks = new ArrayList<>();
    AtomicInteger index = new AtomicInteger(0);
    do {
      fetchResultsResp = thriftAccessor.getResultSetResp(getOperationHandle(statementId), context);
      fetchResultsResp
          .getResults()
          .getResultLinks()
          .forEach(
              resultLink ->
                  externalLinks.add(createExternalLink(resultLink, index.getAndIncrement())));
    } while (fetchResultsResp.hasMoreRows);
    if (chunkIndex < 0 || externalLinks.size() <= chunkIndex) {
      String error = String.format("Out of bounds error for chunkIndex. Context: %s", context);
      LOGGER.error(error);
      throw new DatabricksSQLException(error, DatabricksDriverErrorCode.INVALID_STATE);
    }
    return externalLinks;
  }

  @Override
  public DatabricksResultSet listTypeInfo(IDatabricksSession session)
      throws DatabricksSQLException {
    LOGGER.debug("public ResultSet getTypeInfo()");
    TGetTypeInfoReq request =
        new TGetTypeInfoReq()
            .setSessionHandle(Objects.requireNonNull(session.getSessionInfo()).sessionHandle())
            .setRunAsync(true);
    TFetchResultsResp response = (TFetchResultsResp) thriftAccessor.getThriftResponse(request);
    return getTypeInfoResult(extractValuesColumnar(response.getResults().getColumns()));
  }

  @Override
  public DatabricksResultSet listCatalogs(IDatabricksSession session) throws SQLException {
    String context =
        String.format(
            "Fetching catalogs for all purpose cluster. Session {%s}", session.toString());
    LOGGER.debug(context);
    TGetCatalogsReq request =
        new TGetCatalogsReq()
            .setSessionHandle(Objects.requireNonNull(session.getSessionInfo()).sessionHandle())
            .setRunAsync(true);
    TFetchResultsResp response = (TFetchResultsResp) thriftAccessor.getThriftResponse(request);
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
            .setSessionHandle(Objects.requireNonNull(session.getSessionInfo()).sessionHandle())
            .setCatalogName(catalog)
            .setRunAsync(true);
    if (schemaNamePattern != null) {
      request.setSchemaName(schemaNamePattern);
    }
    TFetchResultsResp response = (TFetchResultsResp) thriftAccessor.getThriftResponse(request);
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
            .setSessionHandle(Objects.requireNonNull(session.getSessionInfo()).sessionHandle())
            .setCatalogName(catalog)
            .setSchemaName(schemaNamePattern)
            .setTableName(tableNamePattern)
            .setRunAsync(true);
    if (tableTypes != null) {
      request.setTableTypes(Arrays.asList(tableTypes));
    }
    TFetchResultsResp response = (TFetchResultsResp) thriftAccessor.getThriftResponse(request);
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
    String context =
        String.format(
            "Fetching columns for all purpose cluster. Session {%s}, catalog {%s}, schemaNamePattern {%s}, tableNamePattern {%s}, columnNamePattern {%s}",
            session.toString(), catalog, schemaNamePattern, tableNamePattern, columnNamePattern);
    LOGGER.debug(context);
    TGetColumnsReq request =
        new TGetColumnsReq()
            .setSessionHandle(Objects.requireNonNull(session.getSessionInfo()).sessionHandle())
            .setCatalogName(catalog)
            .setSchemaName(schemaNamePattern)
            .setTableName(tableNamePattern)
            .setColumnName(columnNamePattern)
            .setRunAsync(true);
    TFetchResultsResp response = (TFetchResultsResp) thriftAccessor.getThriftResponse(request);
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
            .setSessionHandle(Objects.requireNonNull(session.getSessionInfo()).sessionHandle())
            .setCatalogName(catalog)
            .setSchemaName(schemaNamePattern)
            .setFunctionName(functionNamePattern)
            .setRunAsync(true);
    TFetchResultsResp response = (TFetchResultsResp) thriftAccessor.getThriftResponse(request);
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
            .setSessionHandle(Objects.requireNonNull(session.getSessionInfo()).sessionHandle())
            .setCatalogName(catalog)
            .setSchemaName(schema)
            .setTableName(table)
            .setRunAsync(true);
    TFetchResultsResp response = (TFetchResultsResp) thriftAccessor.getThriftResponse(request);
    return getPrimaryKeysResult(extractValues(response.getResults().getColumns()));
  }

  public TFetchResultsResp getMoreResults(IDatabricksStatementInternal parentStatement)
      throws DatabricksSQLException {
    return thriftAccessor.getMoreResults(parentStatement);
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
}
