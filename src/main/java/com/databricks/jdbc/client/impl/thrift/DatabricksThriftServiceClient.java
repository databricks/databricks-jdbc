package com.databricks.jdbc.client.impl.thrift;

import static com.databricks.jdbc.client.impl.helper.MetadataResultSetBuilder.*;
import static com.databricks.jdbc.client.impl.thrift.commons.DatabricksThriftHelper.*;
import static com.databricks.jdbc.client.impl.thrift.commons.DatabricksThriftHelper.byteBufferToString;
import static com.databricks.jdbc.client.impl.thrift.commons.DatabricksThriftHelper.verifySuccessStatus;
import static com.databricks.jdbc.commons.EnvironmentVariables.JDBC_THRIFT_VERSION;

import com.databricks.jdbc.client.DatabricksClient;
import com.databricks.jdbc.client.DatabricksMetadataClient;
import com.databricks.jdbc.client.StatementType;
import com.databricks.jdbc.client.impl.thrift.commons.DatabricksThriftAccessor;
import com.databricks.jdbc.client.impl.thrift.generated.*;
import com.databricks.jdbc.client.sqlexec.ExternalLink;
import com.databricks.jdbc.commons.CommandName;
import com.databricks.jdbc.core.*;
import com.databricks.jdbc.core.types.ComputeResource;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
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
    return ImmutableSessionInfo.builder()
        .sessionId(sessionId)
        .sessionHandle(response.sessionHandle)
        .computeResource(cluster)
        .build();
  }

  @Override
  public void deleteSession(IDatabricksSession session, ComputeResource cluster)
      throws DatabricksSQLException {
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
            .setCanDownloadResult(true);
    return thriftAccessor.execute(request, parentStatement, session, statementType);
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
    return getTablesResult(extractValuesColumnar(response.getResults().getColumns()));
  }

  @Override
  public DatabricksResultSet listTableTypes(IDatabricksSession session)
      throws DatabricksSQLException {
    LOGGER.debug("Fetching table types for all purpose cluster. Session {}", session.toString());
    TGetTableTypesReq request =
        new TGetTableTypesReq().setSessionHandle(session.getSessionInfo().sessionHandle());
    TFetchResultsResp response =
        (TFetchResultsResp)
            thriftAccessor.getThriftResponse(request, CommandName.LIST_TABLE_TYPES, null);
    return getTableTypesResult(extractValues(response.getResults().getColumns()));
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
            .setSessionHandle(session.getSessionInfo().sessionHandle())
            .setCatalogName(catalog)
            .setSchemaName(schemaNamePattern)
            .setTableName(tableNamePattern)
            .setColumnName(columnNamePattern);
    TFetchResultsResp response =
        (TFetchResultsResp)
            thriftAccessor.getThriftResponse(request, CommandName.LIST_COLUMNS, null);
    return getColumnsResult(extractValues(response.getResults().getColumns()));
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
    return getFunctionsResult(extractValues(response.getResults().getColumns()));
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
