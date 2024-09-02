package com.databricks.jdbc.dbclient.impl.sqlexec;

import static com.databricks.jdbc.common.EnvironmentVariables.DEFAULT_ROW_LIMIT;
import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.api.IDatabricksStatement;
import com.databricks.jdbc.api.impl.*;
import com.databricks.jdbc.auth.ClientUtils;
import com.databricks.jdbc.auth.OAuthAuthenticator;
import com.databricks.jdbc.common.*;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.common.util.LoggingUtil;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksTimeoutException;
import com.databricks.jdbc.model.client.sqlexec.*;
import com.databricks.jdbc.model.client.sqlexec.ExecuteStatementRequest;
import com.databricks.jdbc.model.client.sqlexec.ExecuteStatementResponse;
import com.databricks.jdbc.model.client.sqlexec.GetStatementResponse;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.sql.*;
import com.google.common.annotations.VisibleForTesting;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Implementation of IDatabricksClient interface using Databricks Java SDK. */
public class DatabricksSdkClient implements IDatabricksClient {
  private static final String SYNC_TIMEOUT_VALUE = "10s";
  private final IDatabricksConnectionContext connectionContext;
  private final DatabricksConfig databricksConfig;
  private final WorkspaceClient workspaceClient;

  @Override
  public IDatabricksConnectionContext getConnectionContext() {
    return connectionContext;
  }

  private static Map<String, String> getHeaders() {
    return Map.of(
        "Accept", "application/json",
        "Content-Type", "application/json");
  }

  public DatabricksSdkClient(IDatabricksConnectionContext connectionContext)
      throws DatabricksParsingException {
    this.connectionContext = connectionContext;
    this.databricksConfig = ClientUtils.generateDatabricksConfig(connectionContext);
    OAuthAuthenticator authenticator = new OAuthAuthenticator(connectionContext);
    this.workspaceClient = authenticator.getWorkspaceClient(this.databricksConfig);
  }

  @VisibleForTesting
  public DatabricksSdkClient(
      IDatabricksConnectionContext connectionContext,
      StatementExecutionService statementExecutionService,
      ApiClient apiClient)
      throws DatabricksParsingException {
    this.connectionContext = connectionContext;
    this.databricksConfig = ClientUtils.generateDatabricksConfig(connectionContext);

    this.workspaceClient =
        new WorkspaceClient(true /* mock */, apiClient)
            .withStatementExecutionImpl(statementExecutionService);
  }

  @Override
  public ImmutableSessionInfo createSession(
      IDatabricksComputeResource warehouse,
      String catalog,
      String schema,
      Map<String, String> sessionConf) {
    // TODO: [PECO-1460] Handle sessionConf in public session API
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format(
            "public Session createSession(String warehouseId = {%s}, String catalog = {%s}, String schema = {%s}, Map<String, String> sessionConf = {%s})",
            ((Warehouse) warehouse).getWarehouseId(), catalog, schema, sessionConf));
    CreateSessionRequest request =
        new CreateSessionRequest().setWarehouseId(((Warehouse) warehouse).getWarehouseId());
    if (catalog != null) {
      request.setCatalog(catalog);
    }
    if (schema != null) {
      request.setSchema(schema);
    }
    if (sessionConf != null && !sessionConf.isEmpty()) {
      request.setSessionConfigs(sessionConf);
    }
    CreateSessionResponse createSessionResponse =
        workspaceClient
            .apiClient()
            .POST(SESSION_PATH, request, CreateSessionResponse.class, getHeaders());

    return ImmutableSessionInfo.builder()
        .computeResource(warehouse)
        .sessionId(createSessionResponse.getSessionId())
        .build();
  }

  @Override
  public void deleteSession(IDatabricksSession session, IDatabricksComputeResource warehouse)
      throws DatabricksSQLException {
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format(
            "public void deleteSession(String sessionId = {%s})", session.getSessionId()));
    DeleteSessionRequest request =
        new DeleteSessionRequest()
            .setSessionId(session.getSessionId())
            .setWarehouseId(((Warehouse) warehouse).getWarehouseId());
    String path = String.format(DELETE_SESSION_PATH_WITH_ID, request.getSessionId());
    Map<String, String> headers = new HashMap<>();
    workspaceClient.apiClient().DELETE(path, request, Void.class, headers);
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
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format(
            "public DatabricksResultSet executeStatement(String sql = {%s}, compute resource = {%s}, Map<Integer, ImmutableSqlParameter> parameters, StatementType statementType = {%s}, IDatabricksSession session)",
            sql, computeResource.toString(), statementType),
        this.getClass().getName());
    long pollCount = 0;
    long executionStartTime = Instant.now().toEpochMilli();
    ExecuteStatementRequest request =
        getRequest(
            statementType,
            sql,
            ((Warehouse) computeResource).getWarehouseId(),
            session,
            parameters,
            parentStatement);
    ExecuteStatementResponse response =
        workspaceClient
            .apiClient()
            .POST(STATEMENT_PATH, request, ExecuteStatementResponse.class, getHeaders());

    String statementId = response.getStatementId();
    if (parentStatement != null) {
      parentStatement.setStatementId(statementId);
    }
    StatementState responseState = response.getStatus().getState();
    while (responseState == StatementState.PENDING || responseState == StatementState.RUNNING) {
      if (pollCount > 0) { // First poll happens without a delay
        try {
          Thread.sleep(this.connectionContext.getAsyncExecPollInterval());
        } catch (InterruptedException e) {
          throw new DatabricksTimeoutException("Thread interrupted due to statement timeout");
        }
      }
      String getStatusPath = String.format(STATEMENT_PATH_WITH_ID, statementId);
      response =
          wrapGetStatementResponse(
              workspaceClient
                  .apiClient()
                  .GET(getStatusPath, request, GetStatementResponse.class, getHeaders()));
      responseState = response.getStatus().getState();
      pollCount++;
    }
    long executionEndTime = Instant.now().toEpochMilli();
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format(
            "Executed sql [%s] with status [%s], total time taken [%s] and pollCount [%s]",
            sql, responseState, (executionEndTime - executionStartTime), pollCount));
    if (responseState != StatementState.SUCCEEDED) {
      handleFailedExecution(response, statementId, sql);
    }
    return new DatabricksResultSet(
        response.getStatus(),
        statementId,
        response.getResult(),
        response.getManifest(),
        statementType,
        session,
        parentStatement);
  }

  private boolean useCloudFetchForResult(StatementType statementType) {
    return this.connectionContext.shouldEnableArrow()
        && (statementType == StatementType.QUERY || statementType == StatementType.SQL);
  }

  @Override
  public void closeStatement(String statementId) {
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format("public void closeStatement(String statementId = {%s})", statementId));
    CloseStatementRequest request = new CloseStatementRequest().setStatementId(statementId);
    String path = String.format(STATEMENT_PATH_WITH_ID, request.getStatementId());
    workspaceClient.apiClient().DELETE(path, request, Void.class, getHeaders());
  }

  @Override
  public void cancelStatement(String statementId) {
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format("public void cancelStatement(String statementId = {%s})", statementId));
    CancelStatementRequest request = new CancelStatementRequest().setStatementId(statementId);
    String path = String.format(CANCEL_STATEMENT_PATH_WITH_ID, request.getStatementId());
    workspaceClient.apiClient().POST(path, request, Void.class, getHeaders());
  }

  @Override
  public Collection<ExternalLink> getResultChunks(String statementId, long chunkIndex) {
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format(
            "public Optional<ExternalLink> getResultChunk(String statementId = {%s}, long chunkIndex = {%s})",
            statementId, chunkIndex));
    GetStatementResultChunkNRequest request =
        new GetStatementResultChunkNRequest().setStatementId(statementId).setChunkIndex(chunkIndex);
    String path = String.format(RESULT_CHUNK_PATH, statementId, chunkIndex);
    return workspaceClient
        .apiClient()
        .GET(path, request, ResultData.class, getHeaders())
        .getExternalLinks();
  }

  private ExecuteStatementRequest getRequest(
      StatementType statementType,
      String sql,
      String warehouseId,
      IDatabricksSession session,
      Map<Integer, ImmutableSqlParameter> parameters,
      IDatabricksStatement parentStatement)
      throws SQLException {
    Format format = useCloudFetchForResult(statementType) ? Format.ARROW_STREAM : Format.JSON_ARRAY;
    Disposition disposition =
        useCloudFetchForResult(statementType) ? Disposition.EXTERNAL_LINKS : Disposition.INLINE;
    long maxRows = (parentStatement == null) ? DEFAULT_ROW_LIMIT : parentStatement.getMaxRows();

    List<StatementParameterListItem> collect =
        parameters.values().stream().map(this::mapToParameterListItem).collect(Collectors.toList());
    ExecuteStatementRequest request =
        new ExecuteStatementRequest()
            .setSessionId(session.getSessionId())
            .setStatement(sql)
            .setWarehouseId(warehouseId)
            .setDisposition(disposition)
            .setFormat(format)
            .setCompressionType(session.getCompressionType())
            .setWaitTimeout(SYNC_TIMEOUT_VALUE)
            .setOnWaitTimeout(ExecuteStatementRequestOnWaitTimeout.CONTINUE)
            .setParameters(collect);
    if (maxRows != DEFAULT_ROW_LIMIT) {
      request.setRowLimit(maxRows);
    }
    return request;
  }

  private StatementParameterListItem mapToParameterListItem(ImmutableSqlParameter parameter) {
    return new PositionalStatementParameterListItem()
        .setOrdinal(parameter.cardinal())
        .setType(parameter.type().name())
        .setValue(parameter.value() != null ? parameter.value().toString() : null);
  }

  /** Handles a failed execution and throws appropriate exception */
  void handleFailedExecution(
      ExecuteStatementResponse response, String statementId, String statement) throws SQLException {
    StatementState statementState = response.getStatus().getState();
    String errorMessage =
        String.format(
            "Statement execution failed %s -> %s\n%s: %s",
            statementId, statement, statementState, response.getStatus().getError().getMessage());
    LoggingUtil.log(LogLevel.DEBUG, errorMessage, this.getClass().getName());
    int errorCode;
    switch (statementState) {
      case FAILED:
        errorCode = ErrorCodes.EXECUTE_STATEMENT_FAILED;
        break;
      case CLOSED:
        errorCode = ErrorCodes.EXECUTE_STATEMENT_CLOSED;
        break;
      case CANCELED:
        errorCode = ErrorCodes.EXECUTE_STATEMENT_CANCELLED;
        break;
      default:
        throw new IllegalStateException("Invalid state for error");
    }
    throw new DatabricksSQLException(
        errorMessage, connectionContext, ErrorTypes.EXECUTE_STATEMENT, statementId, errorCode);
  }

  private ExecuteStatementResponse wrapGetStatementResponse(
      GetStatementResponse getStatementResponse) {
    return new ExecuteStatementResponse()
        .setStatementId(getStatementResponse.getStatementId())
        .setStatus(getStatementResponse.getStatus())
        .setManifest(getStatementResponse.getManifest())
        .setResult(getStatementResponse.getResult());
  }
}
