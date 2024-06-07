package com.databricks.jdbc.client.impl.sdk;

import static com.databricks.jdbc.client.impl.sdk.PathConstants.*;
import static com.databricks.jdbc.commons.EnvironmentVariables.DEFAULT_ROW_LIMIT;

import com.databricks.jdbc.client.DatabricksClient;
import com.databricks.jdbc.client.StatementType;
import com.databricks.jdbc.client.sqlexec.*;
import com.databricks.jdbc.client.sqlexec.CloseStatementRequest;
import com.databricks.jdbc.client.sqlexec.CreateSessionRequest;
import com.databricks.jdbc.client.sqlexec.DeleteSessionRequest;
import com.databricks.jdbc.client.sqlexec.ExecuteStatementRequest;
import com.databricks.jdbc.client.sqlexec.ExecuteStatementResponse;
import com.databricks.jdbc.client.sqlexec.ExternalLink;
import com.databricks.jdbc.client.sqlexec.GetStatementResponse;
import com.databricks.jdbc.client.sqlexec.ResultData;
import com.databricks.jdbc.core.*;
import com.databricks.jdbc.core.types.ComputeResource;
import com.databricks.jdbc.core.types.Warehouse;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.sql.*;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Implementation of DatabricksClient interface using Databricks Java SDK. */
public class DatabricksSdkClient implements DatabricksClient {
  private static final Logger LOGGER = LogManager.getLogger(DatabricksSdkClient.class);
  private static final String SYNC_TIMEOUT_VALUE = "10s";
  private static final String ASYNC_TIMEOUT_VALUE = "0s";

  private final IDatabricksConnectionContext connectionContext;
  private final DatabricksConfig databricksConfig;
  private final WorkspaceClient workspaceClient;

  private static Map<String, String> getHeaders() {
    return Map.of(
        "Accept", "application/json",
        "Content-Type", "application/json");
  }

  public DatabricksSdkClient(IDatabricksConnectionContext connectionContext)
      throws DatabricksParsingException {
    this.connectionContext = connectionContext;
    // TODO: [PECO-1486] pass on proxy settings to SDK once changes are merged in SDK
    // Handle more auth types
    this.databricksConfig =
        new DatabricksConfig()
            .setHost(connectionContext.getHostUrl())
            .setToken(connectionContext.getToken());

    OAuthAuthenticator authenticator = new OAuthAuthenticator(connectionContext);
    this.workspaceClient = authenticator.getWorkspaceClient();
  }

  public DatabricksSdkClient(
      IDatabricksConnectionContext connectionContext,
      StatementExecutionService statementExecutionService,
      ApiClient apiClient)
      throws DatabricksParsingException {
    this.connectionContext = connectionContext;
    this.databricksConfig =
        new DatabricksConfig()
            .setHost(connectionContext.getHostUrl())
            .setToken(connectionContext.getToken());

    this.workspaceClient =
        new WorkspaceClient(true /* mock */, apiClient)
            .withStatementExecutionImpl(statementExecutionService);
  }

  @Override
  public ImmutableSessionInfo createSession(
      ComputeResource warehouse, String catalog, String schema, Map<String, String> sessionConf) {
    LOGGER.debug(
        "public Session createSession(String warehouseId = {}, String catalog = {}, String schema = {}, Map<String, String> sessionConf = {})",
        ((Warehouse) warehouse).getWarehouseId(),
        catalog,
        schema,
        sessionConf);
    // TODO: [PECO-1460] Handle sessionConf in public session API
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
  public void deleteSession(IDatabricksSession session, ComputeResource warehouse) {
    LOGGER.debug("public void deleteSession(String sessionId = {})", session.getSessionId());
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
      ComputeResource computeResource,
      Map<Integer, ImmutableSqlParameter> parameters,
      StatementType statementType,
      IDatabricksSession session,
      IDatabricksStatement parentStatement)
      throws SQLException {
    LOGGER.debug(
        "public DatabricksResultSet executeStatement(String sql = {}, compute resource = {}, Map<Integer, ImmutableSqlParameter> parameters, StatementType statementType = {}, IDatabricksSession session)",
        sql,
        computeResource.toString(),
        statementType);

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
    LOGGER.debug(
        "Executed sql [{}] with status [{}], total time taken [{}] and pollCount [{}]",
        sql,
        responseState,
        (executionEndTime - executionStartTime),
        pollCount);
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
    LOGGER.debug("public void closeStatement(String statementId = {})", statementId);
    CloseStatementRequest request = new CloseStatementRequest().setStatementId(statementId);
    String path = String.format(STATEMENT_PATH_WITH_ID, request.getStatementId());
    workspaceClient.apiClient().DELETE(path, request, Void.class, getHeaders());
  }

  @Override
  public void cancelStatement(String statementId) {
    LOGGER.debug("public void cancelStatement(String statementId = {})", statementId);
    CancelStatementRequest request = new CancelStatementRequest().setStatementId(statementId);
    String path = String.format(CANCEL_STATEMENT_PATH_WITH_ID, request.getStatementId());
    workspaceClient.apiClient().POST(path, request, Void.class, getHeaders());
  }

  @Override
  public Collection<ExternalLink> getResultChunks(String statementId, long chunkIndex) {
    LOGGER.debug(
        "public Optional<ExternalLink> getResultChunk(String statementId = {}, long chunkIndex = {})",
        statementId,
        chunkIndex);
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
        .setType(parameter.type())
        .setValue(parameter.value() != null ? parameter.value().toString() : null);
  }

  /** Handles a failed execution and throws appropriate exception */
  private void handleFailedExecution(
      ExecuteStatementResponse response, String statementId, String statement) throws SQLException {
    LOGGER.debug(
        "private void handleFailedExecution(ExecuteStatementResponse response, String statementId = {}, String statement = {})",
        statementId,
        statement);
    StatementState statementState = response.getStatus().getState();
    switch (statementState) {
      case FAILED:
      case CLOSED:
      case CANCELED:
        // TODO: Handle differently for failed, closed and cancelled with proper error codes
        throw new DatabricksSQLException(
            String.format(
                "Statement execution failed %s -> %s\n%s: %s",
                statementId,
                statement,
                statementState,
                response.getStatus().getError().getMessage()));
      default:
        throw new IllegalStateException("Invalid state for error");
    }
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
