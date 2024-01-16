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
import com.databricks.jdbc.client.sqlexec.Session;
import com.databricks.jdbc.core.*;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation of DatabricksClient interface using Databricks Java SDK. */
public class DatabricksSdkClient implements DatabricksClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksSdkClient.class);
  private static final String SYNC_TIMEOUT_VALUE = "10s";
  private static final String ASYNC_TIMEOUT_VALUE = "0s";
  private static final int STATEMENT_RESULT_POLL_INTERVAL_MILLIS = 200;

  private final IDatabricksConnectionContext connectionContext;
  private final DatabricksConfig databricksConfig;
  private final WorkspaceClient workspaceClient;

  private static Map<String, String> getHeaders() {
    return Map.of(
        "Accept", "application/json",
        "Content-Type", "application/json");
  }

  public DatabricksSdkClient(IDatabricksConnectionContext connectionContext) {
    this.connectionContext = connectionContext;
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
      ApiClient apiClient) {
    this.connectionContext = connectionContext;
    // Handle more auth types
    this.databricksConfig =
        new DatabricksConfig()
            .setHost(connectionContext.getHostUrl())
            .setToken(connectionContext.getToken());

    this.workspaceClient =
        new WorkspaceClient(true /* mock */, apiClient)
            .withStatementExecutionImpl(statementExecutionService);
  }

  @Override
  public ImmutableSessionInfo createSession(String warehouseId) {
    LOGGER.debug("public Session createSession(String warehouseId = {})", warehouseId);
    CreateSessionRequest request = new CreateSessionRequest().setWarehouseId(warehouseId);
    Session session =
        workspaceClient.apiClient().POST(SESSION_PATH, request, Session.class, getHeaders());
    return ImmutableSessionInfo.builder()
        .warehouseId(session.getWarehouseId())
        .sessionId(session.getSessionId())
        .build();
  }

  @Override
  public void deleteSession(String sessionId, String warehouseId) {
    LOGGER.debug("public void deleteSession(String sessionId = {})", sessionId);
    DeleteSessionRequest request =
        new DeleteSessionRequest().setSessionId(sessionId).setWarehouseId(warehouseId);
    String path = String.format(SESSION_PATH_WITH_ID, request.getSessionId());
    Map<String, String> headers = new HashMap<>();
    workspaceClient.apiClient().DELETE(path, request, Void.class, headers);
  }

  @Override
  public DatabricksResultSet executeStatement(
      String sql,
      String warehouseId,
      Map<Integer, ImmutableSqlParameter> parameters,
      StatementType statementType,
      IDatabricksSession session,
      IDatabricksStatement parentStatement)
      throws SQLException {
    LOGGER.debug(
        "public DatabricksResultSet executeStatement(String sql = {}, String warehouseId = {}, Map<Integer, ImmutableSqlParameter> parameters, StatementType statementType = {}, IDatabricksSession session)",
        sql,
        warehouseId,
        statementType);

    long pollCount = 0;
    long executionStartTime = Instant.now().toEpochMilli();
    ExecuteStatementRequest request =
        getRequest(statementType, sql, warehouseId, session, parameters, parentStatement);
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
          Thread.sleep(STATEMENT_RESULT_POLL_INTERVAL_MILLIS); // TODO: make this configurable
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
      handleFailedExecution(responseState, statementId, sql);
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
    return statementType == StatementType.QUERY || statementType == StatementType.SQL;
  }

  @Override
  public void closeStatement(String statementId) {
    LOGGER.debug("public void closeStatement(String statementId = {})", statementId);
    CloseStatementRequest request = new CloseStatementRequest().setStatementId(statementId);
    String path = String.format(STATEMENT_PATH_WITH_ID, request.getStatementId());
    workspaceClient.apiClient().DELETE(path, request, Void.class, getHeaders());
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
        .setValue(parameter.value().toString());
  }

  /** Handles a failed execution and throws appropriate exception */
  private void handleFailedExecution(
      StatementState statementState, String statementId, String statement) throws SQLException {
    LOGGER.debug(
        "private void handleFailedExecution(StatementState statementState = {}, String statementId = {}, String statement = {})",
        statementState,
        statementId,
        statement);
    switch (statementState) {
      case FAILED:
      case CLOSED:
      case CANCELED:
        // TODO: Handle differently for failed, closed and cancelled with proper error codes
        throw new DatabricksSQLException(
            "Statement execution failed " + statementId + " -> " + statement);
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
