package com.databricks.jdbc.client.impl;

import com.databricks.jdbc.client.DatabricksClient;
import com.databricks.jdbc.client.StatementType;
import com.databricks.jdbc.client.sqlexec.CloseStatementRequest;
import com.databricks.jdbc.client.sqlexec.CreateSessionRequest;
import com.databricks.jdbc.client.sqlexec.DeleteSessionRequest;
import com.databricks.jdbc.client.sqlexec.Session;
import com.databricks.jdbc.client.sqlexec.*;
import com.databricks.jdbc.core.DatabricksResultSet;
import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.jdbc.core.IDatabricksSession;
import com.databricks.jdbc.core.IDatabricksStatement;
import com.databricks.jdbc.core.ImmutableSessionInfo;
import com.databricks.jdbc.core.ImmutableSqlParameter;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Implementation of DatabricksClient interface using Databricks Java SDK.
 */
public class DatabricksSdkClient implements DatabricksClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksSdkClient.class);
  private static final String ASYNC_TIMEOUT_VALUE = "0s";
  private static final String SYNC_TIMEOUT_VALUE = "10s";
  private static final int STATEMENT_RESULT_POLL_INTERVAL_MILLIS = 200;

  private final IDatabricksConnectionContext connectionContext;
  private final DatabricksConfig databricksConfig;
  private final WorkspaceClient workspaceClient;

  public DatabricksSdkClient(IDatabricksConnectionContext connectionContext) {
    this.connectionContext = connectionContext;
    // Handle more auth types
    this.databricksConfig = new DatabricksConfig()
        .setHost(connectionContext.getHostUrl())
        .setToken(connectionContext.getToken());

    this.workspaceClient = new WorkspaceClient(databricksConfig);
  }

  public DatabricksSdkClient(IDatabricksConnectionContext connectionContext,
                             StatementExecutionService statementExecutionService, ApiClient apiClient) {
    this.connectionContext = connectionContext;
    // Handle more auth types
    this.databricksConfig = new DatabricksConfig()
        .setHost(connectionContext.getHostUrl())
        .setToken(connectionContext.getToken());

    this.workspaceClient = new WorkspaceClient(true /* mock */, apiClient)
        .withStatementExecutionImpl(statementExecutionService);
  }

  @Override
  public ImmutableSessionInfo createSession(String warehouseId) {
    LOGGER.debug("public Session createSession(String warehouseId = {})", warehouseId);
    CreateSessionRequest request = new CreateSessionRequest()
        .setWarehouseId(warehouseId);
    String path = "/api/2.0/sql/statements/sessions";
    Map<String, String> headers = new HashMap<>();
    headers.put("Accept", "application/json");
    headers.put("Content-Type", "application/json");
    Session session = (Session) workspaceClient.apiClient().POST(path, request, Session.class, headers);
    return ImmutableSessionInfo.builder()
        .warehouseId(session.getWarehouseId())
        .sessionId(session.getSessionId())
        .build();
  }

  @Override
  public void deleteSession(String sessionId, String warehouseId) {
    LOGGER.debug("public void deleteSession(String sessionId = {})", sessionId);
    DeleteSessionRequest request = new DeleteSessionRequest().setSessionId(sessionId).setWarehouseId(warehouseId);
    String path = String.format("/api/2.0/sql/statements/sessions/%s", request.getSessionId());
    Map<String, String> headers = new HashMap<>();
    workspaceClient.apiClient().DELETE(path, request, Void.class, headers);
  }

  @Override
  public DatabricksResultSet executeStatement(
      String sql, String warehouseId, Map<Integer, ImmutableSqlParameter> parameters,
      StatementType statementType, IDatabricksSession session, IDatabricksStatement parentStatement) throws SQLException {
    LOGGER.debug("public DatabricksResultSet executeStatement(String sql = {}, String warehouseId = {}, Map<Integer, ImmutableSqlParameter> parameters, StatementType statementType = {}, IDatabricksSession session)", sql, warehouseId, statementType);
    Format format = useCloudFetchForResult(statementType) ? Format.ARROW_STREAM : Format.JSON_ARRAY;
    Disposition disposition = useCloudFetchForResult(statementType) ? Disposition.EXTERNAL_LINKS : Disposition.INLINE;
    ExecuteStatementRequestWithSession request =
        (ExecuteStatementRequestWithSession) new ExecuteStatementRequestWithSession()
            .setSessionId(session.getSessionId())
            .setStatement(sql)
            .setWarehouseId(warehouseId)
            .setDisposition(disposition)
            .setFormat(format)
            .setWaitTimeout(SYNC_TIMEOUT_VALUE)
            .setOnWaitTimeout(TimeoutAction.CONTINUE)
            .setParameters(parameters.values().stream().map(
                param -> new PositionalStatementParameterListItem()
                    .setOrdinal(param.cardinal())
                    .setType(param.type())
                    .setValue(param.value().toString()))
                .collect(Collectors.toList()));

    long pollCount = 0;
    long executionStartTime = Instant.now().toEpochMilli();
    ExecuteStatementResponse response = workspaceClient.statementExecution().executeStatement(request);
    String statementId = response.getStatementId();
    StatementState responseState = response.getStatus().getState();

    // TODO: Add timeout
    while (responseState == StatementState.PENDING || responseState == StatementState.RUNNING) {
      // First poll happens without a delay
      if (pollCount > 0) {
        try {
          // TODO: make this configurable
          Thread.sleep(STATEMENT_RESULT_POLL_INTERVAL_MILLIS);
        } catch (InterruptedException e) {
          // TODO: Handle gracefully
          throw new DatabricksSQLException("Statement execution fetch interrupted");
        }
      }
      response = wrapGetStatementResponse(workspaceClient.statementExecution().getStatement(statementId));
      responseState = response.getStatus().getState();
      pollCount++;
    }
    long executionEndTime = Instant.now().toEpochMilli();
    LOGGER.atDebug().log("Executed sql [%s] with status [%s], total time taken [%d] and pollCount [%d]",
        sql, responseState, (executionEndTime - executionStartTime), pollCount);
    if (responseState != StatementState.SUCCEEDED) {
      handleFailedExecution(responseState, statementId, sql);
    }
    return new DatabricksResultSet(response.getStatus(), statementId, response.getResult(),
          response.getManifest(), statementType, session, parentStatement);
  }

  private boolean useCloudFetchForResult(StatementType statementType) {
    return statementType == StatementType.QUERY || statementType == StatementType.SQL;
  }

  @Override
  public void closeStatement(String statementId) {
    LOGGER.debug("public void closeStatement(String statementId = {})", statementId);
    CloseStatementRequest request = new CloseStatementRequest().setStatementId(statementId);
    String path = String.format("/api/2.0/sql/statements/%s", request.getStatementId());
    Map<String, String> headers = new HashMap<>();
    workspaceClient.apiClient().DELETE(path, request, Void.class, headers);
  }

  @Override
  public Collection<ExternalLink> getResultChunks(String statementId, long chunkIndex) {
    LOGGER.debug("public Optional<ExternalLink> getResultChunk(String statementId = {}, long chunkIndex = {})", statementId, chunkIndex);
    return workspaceClient.statementExecution().getStatementResultChunkN(statementId, chunkIndex).getExternalLinks();
  }

  /**
   * Handles a failed execution and throws appropriate exception
   */
  private void handleFailedExecution(StatementState statementState, String statementId, String statement) throws SQLException {
    LOGGER.debug("private void handleFailedExecution(StatementState statementState = {}, String statementId = {}, String statement = {})", statementState, statementId, statement);
    switch (statementState) {
      case FAILED:
      case CLOSED:
      case CANCELED:
        // TODO: Handle differently for failed, closed and cancelled with proper error codes
        throw new DatabricksSQLException("Statement execution failed " + statementId + " -> " + statement);
      default:
        throw new IllegalStateException("Invalid state for error");
    }
  }

  private ExecuteStatementResponse wrapGetStatementResponse(GetStatementResponse getStatementResponse) {
    return new ExecuteStatementResponse().setStatementId(getStatementResponse.getStatementId())
        .setStatus(getStatementResponse.getStatus())
        .setManifest(getStatementResponse.getManifest())
        .setResult(getStatementResponse.getResult());
  }
}
