package com.databricks.jdbc.client.impl;

import com.databricks.jdbc.client.DatabricksClient;
import com.databricks.jdbc.core.DatabricksResultSet;
import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.jdbc.core.IDatabricksResultSet;
import com.databricks.jdbc.core.IDatabricksSession;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.sql.*;
import com.google.common.annotations.VisibleForTesting;

import java.sql.SQLException;
import java.util.Optional;

/**
 * Implementation of DatabricksClient interface using Databricks Java SDK.
 */
public class DatabricksSdkClient implements DatabricksClient {

  private static final String ASYNC_TIMEOUT_VALUE = "0s";
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

  public DatabricksSdkClient(IDatabricksConnectionContext connectionContext, StatementExecutionService statementExecutionService) {
    this.connectionContext = connectionContext;
    // Handle more auth types
    this.databricksConfig = new DatabricksConfig()
        .setHost(connectionContext.getHostUrl())
        .setToken(connectionContext.getToken());

    this.workspaceClient = new WorkspaceClient(true).withStatementExecutionImpl(statementExecutionService);
  }

  @Override
  public Session createSession(String warehouseId) {
    CreateSessionRequest createSessionRequest = new CreateSessionRequest()
        .setWarehouseId(warehouseId);
    return workspaceClient.statementExecution().createSession(createSessionRequest);
  }

  @Override
  public void deleteSession(String sessionId) {
    workspaceClient.statementExecution().deleteSession(sessionId);
  }

  @Override
  public DatabricksResultSet executeStatement(
      String statement, String warehouseId, boolean isInternal, IDatabricksSession session) throws SQLException {
    // TODO: change disposition and format, and handle pending result
    ExecuteStatementRequest request = new ExecuteStatementRequest()
        .setStatement(statement)
        .setWarehouseId(warehouseId)
        .setDisposition(isInternal ? Disposition.INLINE : Disposition.EXTERNAL_LINKS)
        .setFormat(isInternal ? Format.JSON_ARRAY : Format.ARROW_STREAM)
        .setWaitTimeout(ASYNC_TIMEOUT_VALUE)
        .setSessionId(session.getSessionId());

    ExecuteStatementResponse response = workspaceClient.statementExecution().executeStatement(request);
    String statementId = response.getStatementId();
    StatementState responseState = response.getStatus().getState();

    // TODO: Add timeout
    while (responseState == StatementState.PENDING || responseState == StatementState.RUNNING) {
      try {
        // TODO: make this configurable
        Thread.sleep(STATEMENT_RESULT_POLL_INTERVAL_MILLIS);
      } catch (InterruptedException e) {
        // TODO: Handle gracefully
        throw new DatabricksSQLException("Statement execution fetch interrupted");
      }
      response = wrapGetStatementResponse(workspaceClient.statementExecution().getStatement(statementId));
      responseState = response.getStatus().getState();
    }
    if (responseState != StatementState.SUCCEEDED) {
      handleFailedExecution(responseState, statementId);
    }

    return new DatabricksResultSet(response.getStatus(), statementId, response.getResult(),
          response.getManifest(), session);
  }

  @Override
  public void closeStatement(String statementId) {
    workspaceClient.statementExecution().closeStatement(statementId);
  }

  @Override
  public Optional<ExternalLink> getResultChunk(String statementId, long chunkIndex) {
    return workspaceClient.statementExecution().getStatementResultChunkN(statementId, chunkIndex).getExternalLinks().stream().findFirst();
  }

  /**
   * Handles a failed execution and throws appropriate exception
   */
  private void handleFailedExecution(StatementState statementState, String statementId) throws SQLException {

    switch (statementState) {
      case FAILED:
      case CLOSED:
      case CANCELED:
        // TODO: Handle differently for failed, closed and cancelled with proper error codes
        throw new DatabricksSQLException("Statement execution failed " + statementId);
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
