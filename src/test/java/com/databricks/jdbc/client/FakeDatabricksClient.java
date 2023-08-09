package com.databricks.jdbc.client;

import com.databricks.jdbc.core.DatabricksResultSet;
import com.databricks.sdk.service.sql.*;

import java.sql.SQLException;

public class FakeDatabricksClient implements DatabricksClient {

  private final StatementExecutionService statementExecutionService;

  public FakeDatabricksClient(StatementExecutionService statementExecutionService) {
    this.statementExecutionService = statementExecutionService;
  }

  @Override
  public Session createSession(String warehouseId) {
    return statementExecutionService.createSession(
        new CreateSessionRequest().setWarehouseId(warehouseId));
  }

  @Override
  public void deleteSession(String sessionId) {
    statementExecutionService.deleteSession(new DeleteSessionRequest().setSessionId(sessionId));
  }

  @Override
  public DatabricksResultSet executeStatement(String statement, String sessionId, String warehouseId, boolean isInternal) throws SQLException {
    return null;
  }

  @Override
  public void closeStatement(String statementId) {
    statementExecutionService.closeStatement(new CloseStatementRequest().setStatementId(statementId));
  }
}