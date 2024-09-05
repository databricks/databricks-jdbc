package com.databricks.jdbc.model.client.sqlexec;

public class ExecuteStatementRequestWithSession extends ExecuteStatementRequest {

  public ExecuteStatementRequestWithSession(String sessionId) {
    super.setSessionId(sessionId);
  }
}
