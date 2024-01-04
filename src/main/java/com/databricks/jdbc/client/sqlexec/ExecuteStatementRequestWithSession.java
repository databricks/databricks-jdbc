package com.databricks.jdbc.client.sqlexec;

public class ExecuteStatementRequestWithSession extends ExecuteStatementRequest {

  public ExecuteStatementRequestWithSession(String sessionId) {
    super.setSessionId(sessionId);
  }
}
