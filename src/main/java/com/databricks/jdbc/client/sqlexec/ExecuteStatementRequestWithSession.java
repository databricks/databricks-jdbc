package com.databricks.jdbc.client.sqlexec;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public class ExecuteStatementRequestWithSession extends ExecuteStatementRequest {

  /** session-id */
  @JsonProperty("session_id")
  private String sessionId;

  public ExecuteStatementRequestWithSession setSessionId(String sessionId) {
    this.sessionId = sessionId;
    return this;
  }

  public String getSessionId() {
    return sessionId;
  }

  public boolean equals(Object o) {
    if (o != null && o.getClass() == getClass()) {
      return super.equals(o)
          && Objects.equals(this.sessionId, ((ExecuteStatementRequestWithSession) o).sessionId);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), sessionId);
  }
}
