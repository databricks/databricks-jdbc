package com.databricks.jdbc.model.client.sqlexec;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Create session response
 *
 * <p>TODO: Replace this class with the corresponding SDK implementation once it becomes available
 */
public class CreateSessionResponse {

  /** session_id for the session created */
  @JsonProperty("session_id")
  private String sessionId;

  public CreateSessionResponse setSessionId(String sessionId) {
    this.sessionId = sessionId;
    return this;
  }

  public String getSessionId() {
    return sessionId;
  }
}
