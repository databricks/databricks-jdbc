package com.databricks.jdbc.model.client.sqlexec;

import com.databricks.jdbc.model.core.SessionVersion;
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

  @JsonProperty("session_version")
  private SessionVersion sessionVersion;

  public CreateSessionResponse setSessionId(String sessionId) {
    this.sessionId = sessionId;
    return this;
  }

  public String getSessionId() {
    return sessionId;
  }

  public CreateSessionResponse setSessionVersion(SessionVersion sessionVersion) {
    this.sessionVersion = sessionVersion;
    return this;
  }

  public SessionVersion getSessionVersion() {
    return sessionVersion;
  }
}
