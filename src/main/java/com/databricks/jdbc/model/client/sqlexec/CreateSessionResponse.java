// TODO: Remove these classes when available in the sdk
package com.databricks.jdbc.model.client.sqlexec;

import com.fasterxml.jackson.annotation.JsonProperty;

/** create session response */
public class CreateSessionResponse {
  /** session id for the session created */
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
