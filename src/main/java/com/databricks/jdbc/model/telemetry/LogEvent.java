package com.databricks.jdbc.model.telemetry;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LogEvent {

  @JsonProperty("statement_id")
  String statementId;

  public LogEvent() {}

  public String getStatementId() {
    return statementId;
  }

  public LogEvent setStatementId(String statementId) {
    this.statementId = statementId;
    return this;
  }
}
