package com.databricks.jdbc.model.telemetry;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TelemetryFrontendLog {

  @JsonProperty("workspace_id")
  Long workspaceId;

  @JsonProperty("frontend_log_event_id")
  String frontendLogEventId;

  @JsonProperty("context")
  FrontendLogContext context;

  @JsonProperty("entry")
  FrontendLogEntry entry;

  public TelemetryFrontendLog() {}

  public Long getWorkspaceId() {
    return workspaceId;
  }

  public TelemetryFrontendLog setWorkspaceId(Long workspaceId) {
    this.workspaceId = workspaceId;
    return this;
  }

  public String getFrontendLogEventId() {
    return frontendLogEventId;
  }

  public TelemetryFrontendLog setFrontendLogEventId(String frontendLogEventId) {
    this.frontendLogEventId = frontendLogEventId;
    return this;
  }

  public FrontendLogContext getContext() {
    return context;
  }

  public TelemetryFrontendLog setContext(FrontendLogContext context) {
    this.context = context;
    return this;
  }

  public FrontendLogEntry getEntry() {
    return entry;
  }

  public TelemetryFrontendLog setEntry(FrontendLogEntry entry) {
    this.entry = entry;
    return this;
  }

  public static class FrontendLogEntry {
    @JsonProperty("sql_driver_log")
    TelemetryEvent sqlDriverLog;

    public FrontendLogEntry() {}

    public TelemetryEvent getSqlDriverLog() {
      return sqlDriverLog;
    }

    public FrontendLogEntry setSqlDriverLog(TelemetryEvent sqlDriverLog) {
      this.sqlDriverLog = sqlDriverLog;
      return this;
    }
  }
}
