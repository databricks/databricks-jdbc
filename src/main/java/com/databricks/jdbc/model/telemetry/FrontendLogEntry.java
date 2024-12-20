package com.databricks.jdbc.model.telemetry;

import com.fasterxml.jackson.annotation.JsonProperty;

public class FrontendLogEntry {
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
