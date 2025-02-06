package com.databricks.jdbc.model.telemetry;

import com.databricks.sdk.support.ToStringer;
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

  @Override
  public String toString() {
    return new ToStringer(FrontendLogEntry.class).add("sql_driver_log", sqlDriverLog).toString();
  }
}
