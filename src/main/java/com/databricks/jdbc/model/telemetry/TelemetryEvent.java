package com.databricks.jdbc.model.telemetry;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TelemetryEvent {

  @JsonProperty("session_id")
  String sessionId;

  @JsonProperty("sql_statement_id")
  String sqlStatementId;

  @JsonProperty("system_configuration")
  DriverSystemConfiguration driverSystemConfiguration;

  public TelemetryEvent() {}

  public String getSessionId() {
    return sessionId;
  }

  public TelemetryEvent setSessionId(String sessionId) {
    this.sessionId = sessionId;
    return this;
  }

  public String getSqlStatementId() {
    return sqlStatementId;
  }

  public TelemetryEvent setSqlStatementId(String sqlStatementId) {
    this.sqlStatementId = sqlStatementId;
    return this;
  }

  public DriverSystemConfiguration getDriverSystemConfiguration() {
    return driverSystemConfiguration;
  }

  public TelemetryEvent setDriverSystemConfiguration(
      DriverSystemConfiguration driverSystemConfiguration) {
    this.driverSystemConfiguration = driverSystemConfiguration;
    return this;
  }
}
