package com.databricks.jdbc.model.telemetry;

import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TelemetryEvent {

  @JsonProperty("session_id")
  String sessionId;

  @JsonProperty("sql_statement_id")
  String sqlStatementId;

  @JsonProperty("system_configuration")
  DriverSystemConfiguration driverSystemConfiguration;

  @JsonProperty("driver_connection_params")
  DriverConnectionParameters driverConnectionParameters;

  @JsonProperty("auth_type")
  String authType;

  @JsonProperty("vol_operation")
  DriverVolumeOperation volumeOperation;

  @JsonProperty("sql_operation")
  SqlExecutionEvent sqlOperation;

  @JsonProperty("error_info")
  DriverErrorInfo driverErrorInfo;

  @JsonProperty("operation_latency_ms")
  long latency;

  public TelemetryEvent() {}

  public String getSessionId() {
    return sessionId;
  }

  public TelemetryEvent setSessionId(String sessionId) {
    this.sessionId = sessionId;
    return this;
  }

  public TelemetryEvent setLatency(long latency) {
    this.latency = latency;
    return this;
  }

  public TelemetryEvent setDriverConnectionParameters(
      DriverConnectionParameters connectionParameters) {
    this.driverConnectionParameters = connectionParameters;
    return this;
  }

  public DriverConnectionParameters getDriverConnectionParameters() {
    return driverConnectionParameters;
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

  public TelemetryEvent setAuthType(String authType) {
    this.authType = authType;
    return this;
  }

  public TelemetryEvent setVolumeOperation(DriverVolumeOperation volumeOperation) {
    this.volumeOperation = volumeOperation;
    return this;
  }

  public TelemetryEvent setSqlOperation(SqlExecutionEvent sqlOperation) {
    this.sqlOperation = sqlOperation;
    return this;
  }

  public TelemetryEvent setDriverErrorInfo(DriverErrorInfo driverErrorInfo) {
    this.driverErrorInfo = driverErrorInfo;
    return this;
  }

  @Override
  public String toString() {
    return new ToStringer(TelemetryEvent.class)
        .add("sessionId", sessionId)
        .add("sqlStatementId", sqlStatementId)
        .add("driverSystemConfiguration", driverSystemConfiguration)
        .add("driverConnectionParameters", driverConnectionParameters)
        .add("authType", authType)
        .add("volumeOperation", volumeOperation)
        .add("sqlOperation", sqlOperation)
        .add("driverErrorInfo", driverErrorInfo)
        .add("latency", latency)
        .toString();
  }
}
