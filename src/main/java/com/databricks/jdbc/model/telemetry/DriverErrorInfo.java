package com.databricks.jdbc.model.telemetry;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DriverErrorInfo {
  @JsonProperty("error_name")
  String errorName;

  @JsonProperty("stack_trace")
  String stackTrace;

  public DriverErrorInfo setErrorName(String errorName) {
    this.errorName = errorName;
    return this;
  }

  public DriverErrorInfo setStackTrace(String stackTrace) {
    this.stackTrace = stackTrace;
    return this;
  }
}
