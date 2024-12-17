package com.databricks.jdbc.model.telemetry;

import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.telemetry.TelemetryHelper;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DriverConnectionParameters {
  @JsonProperty("http_path")
  String httpPath;

  @JsonProperty("mode")
  DriverMode driverMode;

  public DriverConnectionParameters setHttpPath(String httpPath) {
    this.httpPath = httpPath;
    return this;
  }

  public DriverConnectionParameters setDriverMode(DatabricksClientType clientType) {
    this.driverMode = TelemetryHelper.toDriverMode(clientType);
    return this;
  }
}
