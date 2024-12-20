package com.databricks.jdbc.model.telemetry;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TelemetryClientContext {

  @JsonProperty("timestamp_millis")
  Long timestampMillis;

  public TelemetryClientContext() {}

  public Long getTimestampMillis() {
    return timestampMillis;
  }

  public TelemetryClientContext setTimestampMillis(Long timestampMillis) {
    this.timestampMillis = timestampMillis;
    return this;
  }
}
