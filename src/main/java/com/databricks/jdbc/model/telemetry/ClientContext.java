package com.databricks.jdbc.model.telemetry;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ClientContext {

  @JsonProperty("timestamp_millis")
  Long timestampMillis;

  public ClientContext() {}

  public Long getTimestampMillis() {
    return timestampMillis;
  }

  public ClientContext setTimestampMillis(Long timestampMillis) {
    this.timestampMillis = timestampMillis;
    return this;
  }
}
