package com.databricks.jdbc.model.telemetry;

import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TelemetryClientContext {

  @JsonProperty("timestamp_millis")
  Long timestampMillis;

  @JsonProperty("user_agent")
  String userAgent;

  public TelemetryClientContext() {}

  public Long getTimestampMillis() {
    return timestampMillis;
  }

  public TelemetryClientContext setTimestampMillis(Long timestampMillis) {
    this.timestampMillis = timestampMillis;
    return this;
  }

  public String getUserAgent() {
    return userAgent;
  }

  public TelemetryClientContext setUserAgent(String userAgent) {
    this.userAgent = userAgent;
    return this;
  }

  @Override
  public String toString() {
    return new ToStringer(TelemetryFrontendLog.class)
        .add("timestampMillis", timestampMillis)
        .add("userAgent", userAgent)
        .toString();
  }
}
