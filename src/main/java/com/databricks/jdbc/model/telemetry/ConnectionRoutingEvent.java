package com.databricks.jdbc.model.telemetry;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Identifier-free outcome of one connection open attempt. */
public class ConnectionRoutingEvent {
  @JsonProperty("compute_type")
  private final String computeType;

  @JsonProperty("protocol")
  private final String protocol;

  @JsonProperty("reason")
  private final String reason;

  @JsonProperty("outcome")
  private final String outcome;

  public ConnectionRoutingEvent(
      String computeType, String protocol, String reason, String outcome) {
    this.computeType = computeType;
    this.protocol = protocol;
    this.reason = reason;
    this.outcome = outcome;
  }

  public String getComputeType() {
    return computeType;
  }

  public String getProtocol() {
    return protocol;
  }

  public String getReason() {
    return reason;
  }

  public String getOutcome() {
    return outcome;
  }
}
