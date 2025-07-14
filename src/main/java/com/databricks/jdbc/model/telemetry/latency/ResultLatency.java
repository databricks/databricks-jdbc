package com.databricks.jdbc.model.telemetry.latency;

import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ResultLatency {

  @JsonProperty("result_set_ready_latency_millis")
  private Long resultSetReadyLatencyMillis;

  @JsonProperty("result_set_consumption_latency_millis")
  private Long resultSetConsumptionLatencyMillis;

  public ResultLatency setResultSetReadyLatencyMillis(Long resultSetReadyLatencyMillis) {
    this.resultSetReadyLatencyMillis = resultSetReadyLatencyMillis;
    return this;
  }

  public ResultLatency setResultSetConsumptionLatencyMillis(
      Long resultSetConsumptionLatencyMillis) {
    this.resultSetConsumptionLatencyMillis = resultSetConsumptionLatencyMillis;
    return this;
  }

  @Override
  public String toString() {
    return new ToStringer(ResultLatency.class)
        .add("resultSetReadyLatencyMillis", resultSetReadyLatencyMillis)
        .add("resultSetConsumptionLatencyMillis", resultSetConsumptionLatencyMillis)
        .toString();
  }
}
