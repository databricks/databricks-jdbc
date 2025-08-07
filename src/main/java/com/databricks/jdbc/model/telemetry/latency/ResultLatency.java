package com.databricks.jdbc.model.telemetry.latency;

import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ResultLatency {

  @JsonProperty("result_set_ready_latency_millis")
  private Long resultSetReadyLatencyMillis;

  @JsonProperty("result_set_consumption_latency_millis")
  private Long resultSetConsumptionLatencyMillis;

  private Long startTimeOfResultSetIterationNano;

  public Long getResultSetReadyLatencyMillis() {
    return resultSetReadyLatencyMillis;
  }

  public Long getResultSetConsumptionLatencyMillis() {
    return resultSetConsumptionLatencyMillis;
  }

  public ResultLatency() {
    resultSetConsumptionLatencyMillis = null;
  }

  public ResultLatency setResultSetReadyLatencyMillis(Long resultSetReadyLatencyMillis) {
    this.resultSetReadyLatencyMillis = resultSetReadyLatencyMillis;
    return this;
  }

  public ResultLatency setResultSetConsumptionLatencyMillis(
      Long resultSetConsumptionLatencyMillis) {
    this.resultSetConsumptionLatencyMillis = resultSetConsumptionLatencyMillis;
    return this;
  }

  public void markResultSetConsumption(boolean hasNext) {
    if (startTimeOfResultSetIterationNano == null) {
      startTimeOfResultSetIterationNano = System.nanoTime();
    }
    if (!hasNext) {
      resultSetConsumptionLatencyMillis =
          (System.nanoTime() - startTimeOfResultSetIterationNano)
              / 1_000_000; // convert nano to milli
    }
  }

  @Override
  public String toString() {
    return new ToStringer(ResultLatency.class)
        .add("resultSetReadyLatencyMillis", resultSetReadyLatencyMillis)
        .add("resultSetConsumptionLatencyMillis", resultSetConsumptionLatencyMillis)
        .toString();
  }
}
