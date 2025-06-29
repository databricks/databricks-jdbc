package com.databricks.jdbc.model.telemetry.latency;

import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;

public class OperationDetail {

  @JsonProperty("n_operation_status_calls")
  private Integer nOperationStatusCalls;

  @JsonProperty("operation_status_latency_millis")
  private Long operationStatusLatencyMillis;

  @JsonProperty("operation_type")
  private OperationType operationType;

  @JsonProperty("is_internal_call")
  private Boolean isInternalCall;

  public OperationDetail setNOperationStatusCalls(Integer nOperationStatusCalls) {
    this.nOperationStatusCalls = nOperationStatusCalls;
    return this;
  }

  public OperationDetail setOperationStatusLatencyMillis(Long operationStatusLatencyMillis) {
    this.operationStatusLatencyMillis = operationStatusLatencyMillis;
    return this;
  }

  public OperationDetail setOperationType(OperationType operationType) {
    this.operationType = operationType;
    return this;
  }

  public OperationDetail setInternalCall(Boolean isInternalCall) {
    this.isInternalCall = isInternalCall;
    return this;
  }

  @Override
  public String toString() {
    return new ToStringer(OperationDetail.class)
        .add("nOperationStatusCalls", nOperationStatusCalls)
        .add("operationLatencyMillis", operationStatusLatencyMillis)
        .add("operationName", operationType)
        .add("isInternalCall", isInternalCall)
        .toString();
  }
}
