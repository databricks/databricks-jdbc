package com.databricks.jdbc.model.telemetry.latency;

import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;

// Note: Thread safety is not required for this class as it is used within
// a single statement context that is not shared across multiple threads.
public class OperationDetail {

  @JsonProperty("n_operation_status_calls")
  private Long nOperationStatusCalls;

  @JsonProperty("operation_status_latency_millis")
  private Long operationStatusLatencyMillis;

  @JsonProperty("operation_type")
  private OperationType operationType;

  @JsonProperty("is_internal_call")
  private Boolean isInternalCall;

  public OperationDetail(Boolean isInternalCall) {
    this.nOperationStatusCalls = 0L;
    this.operationStatusLatencyMillis = 0L;
    this.operationType = OperationType.TYPE_UNSPECIFIED;
    this.isInternalCall = isInternalCall;
  }

  public void addOperationStatusLatencyMillis(Long latencyMillis) {
    this.operationStatusLatencyMillis += latencyMillis;
    this.nOperationStatusCalls++;
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
