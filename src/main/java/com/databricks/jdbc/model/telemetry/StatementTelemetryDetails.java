package com.databricks.jdbc.model.telemetry;

import com.databricks.jdbc.model.telemetry.enums.ExecutionResultFormat;
import com.databricks.jdbc.model.telemetry.latency.ChunkDetails;
import com.databricks.jdbc.model.telemetry.latency.OperationDetail;
import com.databricks.jdbc.model.telemetry.latency.OperationType;
import com.databricks.jdbc.model.telemetry.latency.ResultLatency;

/** This class is used to store the telemetry details for a statement. */
public class StatementTelemetryDetails {
  private boolean isInternalCall;
  private final ChunkDetails chunkDetails;
  private final ResultLatency resultLatency =
      new ResultLatency()
          .setResultSetReadyLatencyMillis(null)
          .setResultSetConsumptionLatencyMillis(null);
  private final OperationDetail operationDetail;
  private Long operationLatencyMillis;
  private final String statementId;
  ExecutionResultFormat executionResultFormat;

  /*
   * TODO :
   * 1. Add connectionContext, sessionId, etc here to eliminate the need of threadLocal
   * 2. Add volumeOperationDetails here
   * 3. Add internal call flag here
   */

  public StatementTelemetryDetails(String statementId) {
    this.chunkDetails = new ChunkDetails();
    this.isInternalCall = false;
    this.operationDetail = new OperationDetail(isInternalCall);
    this.operationLatencyMillis = null;
    this.statementId = statementId;
  }

  public String getStatementId() {
    return statementId;
  }

  public boolean isInternalCall() {
    return isInternalCall;
  }

  public ResultLatency getResultLatency() {
    return resultLatency;
  }

  public OperationDetail getOperationDetail() {
    return operationDetail;
  }

  public ChunkDetails getChunkDetails() {
    return chunkDetails;
  }

  public StatementTelemetryDetails setOperationLatencyMillis(Long operationLatencyMillis) {
    this.operationLatencyMillis = operationLatencyMillis;
    return this;
  }

  public StatementTelemetryDetails setInternalCall(boolean isInternalCall) {
    this.isInternalCall = isInternalCall;
    return this;
  }

  public void recordChunkDownloadLatency(long chunkIndex, long latencyMillis) {
    // Record initial chunk latency (first chunk downloaded)
    if (chunkIndex == 0) {
      chunkDetails.setInitialChunkLatencyMillis(latencyMillis);
    }
    // Update the slowest chunk latency
    Long currentSlowest = chunkDetails.getSlowestChunkLatencyMillis();
    if (currentSlowest == null || latencyMillis > currentSlowest) {
      chunkDetails.setSlowestChunkLatencyMillis(latencyMillis);
    }
    // Add to sum of all chunk download times
    Long currentSum = chunkDetails.getSumChunksDownloadTimeMillis();
    if (currentSum == null) {
      currentSum = 0L;
    }
    chunkDetails.setSumChunksDownloadTimeMillis(currentSum + latencyMillis);
  }

  public void recordChunkIteration(Long totalChunks) {
    Long currentIterated = chunkDetails.getTotalChunksIterated();
    if (currentIterated == null) {
      currentIterated = 0L;
      chunkDetails.setTotalChunksPresent(totalChunks);
    }
    chunkDetails.setTotalChunksIterated(currentIterated + 1);
  }

  public void recordResultSetIteration(Long totalChunks, boolean hasNext) {
    this.chunkDetails.setTotalChunksPresent(totalChunks);
    this.resultLatency.markResultSetConsumption(hasNext);
  }

  public StatementTelemetryDetails recordOperationLatency(
      long latencyMillis, OperationType operationType) {
    this.operationDetail.setOperationType(operationType);
    this.operationLatencyMillis = latencyMillis;
    return this;
  }

  public void recordGetOperationStatusLatency(long latencyMillis) {
    this.operationDetail.addOperationStatusLatencyMillis(latencyMillis);
  }

  public Long getOperationLatencyMillis() {
    return operationLatencyMillis;
  }

  public StatementTelemetryDetails setExecutionResultFormat(
      ExecutionResultFormat executionResultFormat) {
    this.executionResultFormat = executionResultFormat;
    return this;
  }

  public ExecutionResultFormat getExecutionResultFormat() {
    return executionResultFormat;
  }
}
