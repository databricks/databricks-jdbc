package com.databricks.jdbc.model.telemetry;

import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.model.telemetry.enums.ExecutionResultFormat;
import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SqlExecutionEvent {
  @JsonProperty("statement_type")
  StatementType driverStatementType;

  @JsonProperty("is_compressed")
  boolean isCompressed;

  @JsonProperty("execution_result")
  ExecutionResultFormat executionResultFormat;

  @JsonProperty("chunk_id")
  Long chunkId;

  @JsonProperty("retry_count")
  Integer retryCount;

  public SqlExecutionEvent setDriverStatementType(StatementType driverStatementType) {
    this.driverStatementType = driverStatementType;
    return this;
  }

  public SqlExecutionEvent setCompressed(boolean isCompressed) {
    this.isCompressed = isCompressed;
    return this;
  }

  public SqlExecutionEvent setExecutionResultFormat(ExecutionResultFormat executionResultFormat) {
    this.executionResultFormat = executionResultFormat;
    return this;
  }

  public SqlExecutionEvent setChunkId(Long chunkId) {
    this.chunkId = chunkId;
    return this;
  }

  public SqlExecutionEvent setRetryCount(Integer retryCount) {
    this.retryCount = retryCount;
    return this;
  }

  @Override
  public String toString() {
    return new ToStringer(SqlExecutionEvent.class)
        .add("statement_type", driverStatementType)
        .add("is_compressed", isCompressed)
        .add("execution_result", executionResultFormat)
        .add("chunk_id", chunkId)
        .add("retry_count", retryCount)
        .toString();
  }
}
