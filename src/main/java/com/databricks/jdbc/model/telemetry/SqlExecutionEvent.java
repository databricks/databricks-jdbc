package com.databricks.jdbc.model.telemetry;

import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.model.telemetry.enums.ExecutionResultFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SqlExecutionEvent {
  @JsonProperty("statement_type")
  StatementType driverStatementType;

  @JsonProperty("is_compressed")
  boolean isCompressed;

  @JsonProperty("execution_result")
  ExecutionResultFormat executionResultFormat;

  @JsonProperty("chunk_id")
  int chunkId;

  @JsonProperty("retry_count")
  int retryCount;

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

  public SqlExecutionEvent setChunkId(int chunkId) {
    this.chunkId = chunkId;
    return this;
  }

  public SqlExecutionEvent setRetryCount(int retryCount) {
    this.retryCount = retryCount;
    return this;
  }
}
