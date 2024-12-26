package com.databricks.jdbc.model.telemetry;

import com.databricks.jdbc.model.telemetry.enums.DriverStatementType;
import com.databricks.jdbc.model.telemetry.enums.ExecutionResultFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SqlExecutionEvent {
    @JsonProperty("auth_type")
    DriverStatementType driverStatementType;

    @JsonProperty("is_compressed")
    boolean isCompressed;

    @JsonProperty("execution_result")
    ExecutionResultFormat executionResultFormat;

    @JsonProperty("chunk_id")
    int chunkId;

    @JsonProperty("retry_count")
    int retryCount;

}
