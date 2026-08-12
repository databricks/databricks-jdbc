package com.databricks.jdbc.model.client.sqlexec;

import com.databricks.sdk.support.QueryParam;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Request fields not carried in the GetResultData path. */
public class GetResultDataRequest {
  @JsonProperty("statement_id")
  private String statementId;

  @JsonProperty("chunk_index")
  private Long chunkIndex;

  @JsonProperty("row_offset")
  @QueryParam("row_offset")
  private Long rowOffset;

  @JsonProperty("backend_compute")
  @QueryParam("backend_compute")
  private BackendCompute backendCompute;

  public String getStatementId() {
    return statementId;
  }

  public GetResultDataRequest setStatementId(String statementId) {
    this.statementId = statementId;
    return this;
  }

  public Long getChunkIndex() {
    return chunkIndex;
  }

  public GetResultDataRequest setChunkIndex(Long chunkIndex) {
    this.chunkIndex = chunkIndex;
    return this;
  }

  public Long getRowOffset() {
    return rowOffset;
  }

  public GetResultDataRequest setRowOffset(Long rowOffset) {
    this.rowOffset = rowOffset;
    return this;
  }

  public BackendCompute getBackendCompute() {
    return backendCompute;
  }

  public GetResultDataRequest setBackendCompute(BackendCompute backendCompute) {
    this.backendCompute = backendCompute;
    return this;
  }
}
