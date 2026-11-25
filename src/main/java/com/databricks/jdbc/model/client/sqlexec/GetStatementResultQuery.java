package com.databricks.jdbc.model.client.sqlexec;

import com.databricks.sdk.support.QueryParam;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Query parameters for an APC GetStatementResult request. */
public class GetStatementResultQuery {
  @JsonProperty("row_offset")
  @QueryParam("row_offset")
  private Long rowOffset;

  @JsonProperty("backend_compute")
  @QueryParam("backend_compute")
  private BackendCompute backendCompute;

  public Long getRowOffset() {
    return rowOffset;
  }

  public GetStatementResultQuery setRowOffset(Long rowOffset) {
    this.rowOffset = rowOffset;
    return this;
  }

  public BackendCompute getBackendCompute() {
    return backendCompute;
  }

  public GetStatementResultQuery setBackendCompute(BackendCompute backendCompute) {
    this.backendCompute = backendCompute;
    return this;
  }
}
