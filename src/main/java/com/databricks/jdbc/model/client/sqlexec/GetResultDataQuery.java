package com.databricks.jdbc.model.client.sqlexec;

import com.databricks.sdk.support.QueryParam;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Query parameters for an APC GetResultData request. */
public class GetResultDataQuery {
  @JsonProperty("row_offset")
  @QueryParam("row_offset")
  private Long rowOffset;

  @JsonProperty("backend_compute")
  @QueryParam("backend_compute")
  private BackendCompute backendCompute;

  public Long getRowOffset() {
    return rowOffset;
  }

  public GetResultDataQuery setRowOffset(Long rowOffset) {
    this.rowOffset = rowOffset;
    return this;
  }

  public BackendCompute getBackendCompute() {
    return backendCompute;
  }

  public GetResultDataQuery setBackendCompute(BackendCompute backendCompute) {
    this.backendCompute = backendCompute;
    return this;
  }
}
