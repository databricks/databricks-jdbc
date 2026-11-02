package com.databricks.jdbc.model.client.sqlexec;

import com.databricks.sdk.support.QueryParam;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Query parameters for an APC GetStatementResult request. */
public class GetStatementResultQuery {
  @JsonProperty("backend_compute")
  @QueryParam("backend_compute")
  private BackendCompute backendCompute;

  public BackendCompute getBackendCompute() {
    return backendCompute;
  }

  public GetStatementResultQuery setBackendCompute(BackendCompute backendCompute) {
    this.backendCompute = backendCompute;
    return this;
  }
}
