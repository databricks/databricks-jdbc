package com.databricks.jdbc.model.client.sqlexec;

import com.databricks.sdk.support.QueryParam;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Request fields not carried in the GetStatementResult path. */
public class GetStatementResultRequest {
  @JsonProperty("statement_id")
  private String statementId;

  @JsonProperty("backend_compute")
  @QueryParam("backend_compute")
  private BackendCompute backendCompute;

  public String getStatementId() {
    return statementId;
  }

  public GetStatementResultRequest setStatementId(String statementId) {
    this.statementId = statementId;
    return this;
  }

  public BackendCompute getBackendCompute() {
    return backendCompute;
  }

  public GetStatementResultRequest setBackendCompute(BackendCompute backendCompute) {
    this.backendCompute = backendCompute;
    return this;
  }
}
