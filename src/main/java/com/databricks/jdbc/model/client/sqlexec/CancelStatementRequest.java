package com.databricks.jdbc.model.client.sqlexec;

import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public class CancelStatementRequest {
  private String statementId;

  @JsonProperty("backend_compute")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private BackendCompute backendCompute;

  public BackendCompute getBackendCompute() {
    return backendCompute;
  }

  public CancelStatementRequest setBackendCompute(BackendCompute backendCompute) {
    this.backendCompute = backendCompute;
    return this;
  }

  public CancelStatementRequest() {}

  public CancelStatementRequest setStatementId(String statementId) {
    this.statementId = statementId;
    return this;
  }

  public String getStatementId() {
    return this.statementId;
  }

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o != null && this.getClass() == o.getClass()) {
      CancelStatementRequest that = (CancelStatementRequest) o;
      return Objects.equals(this.statementId, that.statementId)
          && Objects.equals(this.backendCompute, that.backendCompute);
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hash(this.statementId, this.backendCompute);
  }

  public String toString() {
    return (new ToStringer(CancelStatementRequest.class))
        .add("statementId", this.statementId)
        .toString();
  }
}
