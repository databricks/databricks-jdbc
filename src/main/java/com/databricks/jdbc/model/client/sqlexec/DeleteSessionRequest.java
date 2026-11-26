package com.databricks.jdbc.model.client.sqlexec;

import com.databricks.sdk.support.QueryParam;
import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public class DeleteSessionRequest {

  @JsonProperty("session_id")
  private String sessionId;

  @JsonProperty("warehouse_id")
  @QueryParam("warehouse_id")
  private String warehouseId;

  @JsonProperty("backend_compute")
  @QueryParam("backend_compute")
  private BackendCompute backendCompute;

  public DeleteSessionRequest() {}

  public DeleteSessionRequest setSessionId(String sessionId) {
    this.sessionId = sessionId;
    return this;
  }

  public DeleteSessionRequest setWarehouseId(String warehouseId) {
    this.warehouseId = warehouseId;
    return this;
  }

  public String getSessionId() {
    return this.sessionId;
  }

  public String getWarehouseId() {
    return this.warehouseId;
  }

  public DeleteSessionRequest setBackendCompute(BackendCompute backendCompute) {
    this.backendCompute = backendCompute;
    return this;
  }

  public BackendCompute getBackendCompute() {
    return backendCompute;
  }

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o != null && this.getClass() == o.getClass()) {
      DeleteSessionRequest that = (DeleteSessionRequest) o;
      return Objects.equals(this.sessionId, that.sessionId)
          && Objects.equals(this.warehouseId, that.warehouseId)
          && Objects.equals(this.backendCompute, that.backendCompute);
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hash(this.sessionId, this.warehouseId, this.backendCompute);
  }

  public String toString() {
    return (new ToStringer(DeleteSessionRequest.class))
        .add("sessionId", this.sessionId)
        .add("warehouseId", this.warehouseId)
        .add("backendCompute", this.backendCompute)
        .toString();
  }
}
