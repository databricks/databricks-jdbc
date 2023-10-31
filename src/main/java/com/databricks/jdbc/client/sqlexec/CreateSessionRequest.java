package com.databricks.jdbc.client.sqlexec;

import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/** Create session request */
public class CreateSessionRequest {

  /** Warehouse-Id for session */
  @JsonProperty("warehouse_id")
  private String warehouseId;

  public CreateSessionRequest setWarehouseId(String warehouseId) {
    this.warehouseId = warehouseId;
    return this;
  }

  public String getWarehouseId() {
    return warehouseId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CreateSessionRequest that = (CreateSessionRequest) o;
    return Objects.equals(warehouseId, that.warehouseId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(warehouseId);
  }

  @Override
  public String toString() {
    return new ToStringer(CreateSessionRequest.class).add("warehouseId", warehouseId).toString();
  }
}
