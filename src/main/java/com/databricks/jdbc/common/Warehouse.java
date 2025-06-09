package com.databricks.jdbc.common;

import java.util.Objects;

public class Warehouse implements IDatabricksComputeResource {
  private final String warehouseId;

  public Warehouse(String warehouseId) {
    this.warehouseId = warehouseId;
  }

  public String getWarehouseId() {
    return this.warehouseId;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Warehouse)) {
      return false;
    }
    return Objects.equals(((Warehouse) obj).warehouseId, this.getWarehouseId());
  }

  @Override
  public String toString() {
    return String.format("SQL Warehouse with warehouse ID {%s}", warehouseId);
  }

  @Override
  public String getWorkspaceId() {
    // TODO: Return workspace ID instead of warehouse ID
    return this.warehouseId;
  }

  @Override
  public String getUniqueIdentifier() {
    return warehouseId;
  }
}
