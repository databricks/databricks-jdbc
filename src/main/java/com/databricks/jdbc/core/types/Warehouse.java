package com.databricks.jdbc.core.types;

public class Warehouse implements ComputeResource {
  private final String warehouseId;

  public Warehouse(String warehouseId) {
    this.warehouseId = warehouseId;
  }

  public String getWarehouseId() {
    return this.warehouseId;
  }

  @Override
  public String toString() {
    return String.format("SQL Warehouse with warehouse ID {%s}", warehouseId);
  }
}
