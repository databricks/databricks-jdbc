package com.databricks.jdbc.core.types;

import com.databricks.jdbc.client.DatabricksClientType;
import java.util.Objects;

public class Warehouse implements ComputeResource {
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
  public DatabricksClientType getClientType() {
    // This can be overridden by connection property to use Thrift client for DBSQL
    return DatabricksClientType.SQL_EXEC;
  }
}
