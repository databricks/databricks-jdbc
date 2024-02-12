package com.databricks.jdbc.client.sqlexec;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Create session request */
public class CreateSessionRequest {

  /** Warehouse-Id for session */
  @JsonProperty("warehouse_id")
  private String warehouseId;

  @JsonProperty("initial_schema")
  private String schema;

  @JsonProperty("initial_catalog")
  private String catalog;

  public CreateSessionRequest setWarehouseId(String warehouseId) {
    this.warehouseId = warehouseId;
    return this;
  }

  public String getWarehouseId() {
    return warehouseId;
  }

  public CreateSessionRequest setSchema(String schema) {
    this.schema = schema;
    return this;
  }

  public String getSchema() {
    return schema;
  }

  public CreateSessionRequest setCatalog(String catalog) {
    this.catalog = catalog;
    return this;
  }

  public String getCatalog() {
    return catalog;
  }
}
