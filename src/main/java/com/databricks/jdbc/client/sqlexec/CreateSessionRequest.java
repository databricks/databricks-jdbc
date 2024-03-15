package com.databricks.jdbc.client.sqlexec;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

/** Create session request */
public class CreateSessionRequest {

  /** Warehouse-Id for session */
  @JsonProperty("warehouse_id")
  private String warehouseId;

  @JsonProperty("schema")
  private String schema;

  @JsonProperty("catalog")
  private String catalog;

  @JsonProperty("session_confs")
  private Map<String, String> sessionConfigs;

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

  public CreateSessionRequest setSessionConfigs(Map<String, String> sessionConfigs) {
    this.sessionConfigs = sessionConfigs;
    return this;
  }

  public Map<String, String> getSessionConfigs() {
    return sessionConfigs;
  }
}
