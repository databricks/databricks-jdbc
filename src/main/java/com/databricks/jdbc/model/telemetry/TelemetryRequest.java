package com.databricks.jdbc.model.telemetry;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Optional;

public class TelemetryRequest {
  @JsonProperty("uploadTime")
  Long uploadTime;

  @JsonProperty("items")
  List<String> items;

  @JsonProperty("protoLogs")
  Optional<List<String>> protoLogs;

  public TelemetryRequest() {}

  public Long getUploadTime() {
    return uploadTime;
  }

  public TelemetryRequest setUploadTime(Long uploadTime) {
    this.uploadTime = uploadTime;
    return this;
  }

  public List<String> getItems() {
    return items;
  }

  public TelemetryRequest setItems(List<String> items) {
    this.items = items;
    return this;
  }

  public Optional<List<String>> getProtoLogs() {
    return protoLogs;
  }

  public TelemetryRequest setProtoLogs(Optional<List<String>> protoLogs) {
    this.protoLogs = protoLogs;
    return this;
  }
}
