package com.databricks.jdbc.model.telemetry;

import static java.util.Collections.EMPTY_LIST;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class TelemetryRequest {
  @JsonProperty("uploadTime")
  Long uploadTime;

  @JsonProperty("items")
  List<String> items =
      EMPTY_LIST; // We only care about protoLogs, but items is not an optional field.

  @JsonProperty("protoLogs")
  List<String> protoLogs;

  public TelemetryRequest() {}

  public Long getUploadTime() {
    return uploadTime;
  }

  public TelemetryRequest setUploadTime(Long uploadTime) {
    this.uploadTime = uploadTime;
    return this;
  }

  public List<String> getProtoLogs() {
    return protoLogs;
  }

  public TelemetryRequest setProtoLogs(List<String> protoLogs) {
    this.protoLogs = protoLogs;
    return this;
  }
}
