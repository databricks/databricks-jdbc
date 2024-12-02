package com.databricks.jdbc.model.telemetry;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Optional;

public class TelemetryRequest {
  @JsonProperty("uploadTime")
  Long uploadTime;

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

  public Optional<List<String>> getProtoLogs() {
    return protoLogs;
  }

  public TelemetryRequest setProtoLogs(Optional<List<String>> protoLogs) {
    this.protoLogs = protoLogs;
    return this;
  }
}
