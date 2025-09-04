package com.databricks.jdbc.model.telemetry;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TelemetryResponse {

  @JsonProperty("errors")
  List<String> errors;

  @JsonProperty("numSuccess")
  Long numSuccess;

  @JsonProperty("numProtoSuccess")
  Long numProtoSuccess;

  public TelemetryResponse() {}

  public List<String> getErrors() {
    return errors;
  }

  public TelemetryResponse setErrors(List<String> errors) {
    this.errors = errors;
    return this;
  }

  public Long getNumSuccess() {
    return numSuccess;
  }

  public TelemetryResponse setNumSuccess(Long numSuccess) {
    this.numSuccess = numSuccess;
    return this;
  }

  public Long getNumProtoSuccess() {
    return numProtoSuccess;
  }

  public TelemetryResponse setNumProtoSuccess(Long numProtoSuccess) {
    this.numProtoSuccess = numProtoSuccess;
    return this;
  }
}
