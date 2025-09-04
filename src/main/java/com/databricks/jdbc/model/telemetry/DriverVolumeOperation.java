package com.databricks.jdbc.model.telemetry;

import com.databricks.jdbc.model.telemetry.enums.DriverVolumeOperationType;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DriverVolumeOperation {
  @JsonProperty("volume_operation_type")
  DriverVolumeOperationType volumeOperationType;

  @JsonProperty("volume_path")
  String volumePath;

  public DriverVolumeOperation setVolumeOperationType(
      DriverVolumeOperationType volumeOperationType) {
    this.volumeOperationType = volumeOperationType;
    return this;
  }

  public DriverVolumeOperation setVolumePath(String volumePath) {
    this.volumePath = volumePath;
    return this;
  }
}
