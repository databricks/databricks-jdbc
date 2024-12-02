package com.databricks.jdbc.model.telemetry;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ClientContext {

  @JsonProperty("idle_time_millis")
  Long idleTimeMillis;
}
