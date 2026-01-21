package com.databricks.jdbc.common.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtil {
  // Thread-safe singleton instance
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final ObjectMapper TELEMETRY_MAPPER =
      new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

  // Use the shared instance for driver operations
  public static ObjectMapper getMapper() {
    return MAPPER;
  }

  // Use the shared instance for telemetry operations
  public static ObjectMapper getTelemetryMapper() {
    return TELEMETRY_MAPPER;
  }
}
