package com.databricks.jdbc.common.util;

import com.databricks.jdbc.api.IDatabricksGeospatial;
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

  /** Formats a leaf in the public complex-value text view without changing its native value. */
  public static String formatComplexValue(Object value) {
    if (value == null) {
      return "null";
    }
    if (value instanceof String
        || value instanceof Character
        || value instanceof IDatabricksGeospatial
        || DatabricksTypeUtil.isTemporalType(value)) {
      return MAPPER.valueToTree(value.toString()).toString();
    }
    if (value instanceof byte[]) {
      return MAPPER.valueToTree(value).toString(); // JSON binary values use base64.
    }
    // Numbers, JSON nodes, and nested driver containers already expose JSON text.
    return value.toString();
  }

  // Use the shared instance for telemetry operations
  public static ObjectMapper getTelemetryMapper() {
    return TELEMETRY_MAPPER;
  }
}
