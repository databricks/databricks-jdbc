package com.databricks.jdbc.common.util;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtil {
  // Thread-safe singleton instance
  private static final ObjectMapper MAPPER = new ObjectMapper();

  // Use the shared instance for your operations
  public static ObjectMapper getMapper() {
    return MAPPER;
  }
}
