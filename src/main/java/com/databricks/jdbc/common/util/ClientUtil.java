package com.databricks.jdbc.common.util;

import java.util.Map;

public class ClientUtil {

  public static Map<String, String> getHeaders() {
    return Map.of(
        "Accept", "application/json",
        "Content-Type", "application/json");
  }
}
