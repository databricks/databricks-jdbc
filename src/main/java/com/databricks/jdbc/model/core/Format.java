package com.databricks.jdbc.model.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/** SQL Execution API result formats, including the APC-only native Arrow-batch shape. */
public enum Format {
  ARROW_STREAM,
  ARROW_BATCH,
  COLUMN_BASED_SET,
  CSV,
  JSON_ARRAY,
  UNKNOWN;

  @JsonCreator
  public static Format fromWireValue(String value) {
    if ("6".equals(value)) {
      return COLUMN_BASED_SET;
    }
    if (value != null) {
      for (Format format : values()) {
        if (format.name().equals(value)) {
          return format;
        }
      }
    }
    return UNKNOWN;
  }

  @JsonValue
  public String wireValue() {
    return name();
  }
}
