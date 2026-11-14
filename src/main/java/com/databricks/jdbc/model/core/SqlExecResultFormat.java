package com.databricks.jdbc.model.core;

import com.databricks.sdk.service.sql.Format;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/** Result formats returned by the SQL Execution API, including formats not yet in the SDK. */
public enum SqlExecResultFormat {
  ARROW_STREAM,
  CSV,
  JSON_ARRAY,
  ARROW_BATCH,
  COLUMN_BASED_SET,
  UNKNOWN;

  @JsonCreator
  public static SqlExecResultFormat fromWireValue(String value) {
    if ("6".equals(value)) {
      return COLUMN_BASED_SET;
    }
    if (value != null) {
      for (SqlExecResultFormat format : values()) {
        if (format.name().equals(value)) {
          return format;
        }
      }
    }
    return UNKNOWN;
  }

  /** Converts the SDK's request format without tying response parsing to its smaller enum. */
  public static SqlExecResultFormat fromSdkFormat(Format format) {
    return format == null ? null : fromWireValue(format.name());
  }

  @JsonValue
  public String wireValue() {
    return name();
  }
}
