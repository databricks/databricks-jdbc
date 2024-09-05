package com.databricks.jdbc.common;

import com.databricks.jdbc.common.util.LoggingUtil;

public enum CompressionType {
  NONE(0),
  LZ4_COMPRESSION(1);
  private final int compressionTypeVal;

  CompressionType(int value) {
    this.compressionTypeVal = value;
  }

  public static CompressionType parseCompressionType(String compressionType) {
    try {
      int value = Integer.parseInt(compressionType);
      for (CompressionType type : values()) {
        if (type.compressionTypeVal == value) {
          return type;
        }
      }
    } catch (NumberFormatException ignored) {
      LoggingUtil.log(LogLevel.DEBUG, "Invalid or no compression type provided as input.");
    }
    LoggingUtil.log(LogLevel.DEBUG, "Defaulting to no compression for fetching results.");
    return NONE;
  }
}
