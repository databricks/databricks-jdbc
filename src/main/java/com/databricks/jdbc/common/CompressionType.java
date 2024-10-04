package com.databricks.jdbc.common;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;

public enum CompressionType {
  NONE(0),
  LZ4_COMPRESSION(1);

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(CompressionType.class);
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
      LOGGER.debug("Invalid or no compression type provided as input.");
    }
    LOGGER.debug("Defaulting to no compression for fetching results.");
    return NONE;
  }
}
