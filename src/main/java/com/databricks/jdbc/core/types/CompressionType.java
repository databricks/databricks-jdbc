package com.databricks.jdbc.core.types;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public enum CompressionType {
  NONE(0),
  LZ4_COMPRESSION(1);
  private final int compressionTypeVal;
  private static final Logger LOGGER = LogManager.getLogger(CompressionType.class);

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
