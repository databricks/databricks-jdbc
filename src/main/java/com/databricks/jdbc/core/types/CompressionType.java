package com.databricks.jdbc.core.types;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum CompressionType {
  NONE,
  LZ4_COMPRESSION;

  private static final Logger LOGGER = LoggerFactory.getLogger(CompressionType.class);

  public static CompressionType parseCompressionType(String compressionType) {
    int compressionTypeValue = Integer.parseInt(compressionType);
    switch (compressionTypeValue) {
      case 0:
        return CompressionType.NONE;
      case 1:
        return CompressionType.LZ4_COMPRESSION;
      default:
        LOGGER.info("Invalid compression type provided {}. Defaulting to None", compressionType);
        return CompressionType.NONE;
    }
  }
}
