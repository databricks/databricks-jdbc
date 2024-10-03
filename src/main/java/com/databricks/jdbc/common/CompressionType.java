package com.databricks.jdbc.common;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.TGetResultSetMetadataResp;

public enum CompressionType {
  NONE(0),
  LZ4_COMPRESSION(1);
  private final int compressionTypeVal;

  CompressionType(int value) {
    this.compressionTypeVal = value;
  }

  public static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(CompressionType.class);

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

  public static CompressionType getCompressionMapping(TGetResultSetMetadataResp metadataResp) {
    if (!metadataResp.isSetLz4Compressed()) {
      return CompressionType.NONE;
    }
    return metadataResp.isLz4Compressed() ? CompressionType.LZ4_COMPRESSION : CompressionType.NONE;
  }
}
