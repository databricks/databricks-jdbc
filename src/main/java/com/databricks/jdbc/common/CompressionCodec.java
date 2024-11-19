package com.databricks.jdbc.common;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.TGetResultSetMetadataResp;

public enum CompressionCodec {
  NONE(0),
  LZ4_FRAME(1);

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(CompressionCodec.class);
  private final int compressionTypeVal;

  CompressionCodec(int value) {
    this.compressionTypeVal = value;
  }

  public static CompressionCodec parseCompressionType(String compressionType) {
    try {
      int value = Integer.parseInt(compressionType);
      for (CompressionCodec type : values()) {
        if (type.compressionTypeVal == value) {
          return type;
        }
      }
    } catch (NumberFormatException ignored) {
      LOGGER.trace("Invalid or no compression type provided as input.");
    }
    LOGGER.trace("Defaulting to LZ4_FRAME compression for fetching results.");
    return LZ4_FRAME;
  }

  public static CompressionCodec getCompressionMapping(TGetResultSetMetadataResp metadataResp) {
    if (!metadataResp.isSetLz4Compressed()) {
      return CompressionCodec.NONE;
    }
    return metadataResp.isLz4Compressed() ? CompressionCodec.LZ4_FRAME : CompressionCodec.NONE;
  }
}
