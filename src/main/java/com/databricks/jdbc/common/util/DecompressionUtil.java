package com.databricks.jdbc.common.util;

import com.databricks.jdbc.common.CompressionType;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import net.jpountz.lz4.LZ4FrameInputStream;

public class DecompressionUtil {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DecompressionUtil.class);

  public static InputStream decompressLZ4Frame(InputStream compressedInputStream, String context)
      throws DatabricksSQLException {
    LOGGER.debug("Decompressing using LZ4 Frame algorithm. Context: " + context);
    try {
      return new LZ4FrameInputStream(compressedInputStream);
    } catch (IOException e) {
      String errorMessage =
          String.format("Unable to de-compress LZ4 Frame compressed result %s", context);
      LOGGER.error(errorMessage + e.getMessage());
      throw new DatabricksParsingException(errorMessage, e);
    }
  }

  public static InputStream decompress(
      InputStream compressedInputStream, CompressionType compressionType, String context)
      throws DatabricksSQLException {
    if (compressionType == null || compressedInputStream == null) {
      LOGGER.debug("Compression/InputStream is `NULL`. Skipping compression.");
      return compressedInputStream;
    }
    switch (compressionType) {
      case NONE:
        LOGGER.debug("Compression/InputStream is `NULL`. Skipping compression.");
        return compressedInputStream;
      case LZ4_COMPRESSION:
        return decompressLZ4Frame(compressedInputStream, context);
      default:
        String errorMessage =
            String.format("Unknown compression type: %s. Context : %s", compressionType, context);
        LOGGER.error(errorMessage);
        throw new DatabricksSQLException(errorMessage);
    }
  }
}
