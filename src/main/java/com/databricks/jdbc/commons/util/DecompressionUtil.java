package com.databricks.jdbc.commons.util;

import com.databricks.jdbc.core.DatabricksParsingException;
import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.jdbc.core.types.CompressionType;
import java.io.IOException;
import java.io.InputStream;
import net.jpountz.lz4.LZ4FrameInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DecompressionUtil {
  private static final Logger LOGGER = LogManager.getLogger(DecompressionUtil.class);

  public static InputStream decompressLZ4Frame(InputStream compressedInputStream, String context)
      throws DatabricksSQLException {
    LOGGER.debug("Decompressing using LZ4 Frame algorithm. Context: {}", context);
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
      LOGGER.debug(
          "Compression/InputStream is `NULL`. Skipping compression for context {}", context);
      return compressedInputStream;
    }
    switch (compressionType) {
      case NONE:
        LOGGER.debug("Compression is `NONE`. Skipping compression for context {}", context);
        return compressedInputStream;
      case LZ4_COMPRESSION:
        return decompressLZ4Frame(compressedInputStream, context);
      default:
        throw new DatabricksSQLException(
            String.format("Unknown compression type: %s. Context : %s", compressionType, context));
    }
  }
}
