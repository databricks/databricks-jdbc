package com.databricks.jdbc.common.util;

import com.databricks.jdbc.common.CompressionType;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import net.jpountz.lz4.LZ4FrameInputStream;
import org.apache.commons.io.IOUtils;

public class DecompressionUtil {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DecompressionUtil.class);

  private static byte[] decompressLZ4Frame(byte[] compressedInput, String context)
      throws DatabricksSQLException {
    LOGGER.debug("Decompressing using LZ4 Frame algorithm. Context: " + context);
    try {
      return IOUtils.toByteArray(
          new LZ4FrameInputStream(new ByteArrayInputStream(compressedInput)));
    } catch (IOException e) {
      String errorMessage =
          String.format("Unable to de-compress LZ4 Frame compressed result %s", context);
      LOGGER.error(e, errorMessage + e.getMessage());
      throw new DatabricksParsingException(errorMessage, e);
    }
  }

  public static byte[] decompress(
      byte[] compressedInput, CompressionType compressionType, String context)
      throws DatabricksSQLException {
    if (compressedInput == null) {
      LOGGER.debug("compressedInputBytes is `NULL`. Skipping compression.");
      return compressedInput;
    }
    switch (compressionType) {
      case NONE:
        LOGGER.debug("Compression type is `NONE`. Skipping compression.");
        return compressedInput;
      case LZ4_COMPRESSION:
        return decompressLZ4Frame(compressedInput, context);
      default:
        String errorMessage =
            String.format("Unknown compression type: %s. Context : %s", compressionType, context);
        LOGGER.error(errorMessage);
        throw new DatabricksSQLException(errorMessage);
    }
  }

  public static InputStream decompress(
      InputStream compressedStream, CompressionType compressionType, String context)
      throws IOException, DatabricksSQLException {
    if (compressionType.equals(CompressionType.NONE) || compressedStream == null) {
      // Save the time to convert to byte array if compression type is none.
      LOGGER.debug("Compression is NONE /InputStream is `NULL`. Skipping compression.");
      return compressedStream;
    }
    byte[] compressedBytes = IOUtils.toByteArray(compressedStream);
    byte[] uncompressedBytes = decompress(compressedBytes, compressionType, context);
    return new ByteArrayInputStream(uncompressedBytes);
  }
}
