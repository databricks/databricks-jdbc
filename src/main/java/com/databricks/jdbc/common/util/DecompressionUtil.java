package com.databricks.jdbc.common.util;

import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import net.jpountz.lz4.LZ4FrameInputStream;
import org.apache.commons.io.IOUtils;

public class DecompressionUtil {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DecompressionUtil.class);

  private static byte[] decompressLZ4Frame(byte[] compressedInput, String context)
      throws DatabricksSQLException {
    LOGGER.debug("Decompressing using LZ4 Frame algorithm. Context: {}", context);
    try {
      return IOUtils.toByteArray(
          new LZ4FrameInputStream(new ByteArrayInputStream(compressedInput)));
    } catch (IOException e) {
      String errorMessage =
          String.format("Unable to de-compress LZ4 Frame compressed result %s", context);
      LOGGER.error(e, errorMessage);
      throw new DatabricksParsingException(
          errorMessage, e, DatabricksDriverErrorCode.DECOMPRESSION_ERROR);
    }
  }

  public static byte[] decompress(
      byte[] compressedInput, CompressionCodec compressionCodec, String context)
      throws DatabricksSQLException {
    if (compressionCodec == null || compressedInput == null) {
      LOGGER.debug("Compression is NONE /InputStream is `NULL`. Skipping compression.");
      return compressedInput;
    }
    switch (compressionCodec) {
      case NONE:
        LOGGER.debug("Compression type is `NONE`. Skipping compression.");
        return compressedInput;
      case LZ4_FRAME:
        return decompressLZ4Frame(compressedInput, context);
      default:
        String errorMessage =
            String.format("Unknown compression type: %s. Context : %s", compressionCodec, context);
        LOGGER.error(errorMessage);
        throw new DatabricksSQLException(
            errorMessage, DatabricksDriverErrorCode.DECOMPRESSION_ERROR);
    }
  }

  public static InputStream decompressToStream(
      byte[] compressedInput, CompressionCodec compressionCodec, String context)
      throws DatabricksSQLException {
    byte[] uncompressed = decompress(compressedInput, compressionCodec, context);
    return new ByteArrayInputStream(uncompressed);
  }

  public static InputStream decompress(
      InputStream compressedStream, CompressionCodec compressionCodec, String context)
      throws IOException, DatabricksSQLException {
    if (compressionCodec == null
        || compressionCodec.equals(CompressionCodec.NONE)
        || compressedStream == null) {
      // Save the time to convert to byte array if compression type is none.
      LOGGER.debug("Compression is NONE /InputStream is `NULL`. Skipping compression.");
      return compressedStream;
    }
    byte[] compressedBytes = IOUtils.toByteArray(compressedStream);
    byte[] uncompressedBytes = decompress(compressedBytes, compressionCodec, context);
    return new ByteArrayInputStream(uncompressedBytes);
  }
}
