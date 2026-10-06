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

  /**
   * Returns a stream that decompresses {@code compressedInput} lazily as it is read, so the full
   * decompressed payload is never materialized alongside the compressed bytes. Only LZ4_FRAME is
   * wrapped in a decompressing decorator; a {@code null}/NONE codec returns the raw bytes as-is.
   *
   * <p>Any {@link IOException} from constructing the LZ4 decoder propagates to the caller ({@code
   * ArrowResultChunk.downloadData}), which handles it as a chunk-processing failure — the same
   * terminal path as any other decompression error.
   */
  public static InputStream decompressToInputStream(
      byte[] compressedInput, CompressionCodec compressionCodec, String context)
      throws IOException {
    if (compressionCodec == CompressionCodec.LZ4_FRAME) {
      LOGGER.debug("Streaming LZ4 Frame decompression. Context: {}", context);
      return new LZ4FrameInputStream(new ByteArrayInputStream(compressedInput));
    }
    // null / NONE codec: no decompression needed.
    return new ByteArrayInputStream(compressedInput);
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
