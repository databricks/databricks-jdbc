package com.databricks.jdbc.commons.util;

import com.databricks.jdbc.commons.LogLevel;
import com.databricks.jdbc.core.DatabricksParsingException;
import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.jdbc.core.types.CompressionType;
import java.io.IOException;
import java.io.InputStream;
import net.jpountz.lz4.LZ4FrameInputStream;

public class DecompressionUtil {

  public static InputStream decompressLZ4Frame(InputStream compressedInputStream, String context)
      throws DatabricksSQLException {
    LoggingUtil.log(LogLevel.DEBUG, "Decompressing using LZ4 Frame algorithm. Context: " + context);
    try {
      return new LZ4FrameInputStream(compressedInputStream);
    } catch (IOException e) {
      String errorMessage =
          String.format("Unable to de-compress LZ4 Frame compressed result %s", context);
      LoggingUtil.log(LogLevel.ERROR, errorMessage + e.getMessage());
      throw new DatabricksParsingException(errorMessage, e);
    }
  }

  public static InputStream decompress(
      InputStream compressedInputStream, CompressionType compressionType, String context)
      throws DatabricksSQLException {
    System.out.println(
        "SAINANEELOGS7: DecompressionUtil: decompress: compressedInputStream: "
            + compressedInputStream
            + ", compressionType: "
            + compressionType
            + ", context: "
            + context);
    if (compressionType == null || compressedInputStream == null) {
      LoggingUtil.log(
          LogLevel.DEBUG, "Compression/InputStream is `NULL`. Skipping compression.", context);
      return compressedInputStream;
    }
    switch (compressionType) {
      case NONE:
        LoggingUtil.log(
            LogLevel.DEBUG, "Compression/InputStream is `NULL`. Skipping compression.", context);
        return compressedInputStream;
      case LZ4_COMPRESSION:
        return decompressLZ4Frame(compressedInputStream, context);
      default:
        String errorMessage =
            String.format("Unknown compression type: %s. Context : %s", compressionType, context);
        LoggingUtil.log(LogLevel.ERROR, errorMessage);
        throw new DatabricksSQLException(errorMessage);
    }
  }
}
