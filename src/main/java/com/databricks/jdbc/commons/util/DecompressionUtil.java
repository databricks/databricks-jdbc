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
    if (compressionType == null || compressedInputStream == null) {
      return compressedInputStream;
    }
    switch (compressionType) {
      case NONE:
        return compressedInputStream;
      case LZ4_COMPRESSION:
        return decompressLZ4Frame(compressedInputStream, context);
      default:
        throw new DatabricksSQLException(
            String.format("Unknown compression type: %s. Context : %s", compressionType, context));
    }
  }
}
