package com.databricks.jdbc.commons.util;

import java.io.IOException;
import java.io.InputStream;
import net.jpountz.lz4.LZ4FrameInputStream;

public class DecompressionUtil {
  public InputStream decompress(InputStream compressedInputStream) throws IOException {
    return new LZ4FrameInputStream(compressedInputStream);
  }
}
