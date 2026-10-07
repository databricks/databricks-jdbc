package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.common.CompressionCodec;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class DecompressionUtilTest {
  private static final String CONTEXT = "testContext";
  private static final String INITIAL_STRING = "testData";
  private static InputStream compressedInputStream;

  private static DecompressionUtil decompressionUtil = new DecompressionUtil();

  @BeforeAll
  public static void setCompressedInputStream() throws IOException {
    byte[] uncompressedData = INITIAL_STRING.getBytes();
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try (LZ4FrameOutputStream lz4FrameOutputStream =
        new LZ4FrameOutputStream(byteArrayOutputStream)) {
      lz4FrameOutputStream.write(uncompressedData);
    }
    compressedInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
  }

  @Test
  public void testDecompressLZ4Frame() throws Exception {
    InputStream resultStream =
        decompressionUtil.decompress(compressedInputStream, CompressionCodec.LZ4_FRAME, CONTEXT);
    assertNotNull(resultStream, "The decompressed stream should not be null.");
    assertTrue(
        IOUtils.contentEquals(resultStream, new ByteArrayInputStream(INITIAL_STRING.getBytes())));
  }

  @Test
  public void testDecompressToInputStreamLZ4Frame() throws Exception {
    byte[] uncompressed = INITIAL_STRING.getBytes();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (LZ4FrameOutputStream lz4 = new LZ4FrameOutputStream(out)) {
      lz4.write(uncompressed);
    }
    InputStream resultStream =
        DecompressionUtil.decompressToInputStream(
            out.toByteArray(), CompressionCodec.LZ4_FRAME, CONTEXT);
    assertTrue(
        IOUtils.contentEquals(resultStream, new ByteArrayInputStream(uncompressed)),
        "Streaming decompression should yield the original bytes");
  }

  @Test
  public void testDecompressToInputStreamNoneReturnsRawBytes() throws Exception {
    byte[] raw = INITIAL_STRING.getBytes();
    InputStream resultStream =
        DecompressionUtil.decompressToInputStream(raw, CompressionCodec.NONE, CONTEXT);
    assertTrue(IOUtils.contentEquals(resultStream, new ByteArrayInputStream(raw)));
  }

  @Test
  public void testDecompressToInputStreamNullCodecReturnsRawBytes() throws Exception {
    // A null codec is treated as no compression: the raw bytes are returned unchanged.
    byte[] raw = INITIAL_STRING.getBytes();
    InputStream resultStream = DecompressionUtil.decompressToInputStream(raw, null, CONTEXT);
    assertTrue(IOUtils.contentEquals(resultStream, new ByteArrayInputStream(raw)));
  }

  @Test
  public void testDecompressToInputStreamMalformedLz4FailsOnRead() throws Exception {
    // LZ4FrameInputStream validates the frame lazily as it is read, so decompressToInputStream
    // returns the streaming decorator successfully and the IOException surfaces on read. The
    // caller (ArrowResultChunk.downloadData) catches it and routes it through handleFailure.
    byte[] notLz4 = "this-is-not-a-valid-lz4-frame-payload".getBytes();
    InputStream resultStream =
        DecompressionUtil.decompressToInputStream(notLz4, CompressionCodec.LZ4_FRAME, CONTEXT);
    assertNotNull(resultStream, "A streaming decorator should be returned before any read");
    assertThrows(IOException.class, () -> IOUtils.toByteArray(resultStream));
  }

  @Test
  public void testDecompressLZ4FrameSkipsCompression() throws Exception {
    assertEquals(
        decompressionUtil.decompress(compressedInputStream, CompressionCodec.NONE, CONTEXT),
        compressedInputStream);
    assertNull(
        DecompressionUtil.decompress(
            (ByteArrayInputStream) null, CompressionCodec.LZ4_FRAME, CONTEXT));
  }
}
