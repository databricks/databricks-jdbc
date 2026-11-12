package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.exception.DatabricksParsingException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FrameOutputStream;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

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

  private static byte[] checksummedFrame(byte[] data) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (LZ4FrameOutputStream lz4 =
        new LZ4FrameOutputStream(
            out,
            LZ4FrameOutputStream.BLOCKSIZE.SIZE_64KB,
            data.length,
            LZ4FrameOutputStream.FLG.Bits.BLOCK_INDEPENDENCE,
            LZ4FrameOutputStream.FLG.Bits.BLOCK_CHECKSUM,
            LZ4FrameOutputStream.FLG.Bits.CONTENT_CHECKSUM,
            LZ4FrameOutputStream.FLG.Bits.CONTENT_SIZE)) {
      lz4.write(data);
    }
    return out.toByteArray();
  }

  @Test
  public void testDecompressChecksummedMultiBlockFrames() throws Exception {
    byte[] first = new byte[200_000];
    new Random(7).nextBytes(first);
    byte[] second = "second frame".getBytes(StandardCharsets.UTF_8);
    ByteArrayOutputStream concatenated = new ByteArrayOutputStream();
    concatenated.write(checksummedFrame(first));
    concatenated.write(checksummedFrame(second));
    byte[] compressed = concatenated.toByteArray();
    ByteArrayOutputStream expected = new ByteArrayOutputStream();
    expected.write(first);
    expected.write(second);

    assertArrayEquals(
        expected.toByteArray(),
        DecompressionUtil.decompress(compressed, CompressionCodec.LZ4_FRAME, CONTEXT));
    assertArrayEquals(
        expected.toByteArray(),
        IOUtils.toByteArray(
            DecompressionUtil.decompressLazy(compressed, CompressionCodec.LZ4_FRAME, CONTEXT)));

    // Flip a byte of the trailing content checksum.
    byte[] corrupted = checksummedFrame(second);
    corrupted[corrupted.length - 1] ^= 0x1;
    assertThrows(
        DatabricksParsingException.class,
        () -> DecompressionUtil.decompress(corrupted, CompressionCodec.LZ4_FRAME, CONTEXT));
    // The lazy path reports the mismatch when the stream is read.
    assertThrows(
        IOException.class,
        () ->
            IOUtils.toByteArray(
                DecompressionUtil.decompressLazy(corrupted, CompressionCodec.LZ4_FRAME, CONTEXT)));
  }

  @Test
  public void testInlineDecompressionDoesNotLookUpLz4FactoryPerStream() throws Exception {
    byte[] compressed = checksummedFrame("data".getBytes(StandardCharsets.UTF_8));
    // Initialize DecompressionUtil's shared instances before counting lookups.
    DecompressionUtil.decompress(compressed, CompressionCodec.LZ4_FRAME, CONTEXT);
    try (MockedStatic<LZ4Factory> factory =
        Mockito.mockStatic(LZ4Factory.class, Mockito.CALLS_REAL_METHODS)) {
      DecompressionUtil.decompress(compressed, CompressionCodec.LZ4_FRAME, CONTEXT);
      IOUtils.toByteArray(
          DecompressionUtil.decompressLazy(compressed, CompressionCodec.LZ4_FRAME, CONTEXT));
      factory.verify(LZ4Factory::fastestInstance, Mockito.never());
    }
  }

  @Test
  public void testInlineDecompressionDoesNotLookUpXxHashFactoryPerStream() throws Exception {
    // Without CONTENT_CHECKSUM, LZ4FrameInputStream.readHeader() needs no XXHashFactory lookup, so
    // any lookup would come from building a per-stream header checksum.
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (LZ4FrameOutputStream lz4 =
        new LZ4FrameOutputStream(
            out,
            LZ4FrameOutputStream.BLOCKSIZE.SIZE_64KB,
            LZ4FrameOutputStream.FLG.Bits.BLOCK_INDEPENDENCE)) {
      lz4.write("data".getBytes(StandardCharsets.UTF_8));
    }
    byte[] compressed = out.toByteArray();
    DecompressionUtil.decompress(compressed, CompressionCodec.LZ4_FRAME, CONTEXT);
    try (MockedStatic<XXHashFactory> factory =
        Mockito.mockStatic(XXHashFactory.class, Mockito.CALLS_REAL_METHODS)) {
      assertArrayEquals(
          "data".getBytes(StandardCharsets.UTF_8),
          DecompressionUtil.decompress(compressed, CompressionCodec.LZ4_FRAME, CONTEXT));
      IOUtils.toByteArray(
          DecompressionUtil.decompressLazy(compressed, CompressionCodec.LZ4_FRAME, CONTEXT));
      factory.verify(XXHashFactory::fastestInstance, Mockito.never());
    }
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

  @Test
  public void testDecompressLazyLZ4Frame() throws Exception {
    byte[] uncompressed = INITIAL_STRING.getBytes();
    byte[] compressed = compressBytes(uncompressed);

    InputStream result =
        DecompressionUtil.decompressLazy(compressed, CompressionCodec.LZ4_FRAME, CONTEXT);
    assertNotNull(result);
    assertTrue(IOUtils.contentEquals(result, new ByteArrayInputStream(uncompressed)));
  }

  @Test
  public void testDecompressLazyNoneCodec() throws Exception {
    byte[] data = INITIAL_STRING.getBytes();
    InputStream result = DecompressionUtil.decompressLazy(data, CompressionCodec.NONE, CONTEXT);
    assertNotNull(result);
    assertTrue(IOUtils.contentEquals(result, new ByteArrayInputStream(data)));
  }

  @Test
  public void testDecompressLazyNullCodec() throws Exception {
    byte[] data = INITIAL_STRING.getBytes();
    InputStream result = DecompressionUtil.decompressLazy(data, null, CONTEXT);
    assertNotNull(result);
    assertTrue(IOUtils.contentEquals(result, new ByteArrayInputStream(data)));
  }

  @Test
  public void testDecompressLazyNullInput() throws Exception {
    InputStream result =
        DecompressionUtil.decompressLazy(null, CompressionCodec.LZ4_FRAME, CONTEXT);
    assertNull(result);
  }

  @Test
  public void testDecompressLazyNullInputNullCodec() throws Exception {
    InputStream result = DecompressionUtil.decompressLazy(null, null, CONTEXT);
    assertNull(result);
  }

  @Test
  public void testDecompressLazyHighlyCompressibleData() throws Exception {
    // Large repetitive data — high compression ratio
    byte[] largeData = new byte[1_000_000];
    java.util.Arrays.fill(largeData, (byte) 'A');
    byte[] compressed = compressBytes(largeData);

    // Compressed should be much smaller
    assertTrue(
        compressed.length < largeData.length / 10,
        "Compressed size should be much smaller for repetitive data");

    InputStream result =
        DecompressionUtil.decompressLazy(compressed, CompressionCodec.LZ4_FRAME, CONTEXT);
    assertNotNull(result);
    byte[] decompressed = IOUtils.toByteArray(result);
    assertArrayEquals(largeData, decompressed);
  }

  @Test
  public void testDecompressLazyCorruptedDataFailsOnRead() throws Exception {
    byte[] corrupted = new byte[] {0x01, 0x02, 0x03, 0x04};
    // decompressLazy is lazy — creation succeeds, but reading the stream fails
    InputStream stream =
        DecompressionUtil.decompressLazy(corrupted, CompressionCodec.LZ4_FRAME, CONTEXT);
    assertNotNull(stream);
    assertThrows(IOException.class, () -> IOUtils.toByteArray(stream));
  }

  private static byte[] compressBytes(byte[] data) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (LZ4FrameOutputStream lz4 = new LZ4FrameOutputStream(out)) {
      lz4.write(data);
    }
    return out.toByteArray();
  }
}
