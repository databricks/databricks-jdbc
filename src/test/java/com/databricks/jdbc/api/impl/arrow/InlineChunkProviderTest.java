package com.databricks.jdbc.api.impl.arrow;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ColumnInfo;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class InlineChunkProviderTest {

  private static final long TOTAL_ROWS = 2L;
  @Mock private ResultData mockResultData;
  @Mock private ResultManifest mockResultManifest;

  @Test
  void testConstructorSuccessfulCreation() throws DatabricksSQLException, IOException {
    // Create valid Arrow data with two rows and one column: [1, 2]
    byte[] arrowData;
    try (BufferAllocator allocator = new RootAllocator()) {
      arrowData = createArrowData(allocator);
    }

    when(mockResultManifest.getTotalRowCount()).thenReturn(TOTAL_ROWS);
    when(mockResultManifest.getResultCompression()).thenReturn(CompressionCodec.NONE);
    // Mock the attachment to be valid arrow data
    when(mockResultData.getAttachment()).thenReturn(arrowData);

    InlineChunkProvider provider = new InlineChunkProvider(mockResultData, mockResultManifest);

    assertTrue(provider.hasNextChunk());
    assertEquals(TOTAL_ROWS, provider.getRowCount());
    assertNotNull(provider.getChunk());

    // Move to next chunk
    assertTrue(provider.next());

    // Get the iterator
    ArrowResultChunkIterator iterator = provider.getChunk().getChunkIterator();
    ColumnInfo intColumnInfo = new ColumnInfo();

    // Verify the data
    assertTrue(iterator.nextRow());
    assertEquals(
        1, iterator.getColumnObjectAtCurrentRow(0, ColumnInfoTypeName.INT, "INT", intColumnInfo));
    assertTrue(iterator.nextRow());
    assertEquals(
        2, iterator.getColumnObjectAtCurrentRow(0, ColumnInfoTypeName.INT, "INT", intColumnInfo));

    // No more chunk
    assertFalse(provider.next());

    verify(mockResultManifest).getTotalRowCount();
    verify(mockResultManifest).getResultCompression();
    verify(mockResultData).getAttachment();
  }

  @Test
  void testConstructorWithLz4CompressedData() throws DatabricksSQLException, IOException {
    // Create valid Arrow data with two rows and one column: [1, 2] and compress it
    byte[] compressedData;
    try (BufferAllocator allocator = new RootAllocator()) {
      compressedData = createLz4CompressedArrowData(createArrowData(allocator));
    }

    when(mockResultManifest.getTotalRowCount()).thenReturn(TOTAL_ROWS);
    when(mockResultManifest.getResultCompression()).thenReturn(CompressionCodec.LZ4_FRAME);
    // Mock the attachment to be valid LZ4 compressed arrow data
    when(mockResultData.getAttachment()).thenReturn(compressedData);

    InlineChunkProvider provider = new InlineChunkProvider(mockResultData, mockResultManifest);

    assertNotNull(provider.getChunk());
    assertEquals(TOTAL_ROWS, provider.getRowCount());
    assertTrue(provider.hasNextChunk());

    // Move to next chunk
    assertTrue(provider.next());

    // Get the iterator
    ArrowResultChunkIterator iterator = provider.getChunk().getChunkIterator();
    ColumnInfo intColumnInfo = new ColumnInfo();

    // Verify the data
    assertTrue(iterator.nextRow());
    assertEquals(
        1, iterator.getColumnObjectAtCurrentRow(0, ColumnInfoTypeName.INT, "INT", intColumnInfo));
    assertTrue(iterator.nextRow());
    assertEquals(
        2, iterator.getColumnObjectAtCurrentRow(0, ColumnInfoTypeName.INT, "INT", intColumnInfo));

    // No more chunk
    assertFalse(provider.next());

    verify(mockResultManifest).getTotalRowCount();
    verify(mockResultManifest).getResultCompression();
    verify(mockResultData).getAttachment();
  }

  @Test
  void testConstructorNullAttachment() {
    when(mockResultManifest.getTotalRowCount()).thenReturn(TOTAL_ROWS);
    when(mockResultManifest.getResultCompression()).thenReturn(CompressionCodec.LZ4_FRAME);
    when(mockResultData.getAttachment()).thenReturn(null);

    // Expect NullPointerException when initialising InlineChunkProvider with null attachment
    assertThrows(
        NullPointerException.class,
        () -> new InlineChunkProvider(mockResultData, mockResultManifest));
  }

  @Test
  void testConstructorChunkIterationBehavior() throws DatabricksSQLException, IOException {
    // Create valid Arrow data and compress it
    byte[] compressedData;
    try (BufferAllocator allocator = new RootAllocator()) {
      compressedData = createLz4CompressedArrowData(createArrowData(allocator));
    }

    when(mockResultManifest.getTotalRowCount()).thenReturn(TOTAL_ROWS);
    when(mockResultManifest.getResultCompression()).thenReturn(CompressionCodec.LZ4_FRAME);
    // Mock the attachment to be valid LZ4 compressed arrow data
    when(mockResultData.getAttachment()).thenReturn(compressedData);

    InlineChunkProvider provider = new InlineChunkProvider(mockResultData, mockResultManifest);

    assertTrue(provider.hasNextChunk(), "Should have next chunk initially");
    assertTrue(provider.next(), "First next() should return true");
    assertFalse(provider.hasNextChunk(), "Should not have next chunk after first next()");
    assertFalse(provider.next(), "Second next() should return false");
    assertEquals(TOTAL_ROWS, provider.getRowCount(), "Row count should match");
  }

  /** Create a simple Arrow data with two rows and one column: [1, 2]. */
  private byte[] createArrowData(BufferAllocator allocator) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    // Create a simple vector with two values
    try (IntVector intVector = new IntVector("numbers", allocator)) {
      intVector.allocateNew(2);
      intVector.set(0, 1);
      intVector.set(1, 2);
      intVector.setValueCount(2);

      // Create VectorSchemaRoot
      VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.of(intVector);

      // Write to output stream
      ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot, null, out);
      writer.start();
      writer.writeBatch();
      writer.end();
    }

    return out.toByteArray();
  }

  /** Create a LZ4 compressed Arrow data. */
  private byte[] createLz4CompressedArrowData(byte[] arrowData) throws IOException {
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    try (LZ4FrameOutputStream lz4Stream = new LZ4FrameOutputStream(byteStream)) {
      lz4Stream.write(arrowData);
    }

    return byteStream.toByteArray();
  }
}
