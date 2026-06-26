package com.databricks.jdbc.api.impl.arrow;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.dbclient.impl.sqlexec.DatabricksSdkClient;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ColumnInfo;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SeaInlineArrowChunkProviderTest {

  private static final StatementId STATEMENT_ID = new StatementId("test-statement-id");
  private static final int DEFAULT_MAX_BATCHES = 4;
  private static final int DEFAULT_CHUNK_TIMEOUT = 5;

  @Mock private IDatabricksSession mockSession;
  @Mock private IDatabricksConnectionContext mockConnectionContext;
  @Mock private DatabricksSdkClient mockSdkClient;

  private SeaInlineArrowChunkProvider provider;

  @AfterEach
  void tearDown() {
    if (provider != null && !provider.isClosed()) {
      provider.close();
    }
  }

  // ==================== Test Helpers ====================

  private void setupSessionMocks() {
    lenient().when(mockSession.getConnectionContext()).thenReturn(mockConnectionContext);
    lenient().when(mockSession.getDatabricksClient()).thenReturn(mockSdkClient);
    lenient()
        .when(mockConnectionContext.getThriftMaxBatchesInMemory())
        .thenReturn(DEFAULT_MAX_BATCHES);
    lenient()
        .when(mockConnectionContext.getChunkReadyTimeoutSeconds())
        .thenReturn(DEFAULT_CHUNK_TIMEOUT);
  }

  private static byte[] createArrowData(int... values) throws IOException {
    try (BufferAllocator allocator = new RootAllocator()) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (IntVector intVector = new IntVector("numbers", allocator)) {
        intVector.allocateNew(values.length);
        for (int i = 0; i < values.length; i++) {
          intVector.set(i, values[i]);
        }
        intVector.setValueCount(values.length);

        VectorSchemaRoot root = VectorSchemaRoot.of(intVector);
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out);
        writer.start();
        writer.writeBatch();
        writer.end();
      }
      return out.toByteArray();
    }
  }

  private static byte[] compressLz4(byte[] data) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (LZ4FrameOutputStream lz4 = new LZ4FrameOutputStream(out)) {
      lz4.write(data);
    }
    return out.toByteArray();
  }

  private static byte[] createHighlyCompressibleArrowData(int rowCount) throws IOException {
    try (BufferAllocator allocator = new RootAllocator()) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (IntVector intVector = new IntVector("numbers", allocator)) {
        intVector.allocateNew(rowCount);
        for (int i = 0; i < rowCount; i++) {
          intVector.set(i, 42); // all same value = highly compressible
        }
        intVector.setValueCount(rowCount);

        VectorSchemaRoot root = VectorSchemaRoot.of(intVector);
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out);
        writer.start();
        writer.writeBatch();
        writer.end();
      }
      return out.toByteArray();
    }
  }

  private ResultData createResultData(
      byte[] attachment, Long rowCount, Long rowOffset, Long nextChunkIndex) {
    ResultData rd = new ResultData();
    rd.setAttachment(attachment);
    rd.setRowCount(rowCount);
    rd.setRowOffset(rowOffset);
    rd.setNextChunkIndex(nextChunkIndex);
    return rd;
  }

  private ResultManifest createManifest(CompressionCodec codec) {
    ResultManifest manifest = new ResultManifest();
    manifest.setResultCompression(codec);
    return manifest;
  }

  private SeaInlineArrowChunkProvider createProvider(ResultData initialData, CompressionCodec codec)
      throws DatabricksSQLException {
    setupSessionMocks();
    return new SeaInlineArrowChunkProvider(
        initialData, createManifest(codec), STATEMENT_ID, mockSession);
  }

  // ==================== Category 1: Single Chunk (No Prefetch) ====================

  @Nested
  @DisplayName("Category 1: Single Chunk (No Prefetch)")
  class SingleChunkTests {

    @Test
    @DisplayName("Should handle single uncompressed chunk")
    void testSingleChunkUncompressed() throws Exception {
      byte[] arrowData = createArrowData(10, 20);
      ResultData initial = createResultData(arrowData, 2L, 0L, null);

      provider = createProvider(initial, CompressionCodec.NONE);

      assertTrue(provider.hasNextChunk());
      assertTrue(provider.next());
      assertNotNull(provider.getChunk());

      ArrowResultChunkIterator iterator = provider.getChunk().getChunkIterator();
      ColumnInfo colInfo = new ColumnInfo();
      assertTrue(iterator.nextRow());
      assertEquals(
          10, iterator.getColumnObjectAtCurrentRow(0, ColumnInfoTypeName.INT, "INT", colInfo));
      assertTrue(iterator.nextRow());
      assertEquals(
          20, iterator.getColumnObjectAtCurrentRow(0, ColumnInfoTypeName.INT, "INT", colInfo));
      assertFalse(iterator.nextRow());

      assertFalse(provider.hasNextChunk());
      assertFalse(provider.next());
      assertEquals(2L, provider.getRowCount());
    }

    @Test
    @DisplayName("Should handle single LZ4-compressed chunk")
    void testSingleChunkCompressed() throws Exception {
      byte[] arrowData = createArrowData(100, 200, 300);
      byte[] compressed = compressLz4(arrowData);
      ResultData initial = createResultData(compressed, 3L, 0L, null);

      provider = createProvider(initial, CompressionCodec.LZ4_FRAME);

      assertTrue(provider.hasNextChunk());
      assertTrue(provider.next());

      ArrowResultChunkIterator iterator = provider.getChunk().getChunkIterator();
      ColumnInfo colInfo = new ColumnInfo();
      assertTrue(iterator.nextRow());
      assertEquals(
          100, iterator.getColumnObjectAtCurrentRow(0, ColumnInfoTypeName.INT, "INT", colInfo));
      assertTrue(iterator.nextRow());
      assertEquals(
          200, iterator.getColumnObjectAtCurrentRow(0, ColumnInfoTypeName.INT, "INT", colInfo));
      assertTrue(iterator.nextRow());
      assertEquals(
          300, iterator.getColumnObjectAtCurrentRow(0, ColumnInfoTypeName.INT, "INT", colInfo));
      assertFalse(iterator.nextRow());

      assertFalse(provider.hasNextChunk());
      assertEquals(3L, provider.getRowCount());
    }

    @Test
    @DisplayName("Should return null from getChunk() before first next()")
    void testGetChunkBeforeNext() throws Exception {
      byte[] arrowData = createArrowData(1);
      ResultData initial = createResultData(arrowData, 1L, 0L, null);

      provider = createProvider(initial, CompressionCodec.NONE);

      assertNull(provider.getChunk());
      assertTrue(provider.next());
      assertNotNull(provider.getChunk());
    }

    @Test
    @DisplayName("Should report correct chunk count")
    void testChunkCount() throws Exception {
      byte[] arrowData = createArrowData(1);
      ResultData initial = createResultData(arrowData, 1L, 0L, null);

      provider = createProvider(initial, CompressionCodec.NONE);

      assertEquals(0, provider.getChunkCount());
      assertTrue(provider.next());
      assertEquals(1, provider.getChunkCount());
      assertFalse(provider.next());
      assertEquals(1, provider.getChunkCount());
    }
  }

  // ==================== Category 2: Multi-Chunk Iteration ====================

  @Nested
  @DisplayName("Category 2: Multi-Chunk Iteration")
  class MultiChunkTests {

    @Test
    @DisplayName("Should iterate through multiple uncompressed chunks")
    void testMultipleChunksUncompressed() throws Exception {
      byte[] chunk0Data = createArrowData(1, 2);
      byte[] chunk1Data = createArrowData(3, 4);
      byte[] chunk2Data = createArrowData(5, 6);

      ResultData initial = createResultData(chunk0Data, 2L, 0L, 1L);
      ResultData chunk1 = createResultData(chunk1Data, 2L, 2L, 2L);
      ResultData chunk2 = createResultData(chunk2Data, 2L, 4L, null);

      setupSessionMocks();
      when(mockSdkClient.getResultChunksData(eq(STATEMENT_ID), eq(1L), eq(2L))).thenReturn(chunk1);
      when(mockSdkClient.getResultChunksData(eq(STATEMENT_ID), eq(2L), eq(4L))).thenReturn(chunk2);

      provider =
          new SeaInlineArrowChunkProvider(
              initial, createManifest(CompressionCodec.NONE), STATEMENT_ID, mockSession);

      ColumnInfo colInfo = new ColumnInfo();
      int[] expectedValues = {1, 2, 3, 4, 5, 6};
      int valueIndex = 0;

      for (int chunkIdx = 0; chunkIdx < 3; chunkIdx++) {
        assertTrue(provider.hasNextChunk(), "Should have chunk " + chunkIdx);
        assertTrue(provider.next(), "next() should succeed for chunk " + chunkIdx);

        ArrowResultChunkIterator iterator = provider.getChunk().getChunkIterator();
        while (iterator.nextRow()) {
          assertEquals(
              expectedValues[valueIndex],
              iterator.getColumnObjectAtCurrentRow(0, ColumnInfoTypeName.INT, "INT", colInfo),
              "Value at index " + valueIndex);
          valueIndex++;
        }
      }

      assertEquals(6, valueIndex, "Should have read all values");
      assertFalse(provider.hasNextChunk());
      assertEquals(6L, provider.getRowCount());
    }

    @Test
    @DisplayName("Should iterate through multiple LZ4-compressed chunks")
    void testMultipleChunksCompressed() throws Exception {
      byte[] chunk0Data = compressLz4(createArrowData(10, 20));
      byte[] chunk1Data = compressLz4(createArrowData(30, 40));

      ResultData initial = createResultData(chunk0Data, 2L, 0L, 1L);
      ResultData chunk1Result = createResultData(chunk1Data, 2L, 2L, null);

      setupSessionMocks();
      when(mockSdkClient.getResultChunksData(eq(STATEMENT_ID), eq(1L), eq(2L)))
          .thenReturn(chunk1Result);

      provider =
          new SeaInlineArrowChunkProvider(
              initial, createManifest(CompressionCodec.LZ4_FRAME), STATEMENT_ID, mockSession);

      ColumnInfo colInfo = new ColumnInfo();

      assertTrue(provider.next());
      ArrowResultChunkIterator it0 = provider.getChunk().getChunkIterator();
      assertTrue(it0.nextRow());
      assertEquals(10, it0.getColumnObjectAtCurrentRow(0, ColumnInfoTypeName.INT, "INT", colInfo));
      assertTrue(it0.nextRow());
      assertEquals(20, it0.getColumnObjectAtCurrentRow(0, ColumnInfoTypeName.INT, "INT", colInfo));

      assertTrue(provider.next());
      ArrowResultChunkIterator it1 = provider.getChunk().getChunkIterator();
      assertTrue(it1.nextRow());
      assertEquals(30, it1.getColumnObjectAtCurrentRow(0, ColumnInfoTypeName.INT, "INT", colInfo));
      assertTrue(it1.nextRow());
      assertEquals(40, it1.getColumnObjectAtCurrentRow(0, ColumnInfoTypeName.INT, "INT", colInfo));

      assertFalse(provider.hasNextChunk());
      assertEquals(4L, provider.getRowCount());
    }

    @Test
    @DisplayName("Should accumulate total row count across chunks")
    void testRowCountAccumulation() throws Exception {
      byte[] chunk0Data = createArrowData(1, 2, 3);
      byte[] chunk1Data = createArrowData(4, 5);
      byte[] chunk2Data = createArrowData(6);

      ResultData initial = createResultData(chunk0Data, 3L, 0L, 1L);
      ResultData chunk1Result = createResultData(chunk1Data, 2L, 3L, 2L);
      ResultData chunk2Result = createResultData(chunk2Data, 1L, 5L, null);

      setupSessionMocks();
      when(mockSdkClient.getResultChunksData(eq(STATEMENT_ID), eq(1L), eq(3L)))
          .thenReturn(chunk1Result);
      when(mockSdkClient.getResultChunksData(eq(STATEMENT_ID), eq(2L), eq(5L)))
          .thenReturn(chunk2Result);

      provider =
          new SeaInlineArrowChunkProvider(
              initial, createManifest(CompressionCodec.NONE), STATEMENT_ID, mockSession);

      // Initial row count from first chunk
      assertEquals(3L, provider.getRowCount());

      // Consume all chunks, let prefetch accumulate
      while (provider.hasNextChunk()) {
        assertTrue(provider.next());
        assertNotNull(provider.getChunk());
      }

      assertEquals(6L, provider.getRowCount());
    }

    @Test
    @DisplayName("Should pass correct row_offset in each GetResultData call")
    void testRowOffsetProgression() throws Exception {
      byte[] chunk0Data = createArrowData(1, 2, 3); // 3 rows
      byte[] chunk1Data = createArrowData(4, 5); // 2 rows
      byte[] chunk2Data = createArrowData(6); // 1 row

      ResultData initial = createResultData(chunk0Data, 3L, 0L, 1L);
      ResultData chunk1Result = createResultData(chunk1Data, 2L, 3L, 2L);
      ResultData chunk2Result = createResultData(chunk2Data, 1L, 5L, null);

      setupSessionMocks();
      when(mockSdkClient.getResultChunksData(eq(STATEMENT_ID), eq(1L), eq(3L)))
          .thenReturn(chunk1Result);
      when(mockSdkClient.getResultChunksData(eq(STATEMENT_ID), eq(2L), eq(5L)))
          .thenReturn(chunk2Result);

      provider =
          new SeaInlineArrowChunkProvider(
              initial, createManifest(CompressionCodec.NONE), STATEMENT_ID, mockSession);

      while (provider.hasNextChunk()) {
        assertTrue(provider.next());
        assertNotNull(provider.getChunk());
      }

      // Verify exact calls: chunk 1 at offset 3, chunk 2 at offset 5
      verify(mockSdkClient).getResultChunksData(STATEMENT_ID, 1L, 3L);
      verify(mockSdkClient).getResultChunksData(STATEMENT_ID, 2L, 5L);
      verifyNoMoreInteractions(mockSdkClient);
    }
  }

  // ==================== Category 3: Streaming Decompression ====================

  @Nested
  @DisplayName("Category 3: Streaming Decompression (OOM fix)")
  class StreamingDecompressionTests {

    @Test
    @DisplayName("Should handle highly compressible data without OOM (streaming decompression)")
    void testHighlyCompressibleData() throws Exception {
      // 10,000 identical values — compresses to a tiny payload
      byte[] arrowData = createHighlyCompressibleArrowData(10_000);
      byte[] compressed = compressLz4(arrowData);

      // Verify compression ratio is high (the scenario that causes OOM with eager decompress)
      assertTrue(
          compressed.length < arrowData.length / 5,
          "Compressed size ("
              + compressed.length
              + ") should be much smaller than raw ("
              + arrowData.length
              + ")");

      ResultData initial = createResultData(compressed, 10_000L, 0L, null);
      provider = createProvider(initial, CompressionCodec.LZ4_FRAME);

      assertTrue(provider.next());
      ArrowResultChunkIterator iterator = provider.getChunk().getChunkIterator();

      int count = 0;
      ColumnInfo colInfo = new ColumnInfo();
      while (iterator.nextRow()) {
        assertEquals(
            42, iterator.getColumnObjectAtCurrentRow(0, ColumnInfoTypeName.INT, "INT", colInfo));
        count++;
      }
      assertEquals(10_000, count);
    }

    @Test
    @DisplayName("Should handle multi-chunk highly compressible data with LZ4 streaming")
    void testMultiChunkHighlyCompressibleData() throws Exception {
      byte[] chunk0Arrow = createHighlyCompressibleArrowData(5_000);
      byte[] chunk1Arrow = createHighlyCompressibleArrowData(5_000);

      byte[] chunk0Compressed = compressLz4(chunk0Arrow);
      byte[] chunk1Compressed = compressLz4(chunk1Arrow);

      ResultData initial = createResultData(chunk0Compressed, 5_000L, 0L, 1L);
      ResultData chunk1Result = createResultData(chunk1Compressed, 5_000L, 5_000L, null);

      setupSessionMocks();
      when(mockSdkClient.getResultChunksData(eq(STATEMENT_ID), eq(1L), eq(5_000L)))
          .thenReturn(chunk1Result);

      provider =
          new SeaInlineArrowChunkProvider(
              initial, createManifest(CompressionCodec.LZ4_FRAME), STATEMENT_ID, mockSession);

      long totalRows = 0;
      ColumnInfo colInfo = new ColumnInfo();
      while (provider.hasNextChunk()) {
        assertTrue(provider.next());
        ArrowResultChunkIterator it = provider.getChunk().getChunkIterator();
        while (it.nextRow()) {
          assertEquals(
              42, it.getColumnObjectAtCurrentRow(0, ColumnInfoTypeName.INT, "INT", colInfo));
          totalRows++;
        }
      }
      assertEquals(10_000, totalRows);
      assertEquals(10_000L, provider.getRowCount());
    }
  }

  // ==================== Category 4: Error Handling ====================

  @Nested
  @DisplayName("Category 4: Error Handling")
  class ErrorHandlingTests {

    @Test
    @DisplayName("Should throw on null attachment")
    void testNullAttachment() {
      ResultData initial = createResultData(null, 1L, 0L, null);
      setupSessionMocks();

      assertThrows(
          DatabricksSQLException.class,
          () ->
              new SeaInlineArrowChunkProvider(
                  initial, createManifest(CompressionCodec.NONE), STATEMENT_ID, mockSession));
    }

    @Test
    @DisplayName("Should throw on empty attachment")
    void testEmptyAttachment() {
      ResultData initial = createResultData(new byte[0], 1L, 0L, null);
      setupSessionMocks();

      assertThrows(
          DatabricksSQLException.class,
          () ->
              new SeaInlineArrowChunkProvider(
                  initial, createManifest(CompressionCodec.NONE), STATEMENT_ID, mockSession));
    }

    @Test
    @DisplayName("Should propagate server error from prefetch to consumer")
    void testPrefetchServerError() throws Exception {
      byte[] chunk0Data = createArrowData(1);
      ResultData initial = createResultData(chunk0Data, 1L, 0L, 1L);

      setupSessionMocks();
      DatabricksSQLException serverError = new DatabricksSQLException("Server error", "HY000");
      when(mockSdkClient.getResultChunksData(eq(STATEMENT_ID), eq(1L), eq(1L)))
          .thenThrow(serverError);

      provider =
          new SeaInlineArrowChunkProvider(
              initial, createManifest(CompressionCodec.NONE), STATEMENT_ID, mockSession);

      // First chunk should be fine
      assertTrue(provider.next());
      assertNotNull(provider.getChunk());

      // Second chunk should propagate the prefetch error
      assertThrows(DatabricksSQLException.class, () -> provider.next());
    }

    @Test
    @DisplayName("Should propagate invalid Arrow data error")
    void testInvalidArrowData() {
      byte[] invalidData = new byte[] {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07};
      ResultData initial = createResultData(invalidData, 1L, 0L, null);
      setupSessionMocks();

      // Invalid Arrow IPC data should cause an error during initializeData
      assertThrows(
          Exception.class,
          () ->
              new SeaInlineArrowChunkProvider(
                  initial, createManifest(CompressionCodec.NONE), STATEMENT_ID, mockSession));
    }

    @Test
    @DisplayName("Should propagate corrupted compressed data error")
    void testCorruptedCompressedData() {
      byte[] corrupted = new byte[] {0x04, 0x22, 0x4D, 0x18, 0x00, 0x00}; // invalid LZ4 frame body
      ResultData initial = createResultData(corrupted, 1L, 0L, null);
      setupSessionMocks();

      assertThrows(
          Exception.class,
          () ->
              new SeaInlineArrowChunkProvider(
                  initial, createManifest(CompressionCodec.LZ4_FRAME), STATEMENT_ID, mockSession));
    }

    @Test
    @DisplayName("Should propagate error when second chunk has null attachment")
    void testSecondChunkNullAttachment() throws Exception {
      byte[] chunk0Data = createArrowData(1);
      ResultData initial = createResultData(chunk0Data, 1L, 0L, 1L);

      ResultData badChunk = createResultData(null, 1L, 1L, null);

      setupSessionMocks();
      when(mockSdkClient.getResultChunksData(eq(STATEMENT_ID), eq(1L), eq(1L)))
          .thenReturn(badChunk);

      provider =
          new SeaInlineArrowChunkProvider(
              initial, createManifest(CompressionCodec.NONE), STATEMENT_ID, mockSession);

      assertTrue(provider.next());
      assertNotNull(provider.getChunk());

      // Prefetch should fail on null attachment, propagated to consumer
      assertThrows(DatabricksSQLException.class, () -> provider.next());
    }
  }

  // ==================== Category 5: Close / Lifecycle ====================

  @Nested
  @DisplayName("Category 5: Close / Lifecycle")
  class LifecycleTests {

    @Test
    @DisplayName("Should report closed state after close()")
    void testCloseState() throws Exception {
      byte[] arrowData = createArrowData(1);
      ResultData initial = createResultData(arrowData, 1L, 0L, null);

      provider = createProvider(initial, CompressionCodec.NONE);
      assertFalse(provider.isClosed());

      provider.close();
      assertTrue(provider.isClosed());
    }

    @Test
    @DisplayName("Should return false from hasNextChunk/next after close")
    void testOperationsAfterClose() throws Exception {
      byte[] arrowData = createArrowData(1, 2);
      ResultData initial = createResultData(arrowData, 2L, 0L, 1L);

      provider = createProvider(initial, CompressionCodec.NONE);
      provider.close();

      assertFalse(provider.hasNextChunk());
      assertFalse(provider.next());
    }

    @Test
    @DisplayName("Should be safe to close multiple times")
    void testDoubleClose() throws Exception {
      byte[] arrowData = createArrowData(1);
      ResultData initial = createResultData(arrowData, 1L, 0L, null);

      provider = createProvider(initial, CompressionCodec.NONE);
      provider.close();
      provider.close(); // should not throw
      assertTrue(provider.isClosed());
    }

    @Test
    @DisplayName("Should stop prefetch thread on close")
    void testPrefetchThreadStopsOnClose() throws Exception {
      byte[] chunk0Data = createArrowData(1);
      ResultData initial = createResultData(chunk0Data, 1L, 0L, 1L);

      // Create a slow-responding mock to keep prefetch busy
      setupSessionMocks();
      when(mockSdkClient.getResultChunksData(eq(STATEMENT_ID), eq(1L), eq(1L)))
          .thenAnswer(
              inv -> {
                TimeUnit.SECONDS.sleep(10);
                return createResultData(createArrowData(2), 1L, 1L, null);
              });

      provider =
          new SeaInlineArrowChunkProvider(
              initial, createManifest(CompressionCodec.NONE), STATEMENT_ID, mockSession);

      // Close should interrupt the prefetch thread and return promptly
      long start = System.currentTimeMillis();
      provider.close();
      long elapsed = System.currentTimeMillis() - start;

      assertTrue(elapsed < 6000, "close() should return within 6 seconds, took " + elapsed + "ms");
      assertTrue(provider.isClosed());
    }
  }

  // ==================== Category 6: Backpressure ====================

  @Nested
  @DisplayName("Category 6: Backpressure")
  class BackpressureTests {

    @Test
    @DisplayName("Should limit chunks in memory to maxBatchesInMemory")
    void testBackpressureLimitsMemory() throws Exception {
      int maxBatches = 2;

      byte[] chunk0Data = createArrowData(1);
      ResultData initial = createResultData(chunk0Data, 1L, 0L, 1L);

      AtomicInteger fetchCount = new AtomicInteger(0);
      CountDownLatch chunk3Fetched = new CountDownLatch(1);

      setupSessionMocks();
      when(mockConnectionContext.getThriftMaxBatchesInMemory()).thenReturn(maxBatches);

      // Chunks 1-4, each pointing to the next
      for (int i = 1; i <= 4; i++) {
        final int chunkIdx = i;
        final long nextIdx = (i < 4) ? i + 1 : -1;
        final Long nextChunkIndex = (i < 4) ? (long) (i + 1) : null;
        ResultData chunkResult =
            createResultData(createArrowData(chunkIdx * 10), 1L, (long) chunkIdx, nextChunkIndex);

        when(mockSdkClient.getResultChunksData(
                eq(STATEMENT_ID), eq((long) chunkIdx), eq((long) chunkIdx)))
            .thenAnswer(
                inv -> {
                  int count = fetchCount.incrementAndGet();
                  if (count >= 3) chunk3Fetched.countDown();
                  return chunkResult;
                });
      }

      provider =
          new SeaInlineArrowChunkProvider(
              initial, createManifest(CompressionCodec.NONE), STATEMENT_ID, mockSession);

      // Wait a bit for prefetch to run
      TimeUnit.MILLISECONDS.sleep(300);

      // With maxBatches=2, prefetch should have fetched at most 1 additional chunk
      // (initial chunk takes 1 slot, so only 1 prefetch slot available)
      assertTrue(
          fetchCount.get() <= maxBatches,
          "Prefetch should be limited by backpressure, fetched: " + fetchCount.get());

      // Consume chunks to release backpressure
      int consumed = 0;
      while (provider.hasNextChunk()) {
        assertTrue(provider.next());
        assertNotNull(provider.getChunk());
        consumed++;
        if (consumed > 10) fail("Infinite loop");
      }

      assertEquals(5, consumed, "Should consume all 5 chunks");
    }

    @Test
    @DisplayName("Should enforce minimum maxBatchesInMemory of 2")
    void testMinimumMaxBatches() throws Exception {
      byte[] arrowData = createArrowData(1);
      ResultData initial = createResultData(arrowData, 1L, 0L, 1L);

      setupSessionMocks();
      when(mockConnectionContext.getThriftMaxBatchesInMemory()).thenReturn(1); // below minimum

      ResultData chunk1 = createResultData(createArrowData(2), 1L, 1L, null);
      when(mockSdkClient.getResultChunksData(eq(STATEMENT_ID), eq(1L), eq(1L))).thenReturn(chunk1);

      // Should not throw — minimum is enforced to 2
      provider =
          new SeaInlineArrowChunkProvider(
              initial, createManifest(CompressionCodec.NONE), STATEMENT_ID, mockSession);

      assertTrue(provider.next());
      assertNotNull(provider.getChunk());
      assertTrue(provider.next());
      assertNotNull(provider.getChunk());
      assertFalse(provider.hasNextChunk());
    }
  }

  // ==================== Category 7: Edge Cases ====================

  @Nested
  @DisplayName("Category 7: Edge Cases")
  class EdgeCaseTests {

    @Test
    @DisplayName("Should handle null rowCount in ResultData")
    void testNullRowCount() throws Exception {
      byte[] arrowData = createArrowData(1, 2);
      ResultData initial = createResultData(arrowData, null, 0L, null);

      provider = createProvider(initial, CompressionCodec.NONE);

      assertTrue(provider.next());
      assertNotNull(provider.getChunk());
      assertEquals(0L, provider.getRowCount()); // null treated as 0
    }

    @Test
    @DisplayName("Should handle null rowOffset in initial ResultData")
    void testNullRowOffset() throws Exception {
      byte[] arrowData = createArrowData(1);
      ResultData initial = createResultData(arrowData, 1L, null, null);

      provider = createProvider(initial, CompressionCodec.NONE);

      assertTrue(provider.next());
      assertNotNull(provider.getChunk());
      assertEquals(1L, provider.getRowCount());
    }

    @Test
    @DisplayName("Should use server-provided nextChunkIndex for fetching")
    void testServerProvidedNextChunkIndex() throws Exception {
      // Verify that the provider uses the nextChunkIndex from the server response
      // (not an assumed sequential index) when fetching subsequent chunks
      byte[] chunk0Data = createArrowData(1);
      ResultData initial = createResultData(chunk0Data, 1L, 0L, 1L);

      byte[] chunk1Data = createArrowData(2);
      // Server says next chunk is at index 3 (skipping 2)
      ResultData chunk1Result = createResultData(chunk1Data, 1L, 1L, 3L);

      byte[] chunk3Data = createArrowData(3);
      ResultData chunk3Result = createResultData(chunk3Data, 1L, 2L, null);

      setupSessionMocks();
      when(mockSdkClient.getResultChunksData(eq(STATEMENT_ID), eq(1L), eq(1L)))
          .thenReturn(chunk1Result);
      when(mockSdkClient.getResultChunksData(eq(STATEMENT_ID), eq(3L), eq(2L)))
          .thenReturn(chunk3Result);

      provider =
          new SeaInlineArrowChunkProvider(
              initial, createManifest(CompressionCodec.NONE), STATEMENT_ID, mockSession);

      int consumed = 0;
      while (provider.hasNextChunk()) {
        assertTrue(provider.next());
        assertNotNull(provider.getChunk());
        consumed++;
        if (consumed > 5) fail("Infinite loop");
      }

      assertEquals(3, consumed);
      assertEquals(3L, provider.getRowCount());

      // Verify the server-provided chunk indices were used (1, then 3 — not 1, 2)
      verify(mockSdkClient).getResultChunksData(STATEMENT_ID, 1L, 1L);
      verify(mockSdkClient).getResultChunksData(STATEMENT_ID, 3L, 2L);
      verifyNoMoreInteractions(mockSdkClient);
    }

    @Test
    @DisplayName("Should handle single row per chunk across many chunks")
    void testManySmallChunks() throws Exception {
      int totalChunks = 10;
      byte[][] chunkData = new byte[totalChunks][];
      for (int i = 0; i < totalChunks; i++) {
        chunkData[i] = createArrowData(i * 100);
      }

      ResultData initial = createResultData(chunkData[0], 1L, 0L, 1L);

      setupSessionMocks();
      for (int i = 1; i < totalChunks; i++) {
        Long nextIdx = (i < totalChunks - 1) ? (long) (i + 1) : null;
        ResultData chunkResult = createResultData(chunkData[i], 1L, (long) i, nextIdx);
        when(mockSdkClient.getResultChunksData(eq(STATEMENT_ID), eq((long) i), eq((long) i)))
            .thenReturn(chunkResult);
      }

      provider =
          new SeaInlineArrowChunkProvider(
              initial, createManifest(CompressionCodec.NONE), STATEMENT_ID, mockSession);

      ColumnInfo colInfo = new ColumnInfo();
      int consumed = 0;
      while (provider.hasNextChunk()) {
        assertTrue(provider.next());
        ArrowResultChunkIterator it = provider.getChunk().getChunkIterator();
        assertTrue(it.nextRow());
        int val = (int) it.getColumnObjectAtCurrentRow(0, ColumnInfoTypeName.INT, "INT", colInfo);
        assertEquals(consumed * 100, val);
        assertFalse(it.nextRow());
        consumed++;
      }

      assertEquals(totalChunks, consumed);
      assertEquals(totalChunks, provider.getRowCount());
    }

    @Test
    @DisplayName("Should handle mixed compression — NONE codec with uncompressed data")
    void testNoneCompressionCodec() throws Exception {
      byte[] arrowData = createArrowData(7, 8, 9);
      ResultData initial = createResultData(arrowData, 3L, 0L, null);

      provider = createProvider(initial, CompressionCodec.NONE);

      assertTrue(provider.next());
      ArrowResultChunkIterator it = provider.getChunk().getChunkIterator();
      ColumnInfo colInfo = new ColumnInfo();
      assertTrue(it.nextRow());
      assertEquals(7, it.getColumnObjectAtCurrentRow(0, ColumnInfoTypeName.INT, "INT", colInfo));
      assertTrue(it.nextRow());
      assertEquals(8, it.getColumnObjectAtCurrentRow(0, ColumnInfoTypeName.INT, "INT", colInfo));
      assertTrue(it.nextRow());
      assertEquals(9, it.getColumnObjectAtCurrentRow(0, ColumnInfoTypeName.INT, "INT", colInfo));
      assertFalse(it.nextRow());
    }
  }

  // ==================== Category 8: Prefetch Behavior ====================

  @Nested
  @DisplayName("Category 8: Prefetch Behavior")
  class PrefetchBehaviorTests {

    @Test
    @DisplayName("Should not make server calls for single-chunk result")
    void testNoServerCallsForSingleChunk() throws Exception {
      byte[] arrowData = createArrowData(1, 2, 3);
      ResultData initial = createResultData(arrowData, 3L, 0L, null);

      provider = createProvider(initial, CompressionCodec.NONE);

      // Wait to ensure prefetch thread has time to run
      TimeUnit.MILLISECONDS.sleep(200);

      assertTrue(provider.next());
      assertNotNull(provider.getChunk());
      assertFalse(provider.hasNextChunk());

      // No getResultChunksData calls should have been made
      verifyNoInteractions(mockSdkClient);
    }

    @Test
    @DisplayName("Should prefetch next chunk before consumer requests it")
    void testPrefetchAhead() throws Exception {
      byte[] chunk0Data = createArrowData(1);
      byte[] chunk1Data = createArrowData(2);

      ResultData initial = createResultData(chunk0Data, 1L, 0L, 1L);

      CountDownLatch chunk1Fetched = new CountDownLatch(1);
      ResultData chunk1Result = createResultData(chunk1Data, 1L, 1L, null);

      setupSessionMocks();
      when(mockSdkClient.getResultChunksData(eq(STATEMENT_ID), eq(1L), eq(1L)))
          .thenAnswer(
              inv -> {
                chunk1Fetched.countDown();
                return chunk1Result;
              });

      provider =
          new SeaInlineArrowChunkProvider(
              initial, createManifest(CompressionCodec.NONE), STATEMENT_ID, mockSession);

      // Chunk 1 should be prefetched before we call next()
      assertTrue(
          chunk1Fetched.await(2, TimeUnit.SECONDS),
          "Prefetch should fetch chunk 1 before consumer requests it");

      // Both chunks should be available without waiting
      assertTrue(provider.next());
      assertNotNull(provider.getChunk());
      assertTrue(provider.next());
      assertNotNull(provider.getChunk());
      assertFalse(provider.hasNextChunk());
    }

    @Test
    @DisplayName("Consumer waiting should succeed once prefetch delivers chunk")
    void testConsumerWaitsForPrefetch() throws Exception {
      byte[] chunk0Data = createArrowData(1);
      byte[] chunk1Data = createArrowData(2);

      ResultData initial = createResultData(chunk0Data, 1L, 0L, 1L);
      ResultData chunk1Result = createResultData(chunk1Data, 1L, 1L, null);

      setupSessionMocks();
      // Simulate slow server response
      when(mockSdkClient.getResultChunksData(eq(STATEMENT_ID), eq(1L), eq(1L)))
          .thenAnswer(
              inv -> {
                TimeUnit.MILLISECONDS.sleep(500);
                return chunk1Result;
              });

      provider =
          new SeaInlineArrowChunkProvider(
              initial, createManifest(CompressionCodec.NONE), STATEMENT_ID, mockSession);

      // Consume first chunk immediately
      assertTrue(provider.next());
      assertNotNull(provider.getChunk());

      // Second chunk should arrive after server delay
      assertTrue(provider.next());
      assertNotNull(provider.getChunk());
      assertFalse(provider.hasNextChunk());
    }
  }
}
