package com.databricks.jdbc.api.impl.arrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.model.core.ArrowBatch;
import com.databricks.jdbc.model.core.ColumnInfo;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.core.Format;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.core.ResultSchema;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.SchemaUtility;
import org.junit.jupiter.api.Test;

class ArrowBatchChunkProviderFactoryTest {
  private static final StatementId STATEMENT_ID =
      new StatementId("01f0aaaa-bbbb-cccc-dddd-eeeeeeeeeeee");

  @Test
  void readsSingleNativeArrowBatch() throws Exception {
    ChunkProvider provider =
        ArrowBatchChunkProviderFactory.create(
            resultData(Collections.singletonList(arrowBatch(1, 2)), 2), manifest(2));

    assertEquals(Arrays.asList(1, 2), readValues(provider));
  }

  @Test
  void readsOrderedNativeArrowBatches() throws Exception {
    List<ArrowBatch> batches = Arrays.asList(arrowBatch(1, 2), arrowBatch(3), arrowBatch(4, 5));
    ChunkProvider provider =
        ArrowBatchChunkProviderFactory.create(resultData(batches, 5), manifest(5));

    assertEquals(Arrays.asList(1, 2, 3, 4, 5), readValues(provider));
  }

  @Test
  void readsCompressedNativeArrowBatch() throws Exception {
    ArrowBatch batch = arrowBatch(1, 2);
    ChunkProvider provider =
        ArrowBatchChunkProviderFactory.create(
            resultData(Collections.singletonList(compress(batch)), 2),
            manifest(2).setResultCompression(CompressionCodec.LZ4_FRAME));

    assertEquals(Arrays.asList(1, 2), readValues(provider));
  }

  @Test
  void acceptsExplicitZeroBatchResult() throws Exception {
    ChunkProvider provider =
        ArrowBatchChunkProviderFactory.create(resultData(Collections.emptyList(), 0), manifest(0));
    try {
      assertEquals(0, provider.getRowCount());
      assertFalse(provider.hasNextChunk());
    } finally {
      provider.close();
    }
  }

  @Test
  void acceptsOmittedZeroBatchResult() throws Exception {
    for (ResultData resultData : Arrays.asList(new ResultData(), resultData(null, 0))) {
      ChunkProvider provider = ArrowBatchChunkProviderFactory.create(resultData, manifest(0));
      try {
        assertEquals(0, provider.getRowCount());
        assertFalse(provider.hasNextChunk());
      } finally {
        provider.close();
      }
    }
  }

  @Test
  void rejectsMissingBatchFields() {
    assertThrows(
        DatabricksParsingException.class,
        () -> ArrowBatchChunkProviderFactory.create(null, manifest(0)));
    assertThrows(
        DatabricksParsingException.class,
        () ->
            ArrowBatchChunkProviderFactory.create(
                resultData(Collections.singletonList(new ArrowBatch().setRowCount(1L)), 1),
                manifest(1)));
    assertThrows(
        DatabricksParsingException.class,
        () ->
            ArrowBatchChunkProviderFactory.create(
                resultData(Collections.singletonList(new ArrowBatch().setBatch(new byte[] {1})), 1),
                manifest(1)));
  }

  @Test
  void rejectsMissingRawArrowSchema() throws Exception {
    ResultManifest manifest = manifest(1).setArrowSchema(null);

    assertThrows(
        DatabricksParsingException.class,
        () ->
            ArrowBatchChunkProviderFactory.create(
                resultData(Collections.singletonList(arrowBatch(1)), 1), manifest));
  }

  @Test
  void rejectsRowCountMismatch() throws Exception {
    List<ArrowBatch> batches = Collections.singletonList(arrowBatch(7));

    assertThrows(
        DatabricksParsingException.class,
        () -> ArrowBatchChunkProviderFactory.create(resultData(batches, 2), manifest(1)));
    assertThrows(
        DatabricksParsingException.class,
        () -> ArrowBatchChunkProviderFactory.create(resultData(batches, 1), manifest(2)));
  }

  @Test
  void rejectsExternalLinkNativeArrowBatches() throws Exception {
    List<ArrowBatch> batches = Collections.singletonList(arrowBatch(7));

    assertThrows(
        DatabricksParsingException.class,
        () ->
            ArrowBatchChunkProviderFactory.create(
                resultData(batches, 1)
                    .setExternalLinks(Collections.singletonList(new ExternalLink())),
                manifest(1)));
  }

  @Test
  void rejectsPaginationWithoutSession() throws Exception {
    List<ArrowBatch> batches = Collections.singletonList(arrowBatch(7));

    assertThrows(
        DatabricksParsingException.class,
        () ->
            ArrowBatchChunkProviderFactory.create(
                page(batches, 0, 0).setNextChunkIndex(1L), pagedManifest()));
  }

  @Test
  void readsPagesInOrderWithRowOffsets() throws Exception {
    IDatabricksClient client = mock(IDatabricksClient.class);
    when(client.getResultChunksData(STATEMENT_ID, 1L, 2L))
        .thenReturn(page(Collections.singletonList(arrowBatch(3)), 1, 2).setNextChunkIndex(2L));
    when(client.getResultChunksData(STATEMENT_ID, 2L, 3L))
        .thenReturn(page(Arrays.asList(arrowBatch(4), arrowBatch(5, 6)), 2, 3));
    ResultData firstPage = page(Collections.singletonList(arrowBatch(1, 2)), 0, 0);

    ChunkProvider provider =
        ArrowBatchChunkProviderFactory.create(
            firstPage.setNextChunkIndex(1L), pagedManifest(), STATEMENT_ID, session(client));

    assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6), readAllValues(provider));
    verify(client).getResultChunksData(STATEMENT_ID, 1L, 2L);
    verify(client).getResultChunksData(STATEMENT_ID, 2L, 3L);
  }

  @Test
  void fetchesPagesLazily() throws Exception {
    IDatabricksClient client = mock(IDatabricksClient.class);
    when(client.getResultChunksData(STATEMENT_ID, 1L, 1L))
        .thenReturn(page(Collections.singletonList(arrowBatch(2)), 1, 1));

    ChunkProvider provider =
        ArrowBatchChunkProviderFactory.create(
            page(Collections.singletonList(arrowBatch(1)), 0, 0).setNextChunkIndex(1L),
            pagedManifest(),
            STATEMENT_ID,
            session(client));
    try {
      assertTrue(provider.next());
      verify(client, never()).getResultChunksData(any(), anyLong(), anyLong());
      assertTrue(provider.hasNextChunk());
      assertTrue(provider.next());
      verify(client).getResultChunksData(STATEMENT_ID, 1L, 1L);
      assertFalse(provider.hasNextChunk());
    } finally {
      provider.close();
    }
  }

  @Test
  void skipsEmptyPages() throws Exception {
    IDatabricksClient client = mock(IDatabricksClient.class);
    when(client.getResultChunksData(STATEMENT_ID, 1L, 0L))
        .thenReturn(page(Collections.emptyList(), 1, 0).setNextChunkIndex(2L));
    when(client.getResultChunksData(STATEMENT_ID, 2L, 0L))
        .thenReturn(page(Collections.singletonList(arrowBatch(9)), 2, 0));

    ChunkProvider provider =
        ArrowBatchChunkProviderFactory.create(
            page(Collections.emptyList(), 0, 0).setNextChunkIndex(1L),
            pagedManifest(),
            STATEMENT_ID,
            session(client));

    assertEquals(Collections.singletonList(9), readAllValues(provider));
  }

  @Test
  void readsCompressedPages() throws Exception {
    IDatabricksClient client = mock(IDatabricksClient.class);
    when(client.getResultChunksData(STATEMENT_ID, 1L, 1L))
        .thenReturn(page(Collections.singletonList(compress(arrowBatch(2))), 1, 1));

    ChunkProvider provider =
        ArrowBatchChunkProviderFactory.create(
            page(Collections.singletonList(compress(arrowBatch(1))), 0, 0).setNextChunkIndex(1L),
            pagedManifest().setResultCompression(CompressionCodec.LZ4_FRAME),
            STATEMENT_ID,
            session(client));

    assertEquals(Arrays.asList(1, 2), readAllValues(provider));
  }

  @Test
  void rejectsPageAtUnexpectedRowOffset() throws Exception {
    IDatabricksClient client = mock(IDatabricksClient.class);
    when(client.getResultChunksData(STATEMENT_ID, 1L, 1L))
        .thenReturn(page(Collections.singletonList(arrowBatch(2)), 1, 0));

    ChunkProvider provider =
        ArrowBatchChunkProviderFactory.create(
            page(Collections.singletonList(arrowBatch(1)), 0, 0).setNextChunkIndex(1L),
            pagedManifest(),
            STATEMENT_ID,
            session(client));
    try {
      assertTrue(provider.next());
      assertThrows(DatabricksParsingException.class, provider::next);
    } finally {
      provider.close();
    }
  }

  @Test
  void rejectsNonIncreasingNextChunkIndex() throws Exception {
    assertThrows(
        DatabricksParsingException.class,
        () ->
            ArrowBatchChunkProviderFactory.create(
                page(Collections.singletonList(arrowBatch(1)), 0, 0).setNextChunkIndex(0L),
                pagedManifest(),
                STATEMENT_ID,
                session(mock(IDatabricksClient.class))));
  }

  @Test
  void rejectsMalformedBatchBytes() {
    List<ArrowBatch> batches =
        Collections.singletonList(new ArrowBatch().setBatch(new byte[] {1, 2, 3}).setRowCount(1L));

    assertThrows(
        DatabricksParsingException.class,
        () -> ArrowBatchChunkProviderFactory.create(resultData(batches, 1), manifest(1)));
  }

  private static List<Integer> readValues(ChunkProvider provider) throws Exception {
    try {
      assertTrue(provider.next());
      ArrowResultChunkIterator iterator = provider.getChunk().getChunkIterator();
      List<Integer> values = new ArrayList<>();
      ColumnInfo column = intColumn();
      while (iterator.nextRow()) {
        values.add(
            (Integer)
                iterator.getColumnObjectAtCurrentRow(0, ColumnInfoTypeName.INT, "INT", column));
      }
      assertFalse(provider.hasNextChunk());
      return values;
    } finally {
      provider.close();
    }
  }

  private static List<Integer> readAllValues(ChunkProvider provider) throws Exception {
    try {
      List<Integer> values = new ArrayList<>();
      ColumnInfo column = intColumn();
      while (provider.hasNextChunk()) {
        assertTrue(provider.next());
        ArrowResultChunkIterator iterator = provider.getChunk().getChunkIterator();
        while (iterator.nextRow()) {
          values.add(
              (Integer)
                  iterator.getColumnObjectAtCurrentRow(0, ColumnInfoTypeName.INT, "INT", column));
        }
      }
      return values;
    } finally {
      provider.close();
    }
  }

  /** Stands in for the SEA chunk endpoint, which the provider calls for each later page. */
  private static IDatabricksSession session(IDatabricksClient client) {
    IDatabricksSession session = mock(IDatabricksSession.class);
    when(session.getDatabricksClient()).thenReturn(client);
    return session;
  }

  private static ResultData resultData(List<ArrowBatch> batches, long rowCount) {
    return new ResultData().setArrowBatches(batches).setRowCount(rowCount);
  }

  private static ResultData page(List<ArrowBatch> batches, long chunkIndex, long rowOffset) {
    long rowCount = batches.stream().mapToLong(ArrowBatch::getRowCount).sum();
    return resultData(batches, rowCount).setChunkIndex(chunkIndex).setRowOffset(rowOffset);
  }

  /** APC manifests carry no chunk or row totals; pages are discovered by next_chunk_index. */
  private static ResultManifest pagedManifest() {
    return manifest(0).setTotalChunkCount(null).setTotalRowCount(null);
  }

  private static ResultManifest manifest(long rowCount) {
    return new ResultManifest()
        .setFormat(Format.ARROW_BATCH)
        .setSchema(
            new ResultSchema()
                .setColumnCount(1L)
                .setColumns(Collections.singletonList(intColumn())))
        .setArrowSchema(arrowSchema())
        .setTotalChunkCount(1L)
        .setTotalRowCount(rowCount);
  }

  private static byte[] arrowSchema() {
    Field field = new Field("value", new FieldType(true, new ArrowType.Int(32, true), null), null);
    try {
      return SchemaUtility.serialize(new Schema(Collections.singletonList(field)));
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  private static ColumnInfo intColumn() {
    return new ColumnInfo()
        .setName("value")
        .setPosition(0L)
        .setTypeName(ColumnInfoTypeName.INT)
        .setTypeText("INT");
  }

  private static ArrowBatch arrowBatch(int... values) throws Exception {
    try (RootAllocator allocator = new RootAllocator();
        ByteArrayOutputStream output = new ByteArrayOutputStream()) {
      IntVector vector = new IntVector("value", allocator);
      vector.allocateNew(values.length);
      for (int index = 0; index < values.length; index++) {
        vector.set(index, values[index]);
      }
      vector.setValueCount(values.length);

      try (VectorSchemaRoot root = VectorSchemaRoot.of(vector)) {
        root.setRowCount(values.length);
        try (ArrowRecordBatch recordBatch = new VectorUnloader(root).getRecordBatch()) {
          MessageSerializer.serialize(new WriteChannel(Channels.newChannel(output)), recordBatch);
        }
      }
      return new ArrowBatch().setBatch(output.toByteArray()).setRowCount((long) values.length);
    }
  }

  private static ArrowBatch compress(ArrowBatch batch) throws IOException {
    try (ByteArrayOutputStream output = new ByteArrayOutputStream();
        LZ4FrameOutputStream lz4 = new LZ4FrameOutputStream(output)) {
      lz4.write(batch.getBatch());
      lz4.close();
      return new ArrowBatch().setBatch(output.toByteArray()).setRowCount(batch.getRowCount());
    }
  }
}
