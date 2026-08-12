package com.databricks.jdbc.api.impl.arrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.model.core.ArrowBatch;
import com.databricks.jdbc.model.core.ColumnInfo;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import com.databricks.jdbc.model.core.Format;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.core.ResultSchema;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.junit.jupiter.api.Test;

class ArrowBatchInlineChunkProviderTest {

  @Test
  void readsOrderedNativeArrowBatches() throws Exception {
    List<ArrowBatch> batches = Arrays.asList(arrowBatch(1, 2), arrowBatch(3), arrowBatch(4, 5));
    ArrowBatchInlineChunkProvider provider =
        new ArrowBatchInlineChunkProvider(resultData(batches, 5), manifest(5));
    try {
      assertEquals(5, provider.getRowCount());
      assertTrue(provider.next());

      ArrowResultChunkIterator iterator = provider.getChunk().getChunkIterator();
      List<Integer> values = new ArrayList<>();
      ColumnInfo column = intColumn();
      while (iterator.nextRow()) {
        values.add(
            (Integer)
                iterator.getColumnObjectAtCurrentRow(0, ColumnInfoTypeName.INT, "INT", column));
      }

      assertEquals(Arrays.asList(1, 2, 3, 4, 5), values);
      assertFalse(provider.hasNextChunk());
    } finally {
      provider.close();
    }
  }

  @Test
  void acceptsExplicitZeroBatchResult() throws Exception {
    ArrowBatchInlineChunkProvider provider =
        new ArrowBatchInlineChunkProvider(resultData(Collections.emptyList(), 0), manifest(0));
    try {
      assertEquals(0, provider.getRowCount());
      assertFalse(provider.hasNextChunk());
      assertFalse(provider.getChunk().getChunkIterator().nextRow());
    } finally {
      provider.close();
    }
  }

  @Test
  void rejectsMissingBatchFields() {
    assertThrows(
        DatabricksParsingException.class,
        () -> new ArrowBatchInlineChunkProvider(null, manifest(0)));
    assertThrows(
        DatabricksParsingException.class,
        () ->
            new ArrowBatchInlineChunkProvider(
                resultData(Collections.singletonList(new ArrowBatch().setRowCount(1L)), 1),
                manifest(1)));
    assertThrows(
        DatabricksParsingException.class,
        () ->
            new ArrowBatchInlineChunkProvider(
                resultData(Collections.singletonList(new ArrowBatch().setBatch(new byte[] {1})), 1),
                manifest(1)));
  }

  @Test
  void rejectsRowCountMismatch() throws Exception {
    List<ArrowBatch> batches = Collections.singletonList(arrowBatch(7));

    assertThrows(
        DatabricksParsingException.class,
        () -> new ArrowBatchInlineChunkProvider(resultData(batches, 2), manifest(1)));
    assertThrows(
        DatabricksParsingException.class,
        () -> new ArrowBatchInlineChunkProvider(resultData(batches, 1), manifest(2)));
  }

  @Test
  void rejectsMalformedBatchBytes() {
    List<ArrowBatch> batches =
        Collections.singletonList(new ArrowBatch().setBatch(new byte[] {1, 2, 3}).setRowCount(1L));

    assertThrows(
        DatabricksParsingException.class,
        () -> new ArrowBatchInlineChunkProvider(resultData(batches, 1), manifest(1)));
  }

  @Test
  void rejectsUnsupportedStructuredSchemaType() throws Exception {
    ResultManifest manifest = manifest(1);
    manifest
        .getSchema()
        .setColumns(
            Collections.singletonList(
                new ColumnInfo()
                    .setName("value")
                    .setPosition(0L)
                    .setTypeName(ColumnInfoTypeName.ARRAY)
                    .setTypeText("ARRAY<INT>")));

    assertThrows(
        DatabricksParsingException.class,
        () ->
            new ArrowBatchInlineChunkProvider(
                resultData(Collections.singletonList(arrowBatch(1)), 1), manifest));
  }

  private static ResultData resultData(List<ArrowBatch> batches, long rowCount) {
    return new ResultData().setArrowBatches(batches).setRowCount(rowCount);
  }

  private static ResultManifest manifest(long rowCount) {
    return new ResultManifest()
        .setFormat(Format.ARROW_BATCH)
        .setSchema(
            new ResultSchema()
                .setColumnCount(1L)
                .setColumns(Collections.singletonList(intColumn())))
        .setTotalChunkCount(1L)
        .setTotalRowCount(rowCount);
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
}
