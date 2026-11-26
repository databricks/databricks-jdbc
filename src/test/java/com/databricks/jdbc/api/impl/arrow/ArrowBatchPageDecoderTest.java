package com.databricks.jdbc.api.impl.arrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.model.core.ArrowBatch;
import com.databricks.jdbc.model.core.ColumnInfo;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
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

class ArrowBatchPageDecoderTest {
  @Test
  void decodesMultipleBatchesInOrder() throws Exception {
    ArrowResultChunk chunk =
        new ArrowBatchPageDecoder(manifest())
            .decode(
                resultData(Arrays.asList(arrowBatch(1, 2), arrowBatch(3), arrowBatch(4, 5)), 5));

    assertEquals(Arrays.asList(1, 2, 3, 4, 5), readValues(chunk));
  }

  @Test
  void decodesCompressedBatches() throws Exception {
    ResultManifest manifest = manifest().setResultCompression(CompressionCodec.LZ4_FRAME);
    ArrowResultChunk chunk =
        new ArrowBatchPageDecoder(manifest)
            .decode(resultData(Collections.singletonList(compress(arrowBatch(1, 2))), 2));

    assertEquals(Arrays.asList(1, 2), readValues(chunk));
  }

  @Test
  void derivesArrowSchemaFromPublicSchemaWhenRawSchemaIsAbsent() throws Exception {
    for (byte[] arrowSchema : Arrays.asList(null, new byte[0])) {
      ResultManifest manifest = manifest().setArrowSchema(arrowSchema);
      ArrowResultChunk chunk =
          new ArrowBatchPageDecoder(manifest)
              .decode(resultData(Collections.singletonList(arrowBatch(1, 2)), 2));

      assertEquals(Arrays.asList(1, 2), readValues(chunk));
    }
  }

  @Test
  void prefersRawArrowSchemaWhenPresent() throws Exception {
    ResultManifest manifest =
        manifest()
            .setSchema(
                new ResultSchema()
                    .setColumnCount(1L)
                    .setColumns(
                        Collections.singletonList(
                            new ColumnInfo()
                                .setName("value")
                                .setPosition(0L)
                                .setTypeName(ColumnInfoTypeName.STRING)
                                .setTypeText("STRING"))));

    ArrowResultChunk chunk =
        new ArrowBatchPageDecoder(manifest)
            .decode(resultData(Collections.singletonList(arrowBatch(1, 2)), 2));

    assertEquals(Arrays.asList(1, 2), readValues(chunk));
  }

  @Test
  void rejectsMalformedBatchMetadataAndBytes() throws Exception {
    ArrowBatchPageDecoder decoder = new ArrowBatchPageDecoder(manifest());

    assertThrows(
        DatabricksParsingException.class,
        () ->
            decoder.decode(
                resultData(Collections.singletonList(new ArrowBatch().setRowCount(1L)), 1)));
    assertThrows(
        DatabricksParsingException.class,
        () ->
            decoder.decode(
                resultData(
                    Collections.singletonList(new ArrowBatch().setBatch(new byte[] {1})), 1)));
    assertThrows(
        DatabricksParsingException.class,
        () -> decoder.decode(resultData(Collections.singletonList(arrowBatch(7)), 2)));

    assertInstanceOf(
        DatabricksParsingException.class,
        assertThrows(
            Exception.class,
            () ->
                decoder.decode(
                    resultData(
                        Collections.singletonList(
                            new ArrowBatch().setBatch(new byte[] {1, 2, 3}).setRowCount(1L)),
                        1))));
  }

  private static List<Integer> readValues(ArrowResultChunk chunk) throws Exception {
    try {
      ArrowResultChunkIterator iterator = chunk.getChunkIterator();
      List<Integer> values = new ArrayList<>();
      while (iterator.nextRow()) {
        values.add(
            (Integer)
                iterator.getColumnObjectAtCurrentRow(
                    0, ColumnInfoTypeName.INT, "INT", intColumn()));
      }
      return values;
    } finally {
      chunk.releaseChunk();
    }
  }

  private static ResultData resultData(List<ArrowBatch> batches, long rowCount) {
    return new ResultData().setArrowBatches(batches).setRowCount(rowCount);
  }

  private static ResultManifest manifest() {
    return new ResultManifest()
        .setFormat(Format.ARROW_BATCH)
        .setSchema(
            new ResultSchema()
                .setColumnCount(1L)
                .setColumns(Collections.singletonList(intColumn())))
        .setArrowSchema(arrowSchema());
  }

  private static ColumnInfo intColumn() {
    return new ColumnInfo()
        .setName("value")
        .setPosition(0L)
        .setTypeName(ColumnInfoTypeName.INT)
        .setTypeText("INT");
  }

  private static byte[] arrowSchema() {
    Field field = new Field("value", new FieldType(true, new ArrowType.Int(32, true), null), null);
    try {
      return SchemaUtility.serialize(new Schema(Collections.singletonList(field)));
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  private static ArrowBatch arrowBatch(int... values) throws Exception {
    try (RootAllocator allocator = new RootAllocator();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        IntVector vector = new IntVector("value", allocator)) {
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
