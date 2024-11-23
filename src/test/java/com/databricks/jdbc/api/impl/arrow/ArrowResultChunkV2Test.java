package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.TestConstants.*;
import static java.lang.Math.min;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.model.client.thrift.generated.TSparkArrowResultLink;
import com.databricks.sdk.service.sql.BaseChunkInfo;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.ArrowWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

public class ArrowResultChunkV2Test {

  private static final String STATEMENT_ID = "statement_id";
  private final Random random = new Random();
  private final int rowsInRecordBatch = 20;
  private final long totalRows = 110;

  @Test
  public void testReleaseUnusedChunk() throws Exception {
    // Arrange
    BaseChunkInfo chunkInfo =
        new BaseChunkInfo()
            .setChunkIndex(0L)
            .setByteCount(200L)
            .setRowOffset(0L)
            .setRowCount(totalRows);
    ArrowResultChunkV2 arrowResultChunkV2 =
        ArrowResultChunkV2.builder().statementId(STATEMENT_ID).withChunkInfo(chunkInfo).build();

    // Assert
    assert (arrowResultChunkV2.getRecordBatchCountInChunk() == 0);
    arrowResultChunkV2.releaseChunk();
  }

  @Test
  public void testGetArrowDataFromInputStream() throws Exception {
    // Arrange
    BaseChunkInfo chunkInfo =
        new BaseChunkInfo()
            .setChunkIndex(0L)
            .setByteCount(200L)
            .setRowOffset(0L)
            .setRowCount(totalRows);
    ArrowResultChunkV2 arrowResultChunkV2 =
        ArrowResultChunkV2.builder().statementId(STATEMENT_ID).withChunkInfo(chunkInfo).build();
    Schema schema = createTestSchema();
    Object[][] testData = createTestData(schema, (int) totalRows);
    File arrowFile =
        createTestArrowFile("TestFile", schema, testData, new RootAllocator(Integer.MAX_VALUE));

    // Act
    arrowResultChunkV2.initializeData(new FileInputStream(arrowFile));

    // Assert
    int totalRecordBatches = (int) ((totalRows + rowsInRecordBatch) / rowsInRecordBatch);
    assertEquals(arrowResultChunkV2.getRecordBatchCountInChunk(), totalRecordBatches);
    arrowResultChunkV2.releaseChunk();
    arrowResultChunkV2.releaseChunk(); // calling it a second time also does not throw error.
  }

  @Test
  public void testGetArrowDataFromThriftInput() throws DatabricksParsingException {
    TSparkArrowResultLink chunkInfo =
        new TSparkArrowResultLink()
            .setRowCount(totalRows)
            .setFileLink(TEST_STRING)
            .setExpiryTime(1000)
            .setBytesNum(200L);
    ArrowResultChunkV2 arrowResultChunkV2 =
        ArrowResultChunkV2.builder()
            .statementId(TEST_STATEMENT_ID)
            .withThriftChunkInfo(0, chunkInfo)
            .build();
    assertNull(arrowResultChunkV2.getErrorMessage());
    assertEquals(arrowResultChunkV2.getChunkUrl(), TEST_STRING);
    assertEquals(arrowResultChunkV2.getChunkIndex(), 0);
  }

  private File createTestArrowFile(
      String fileName, Schema schema, Object[][] testData, RootAllocator allocator)
      throws IOException {
    File file = new File(fileName);
    int cols = testData.length;
    int rows = testData[0].length;
    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator);
    ArrowWriter writer =
        new ArrowStreamWriter(
            vectorSchemaRoot,
            new DictionaryProvider.MapDictionaryProvider(),
            new FileOutputStream(file));
    writer.start();
    for (int j = 0; j < rows; j += rowsInRecordBatch) {
      int rowsToAddToRecordBatch = min(rowsInRecordBatch, rows - j);
      vectorSchemaRoot.setRowCount(rowsToAddToRecordBatch);
      for (int i = 0; i < cols; i++) {
        Types.MinorType type = Types.getMinorTypeForArrowType(schema.getFields().get(i).getType());
        FieldVector fieldVector = vectorSchemaRoot.getFieldVectors().get(i);
        if (type.equals(Types.MinorType.INT)) {
          IntVector intVector = (IntVector) fieldVector;
          intVector.setInitialCapacity(rowsToAddToRecordBatch);
          for (int k = 0; k < rowsToAddToRecordBatch; k++) {
            intVector.set(k, 1, (int) testData[i][j + k]);
          }
        } else if (type.equals(Types.MinorType.FLOAT8)) {
          Float8Vector float8Vector = (Float8Vector) fieldVector;
          float8Vector.setInitialCapacity(rowsToAddToRecordBatch);
          for (int k = 0; k < rowsToAddToRecordBatch; k++) {
            float8Vector.set(k, 1, (double) testData[i][j + k]);
          }
        }
        fieldVector.setValueCount(rowsToAddToRecordBatch);
      }
      writer.writeBatch();
    }
    return file;
  }

  private Schema createTestSchema() {
    List<Field> fieldList = new ArrayList<>();
    FieldType fieldType1 = new FieldType(false, Types.MinorType.INT.getType(), null);
    FieldType fieldType2 = new FieldType(false, Types.MinorType.FLOAT8.getType(), null);
    fieldList.add(new Field("Field1", fieldType1, null));
    fieldList.add(new Field("Field2", fieldType2, null));
    return new Schema(fieldList);
  }

  private Object[][] createTestData(Schema schema, int rows) {
    int cols = schema.getFields().size();
    Object[][] data = new Object[cols][rows];
    for (int i = 0; i < cols; i++) {
      Types.MinorType type = Types.getMinorTypeForArrowType(schema.getFields().get(i).getType());
      if (type.equals(Types.MinorType.INT)) {
        for (int j = 0; j < rows; j++) {
          data[i][j] = random.nextInt();
        }
      } else if (type.equals(Types.MinorType.FLOAT8)) {
        for (int j = 0; j < rows; j++) {
          data[i][j] = random.nextDouble();
        }
      }
    }
    return data;
  }

  @Test
  public void testHasNextRow() throws DatabricksParsingException {
    BaseChunkInfo emptyChunkInfo =
        new BaseChunkInfo().setChunkIndex(0L).setByteCount(200L).setRowOffset(0L).setRowCount(0L);
    ArrowResultChunkV2 arrowResultChunkV2 =
        ArrowResultChunkV2.builder()
            .statementId(STATEMENT_ID)
            .withChunkInfo(emptyChunkInfo)
            .build();
    arrowResultChunkV2.setIsDataInitialized(true);
    arrowResultChunkV2.recordBatchList = Collections.nCopies(3, new ArrayList<>());
    assertFalse(arrowResultChunkV2.getChunkIterator().hasNextRow());

    BaseChunkInfo chunkInfo =
        new BaseChunkInfo().setChunkIndex(18L).setByteCount(200L).setRowOffset(0L).setRowCount(4L);
    arrowResultChunkV2 =
        ArrowResultChunkV2.builder().statementId(STATEMENT_ID).withChunkInfo(chunkInfo).build();
    arrowResultChunkV2.setIsDataInitialized(true);
    int size = 2;
    IntVector dummyVector = new IntVector("dummy_vector", new RootAllocator());
    dummyVector.allocateNew(size);
    dummyVector.setValueCount(size);
    for (int i = 0; i < size; i++) {
      dummyVector.set(i, i * 10);
    }
    arrowResultChunkV2.recordBatchList =
        List.of(List.of(dummyVector), List.of(dummyVector), new ArrayList<>());
    ArrowResultChunkV2.ArrowResultChunkIterator iterator = arrowResultChunkV2.getChunkIterator();
    assertTrue(iterator.hasNextRow());
    iterator.nextRow();
    assertEquals(0, iterator.getColumnObjectAtCurrentRow(0));
    assertTrue(iterator.hasNextRow());
    iterator.nextRow();
    assertEquals(10, iterator.getColumnObjectAtCurrentRow(0));
    assertTrue(iterator.hasNextRow());
    iterator.nextRow();
    assertEquals(0, iterator.getColumnObjectAtCurrentRow(0));
    assertTrue(iterator.hasNextRow());
    iterator.nextRow();
    assertEquals(10, iterator.getColumnObjectAtCurrentRow(0));
    assertFalse(iterator.hasNextRow());
  }

  @Test
  public void testEmptyRecordBatches() throws DatabricksParsingException {
    BaseChunkInfo chunkInfo =
        new BaseChunkInfo().setChunkIndex(18L).setByteCount(200L).setRowOffset(0L).setRowCount(4L);
    ArrowResultChunkV2 arrowResultChunkV2 =
        ArrowResultChunkV2.builder().statementId(STATEMENT_ID).withChunkInfo(chunkInfo).build();
    arrowResultChunkV2.setIsDataInitialized(true);
    int size = 2;
    IntVector dummyVector = new IntVector("dummy_vector", new RootAllocator());
    dummyVector.allocateNew(size);
    dummyVector.setValueCount(size);
    for (int i = 0; i < size; i++) {
      dummyVector.set(i, i * 10);
    }
    IntVector emptyVector = new IntVector("empty_vector", new RootAllocator());
    emptyVector.allocateNew(0);
    emptyVector.setValueCount(0);
    arrowResultChunkV2.recordBatchList =
        List.of(List.of(dummyVector), List.of(emptyVector), List.of(dummyVector));
    ArrowResultChunkV2.ArrowResultChunkIterator iterator = arrowResultChunkV2.getChunkIterator();
    assertTrue(iterator.hasNextRow());
    iterator.nextRow();
    assertEquals(0, iterator.getColumnObjectAtCurrentRow(0));
    assertTrue(iterator.hasNextRow());
    iterator.nextRow();
    assertEquals(10, iterator.getColumnObjectAtCurrentRow(0));
    assertTrue(iterator.hasNextRow());
    iterator.nextRow();
    assertEquals(0, iterator.getColumnObjectAtCurrentRow(0));
    assertTrue(iterator.hasNextRow());
    iterator.nextRow();
    assertEquals(10, iterator.getColumnObjectAtCurrentRow(0));
    assertFalse(iterator.hasNextRow());
  }
}
