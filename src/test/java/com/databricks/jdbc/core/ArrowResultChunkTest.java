package com.databricks.jdbc.core;

import com.databricks.client.jdbc42.internal.apache.arrow.memory.RootAllocator;
import com.databricks.client.jdbc42.internal.apache.arrow.vector.FieldVector;
import com.databricks.client.jdbc42.internal.apache.arrow.vector.Float8Vector;
import com.databricks.client.jdbc42.internal.apache.arrow.vector.IntVector;
import com.databricks.client.jdbc42.internal.apache.arrow.vector.VectorSchemaRoot;
import com.databricks.client.jdbc42.internal.apache.arrow.vector.dictionary.DictionaryProvider;
import com.databricks.client.jdbc42.internal.apache.arrow.vector.ipc.ArrowStreamWriter;
import com.databricks.client.jdbc42.internal.apache.arrow.vector.ipc.ArrowWriter;
import com.databricks.client.jdbc42.internal.apache.arrow.vector.types.Types;
import com.databricks.client.jdbc42.internal.apache.arrow.vector.types.pojo.Field;
import com.databricks.client.jdbc42.internal.apache.arrow.vector.types.pojo.FieldType;
import com.databricks.client.jdbc42.internal.apache.arrow.vector.types.pojo.Schema;
import com.databricks.sdk.service.sql.ChunkInfo;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static java.lang.Math.min;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ArrowResultChunkTest {

    private Random random = new Random();
    private int rowsInRecordBatch = 20;

    private long totalRows = 110;

    /*
    If running into Arrow memory buffer error, run with jvm argument --add-opens java.base/java.nio=ALL-UNNAMED
    i.e. mvn test -DargLine="--add-opens java.base/java.nio=ALL-UNNAMED"
     */
    @Test
    public void testGetArrowDataFromInputStream() throws Exception {
        // Arrange
        ChunkInfo chunkInfo = new ChunkInfo()
                .setChunkIndex(0L)
                .setByteCount(200L)
                .setRowOffset(0L)
                .setRowCount(totalRows);
        ArrowResultChunk arrowResultChunk = new ArrowResultChunk(chunkInfo, new RootAllocator(Integer.MAX_VALUE));
        Schema schema = createTestSchema();
        Object[][] testData = createTestData(schema, (int) totalRows);
        File arrowFile = createTestArrowFile("TestFile", schema, testData, new RootAllocator(Integer.MAX_VALUE));

        // Act
        arrowResultChunk.getArrowDataFromInputStream(new FileInputStream(arrowFile));

        // Assert
        int totalRecordBatches = (int) ((totalRows + rowsInRecordBatch)/rowsInRecordBatch);
        assertEquals(arrowResultChunk.getRecordBatchCountInChunk(), totalRecordBatches);
    }

    private File createTestArrowFile(String fileName, Schema schema, Object[][] testData, RootAllocator allocator) throws IOException {
        File file = new File(fileName);
        int cols = testData.length;
        int rows = testData[0].length;
        VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator);
        ArrowWriter writer = new ArrowStreamWriter(vectorSchemaRoot, new DictionaryProvider.MapDictionaryProvider(), new FileOutputStream(file));
        writer.start();
        for(int j = 0; j < rows; j += rowsInRecordBatch) {
            int rowsToAddToRecordBatch = min(rowsInRecordBatch, rows - j);
            vectorSchemaRoot.setRowCount(rowsToAddToRecordBatch);
            for(int i = 0; i < cols; i++) {
                Types.MinorType type = Types.getMinorTypeForArrowType(schema.getFields().get(i).getType());
                FieldVector fieldVector = vectorSchemaRoot.getFieldVectors().get(i);
                if(type.equals(Types.MinorType.INT)) {
                    IntVector intVector = (IntVector) fieldVector;
                    intVector.setInitialCapacity(rowsToAddToRecordBatch);
                    for(int k = 1; k < rowsToAddToRecordBatch; k++) {
                        intVector.set(k, 1, (int) testData[i][j + k]);
                    }
                }
                else if(type.equals(Types.MinorType.FLOAT8)) {
                    Float8Vector float8Vector = (Float8Vector) fieldVector;
                    float8Vector.setInitialCapacity(rowsToAddToRecordBatch);
                    for(int k = 1; k < rowsToAddToRecordBatch; k++) {
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
        for(int i = 0; i < cols; i++) {
            Types.MinorType type = Types.getMinorTypeForArrowType(schema.getFields().get(i).getType());
            if(type.equals(Types.MinorType.INT)) {
                for(int j = 0; j < rows; j++) {
                    data[i][j] = random.nextInt();
                }
            }
            else if(type.equals(Types.MinorType.FLOAT8)) {
                for(int j = 0; j < rows; j++) {
                    data[i][j] = random.nextDouble();
                }
            }
        }
        return data;
    }
}
