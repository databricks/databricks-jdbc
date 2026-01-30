package com.databricks.jdbc.api.impl.arrow;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.DatabricksBufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnJre;
import org.junit.jupiter.api.condition.JRE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test the functionality of {@link ArrowBufferAllocator}. */
public class ArrowBufferAllocatorTest {
  /** Logger instance. */
  private static final Logger logger = LoggerFactory.getLogger(ArrowBufferAllocatorTest.class);

  /** Test that root allocator can be created. */
  @Test
  public void testCreateRootAllocator() throws IOException {
    try (BufferAllocator allocator = ArrowBufferAllocator.getBufferAllocator()) {
      assertInstanceOf(RootAllocator.class, allocator, "Should create RootAllocator");
      readAndWriteArrowData(allocator);
    }

    assertFalse(ArrowBufferAllocator.isUsingPatchedAllocator(), "Should use RootAllocator");
  }

  /**
   * Test that the fallback {@code DatabricksBufferAllocator} is used when the creation of {@code
   * RootAllocator} is not possible in the current JVM.
   */
  @Test
  @Tag("Jvm17PlusAndArrowToNioReflectionDisabled")
  @EnabledOnJre({JRE.JAVA_17, JRE.JAVA_21})
  public void testCreateDatabricksBufferAllocator() throws IOException {
    try (BufferAllocator allocator = ArrowBufferAllocator.getBufferAllocator()) {
      assertInstanceOf(
          DatabricksBufferAllocator.class, allocator, "Should create DatabricksBufferAllocator");
      readAndWriteArrowData(allocator);
    }

    assertTrue(
        ArrowBufferAllocator.isUsingPatchedAllocator(), "Should use DatabricksBufferAllocator");
  }

  /** Write and read a sample arrow data to validate that the BufferAllocator works. */
  private void readAndWriteArrowData(BufferAllocator allocator) throws IOException {
    // 1. Write sample data.
    Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
    Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Schema schemaPerson = new Schema(asList(name, age));
    try (VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, allocator)) {
      VarCharVector nameVector = (VarCharVector) vectorSchemaRoot.getVector("name");
      nameVector.allocateNew(3);
      nameVector.set(0, "David".getBytes());
      nameVector.set(1, "Gladis".getBytes());
      nameVector.set(2, "Juan".getBytes());
      IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
      ageVector.allocateNew(3);
      ageVector.set(0, 10);
      ageVector.set(1, 20);
      ageVector.set(2, 30);
      vectorSchemaRoot.setRowCount(3);
      ByteArrayOutputStream arrowData = new ByteArrayOutputStream();
      try (ArrowStreamWriter writer =
          new ArrowStreamWriter(vectorSchemaRoot, null, Channels.newChannel(arrowData))) {
        writer.start();
        writer.writeBatch();
        logger.info("Number of rows written: " + vectorSchemaRoot.getRowCount());
      }

      // 2. Read the sample data.
      int totalRecords = 0;
      try (ArrowStreamReader reader =
          new ArrowStreamReader(new ByteArrayInputStream(arrowData.toByteArray()), allocator)) {
        while (reader.loadNextBatch()) {
          totalRecords += reader.getVectorSchemaRoot().getRowCount();
        }
      }

      assertEquals(3, totalRecords, "Read 3 records");
    }
  }
}
