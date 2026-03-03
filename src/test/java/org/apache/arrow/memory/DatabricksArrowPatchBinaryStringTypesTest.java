package org.apache.arrow.memory;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ViewVarBinaryVector;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.condition.EnabledOnJre;
import org.junit.jupiter.api.condition.JRE;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Test binary and string Arrow data types. */
@Tag("Jvm17PlusAndArrowToNioReflectionDisabled")
@EnabledOnJre({JRE.JAVA_17, JRE.JAVA_21})
public class DatabricksArrowPatchBinaryStringTypesTest
    extends AbstractDatabricksArrowPatchTypesTest {

  /** Test read and write of binary types. */
  @ParameterizedTest
  @MethodSource("getBufferAllocators")
  public void testBinaryTypes(
      BufferAllocator readAllocator, BufferAllocator writeAllocator, int totalRows)
      throws Exception {
    DataTester testBinary = new TestBinaryTypes();
    byte[] data = writeData(testBinary, totalRows, writeAllocator);
    readAndValidate(testBinary, data, readAllocator);
  }

  /** Test read and write of UTF-8 string types. */
  @ParameterizedTest
  @MethodSource("getBufferAllocators")
  public void testUtf8Types(
      BufferAllocator readAllocator, BufferAllocator writeAllocator, int totalRows)
      throws Exception {
    DataTester testUtf8 = new TestUtf8Types();
    byte[] data = writeData(testUtf8, totalRows, writeAllocator);
    readAndValidate(testUtf8, data, readAllocator);
  }

  /** Test read and write of UTF8 view types. */
  @ParameterizedTest
  @MethodSource("getBufferAllocators")
  public void testUtf8ViewTypes(
      BufferAllocator readAllocator, BufferAllocator writeAllocator, int totalRows)
      throws Exception {
    DataTester testUtf8View = new TestUtf8ViewTypes();
    byte[] data = writeData(testUtf8View, totalRows, writeAllocator);
    readAndValidate(testUtf8View, data, readAllocator);
  }

  /** Test read and write of binary view types. */
  @ParameterizedTest
  @MethodSource("getBufferAllocators")
  public void testBinaryViewTypes(
      BufferAllocator readAllocator, BufferAllocator writeAllocator, int totalRows)
      throws Exception {
    DataTester testBinaryView = new TestBinaryViewTypes();
    byte[] data = writeData(testBinaryView, totalRows, writeAllocator);
    readAndValidate(testBinaryView, data, readAllocator);
  }

  /** Test binary types */
  private class TestBinaryTypes implements DataTester {
    private final Field fixedSizeBinaryField;
    private final Field varBinaryField;
    private final Field largeVarBinaryField;
    private final Schema schema;
    private final int FIXED_SIZE_BINARY_LENGTH = 16;
    private final int VAR_BINARY_LENGTH = 32;
    private final int LARGE_VAR_BINARY_LENGTH = 50;

    TestBinaryTypes() {
      fixedSizeBinaryField = newFixedSizeBinaryField();
      varBinaryField = newVarBinaryField();
      largeVarBinaryField = newLargeVarBinaryField();
      schema = new Schema(Arrays.asList(fixedSizeBinaryField, varBinaryField, largeVarBinaryField));
    }

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void writeData(VectorSchemaRoot vectorSchemaRoot, int batchSize) {
      FixedSizeBinaryVector fixedSizeBinaryVector =
          (FixedSizeBinaryVector) vectorSchemaRoot.getVector(fixedSizeBinaryField.getName());
      VarBinaryVector varBinaryVector =
          (VarBinaryVector) vectorSchemaRoot.getVector(varBinaryField.getName());
      LargeVarBinaryVector largeVarBinaryVector =
          (LargeVarBinaryVector) vectorSchemaRoot.getVector(largeVarBinaryField.getName());

      // Set fixed-size binary (16 bytes).
      fixedSizeBinaryVector.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          fixedSizeBinaryVector.setNull(i);
        } else {
          fixedSizeBinaryVector.set(i, getFixedSizeBinary(i, FIXED_SIZE_BINARY_LENGTH));
        }
      }

      // Set variable binary.
      varBinaryVector.allocateNew(batchSize * 20L, batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          varBinaryVector.setNull(i);
        } else {
          varBinaryVector.set(i, getVarBinary(i, VAR_BINARY_LENGTH));
        }
      }

      // Set large variable binary.
      largeVarBinaryVector.clear();
      largeVarBinaryVector.allocateNew(batchSize * 50L, batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          largeVarBinaryVector.setNull(i);
        } else {
          largeVarBinaryVector.set(i, getLargeVarBinary(i, LARGE_VAR_BINARY_LENGTH));
        }
      }
    }

    @Override
    public void validateData(VectorSchemaRoot vectorSchemaRoot) {
      FixedSizeBinaryVector fixedSizeBinaryVector =
          (FixedSizeBinaryVector) vectorSchemaRoot.getVector(fixedSizeBinaryField.getName());
      VarBinaryVector varBinaryVector =
          (VarBinaryVector) vectorSchemaRoot.getVector(varBinaryField.getName());
      LargeVarBinaryVector largeVarBinaryVector =
          (LargeVarBinaryVector) vectorSchemaRoot.getVector(largeVarBinaryField.getName());

      int rowCount = vectorSchemaRoot.getRowCount();

      for (int i = 0; i < rowCount; i++) {
        // Validate fixed-size binary (16 bytes)
        if (i % 2 == 0) {
          assertTrue(
              fixedSizeBinaryVector.isNull(i), "Fixed-size binary should be null at index " + i);
        } else {
          byte[] expected = getFixedSizeBinary(i, FIXED_SIZE_BINARY_LENGTH);
          byte[] actual = fixedSizeBinaryVector.get(i);
          assertNotNull(actual, "Fixed-size binary should not be null at index " + i);
          assertArrayEquals(expected, actual, "Fixed-size binary mismatch at index " + i);
        }

        // Validate variable binary
        if (i % 2 == 0) {
          assertTrue(varBinaryVector.isNull(i), "Variable binary should be null at index " + i);
        } else {
          byte[] expected = getVarBinary(i, VAR_BINARY_LENGTH);
          byte[] actual = varBinaryVector.get(i);
          assertNotNull(actual, "Variable binary should not be null at index " + i);
          assertArrayEquals(expected, actual, "Variable binary mismatch at index " + i);
        }

        // Validate large variable binary
        if (i % 2 == 0) {
          assertTrue(
              largeVarBinaryVector.isNull(i), "Large variable binary should be null at index " + i);
        } else {
          byte[] expected = getLargeVarBinary(i, LARGE_VAR_BINARY_LENGTH);
          byte[] actual = largeVarBinaryVector.get(i);
          assertNotNull(actual, "Large variable binary should not be null at index " + i);
          assertArrayEquals(expected, actual, "Large variable binary mismatch at index " + i);
        }
      }
    }
  }

  /** Test UTF-8 string types */
  private class TestUtf8Types implements DataTester {
    private final Field utf8Field;
    private final Field largeUtf8Field;
    private final Schema schema;

    TestUtf8Types() {
      utf8Field = newUtf8Field();
      largeUtf8Field = newLargeUtf8Field();
      schema = new Schema(Arrays.asList(utf8Field, largeUtf8Field));
    }

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void writeData(VectorSchemaRoot vectorSchemaRoot, int batchSize) {
      VarCharVector utf8Vector = (VarCharVector) vectorSchemaRoot.getVector(utf8Field.getName());
      LargeVarCharVector largeUtf8Vector =
          (LargeVarCharVector) vectorSchemaRoot.getVector(largeUtf8Field.getName());

      // Set UTF-8 strings.
      utf8Vector.allocateNew(batchSize * 50L, batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          utf8Vector.setNull(i);
        } else {
          utf8Vector.set(i, getUtf8String(i).getBytes(StandardCharsets.UTF_8));
        }
      }

      // Set large UTF-8 strings.
      largeUtf8Vector.clear();
      largeUtf8Vector.allocateNew(batchSize * 100L, batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          largeUtf8Vector.setNull(i);
        } else {
          largeUtf8Vector.set(i, getLargeUtf8String(i).getBytes(StandardCharsets.UTF_8));
        }
      }
    }

    @Override
    public void validateData(VectorSchemaRoot vectorSchemaRoot) {
      VarCharVector utf8Vector = (VarCharVector) vectorSchemaRoot.getVector(utf8Field.getName());
      LargeVarCharVector largeUtf8Vector =
          (LargeVarCharVector) vectorSchemaRoot.getVector(largeUtf8Field.getName());

      int rowCount = vectorSchemaRoot.getRowCount();

      for (int i = 0; i < rowCount; i++) {
        // Validate UTF-8 string
        if (i % 2 == 0) {
          assertTrue(utf8Vector.isNull(i), "UTF-8 string should be null at index " + i);
        } else {
          String expected = getUtf8String(i);
          byte[] actualBytes = utf8Vector.get(i);
          assertNotNull(actualBytes, "UTF-8 string should not be null at index " + i);
          String actual = new String(actualBytes, StandardCharsets.UTF_8);
          assertEquals(expected, actual, "UTF-8 string mismatch at index " + i);
        }

        // Validate large UTF-8 string
        if (i % 2 == 0) {
          assertTrue(largeUtf8Vector.isNull(i), "Large UTF-8 string should be null at index " + i);
        } else {
          String expected = getLargeUtf8String(i);
          byte[] actualBytes = largeUtf8Vector.get(i);
          assertNotNull(actualBytes, "Large UTF-8 string should not be null at index " + i);
          String actual = new String(actualBytes, StandardCharsets.UTF_8);
          assertEquals(expected, actual, "Large UTF-8 string mismatch at index " + i);
        }
      }
    }
  }

  /** Test UTF8 view types */
  private class TestUtf8ViewTypes implements DataTester {
    private final Field utf8ViewField;
    private final Schema schema;

    TestUtf8ViewTypes() {
      utf8ViewField = newUtf8ViewField();
      schema = new Schema(Collections.singletonList(utf8ViewField));
    }

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void writeData(VectorSchemaRoot vectorSchemaRoot, int batchSize) {
      ViewVarCharVector utf8ViewVector =
          (ViewVarCharVector) vectorSchemaRoot.getVector(utf8ViewField.getName());

      // Calculate the total bytes needed for the data buffer
      long totalDataBytes = 0;
      for (int i = 0; i < batchSize; i++) {
        byte[] bytes = getUtf8ViewString(i).getBytes(StandardCharsets.UTF_8);
        // Round to nearest power of 64.
        totalDataBytes += (bytes.length + 63) & ~63;
      }

      utf8ViewVector.allocateNew(totalDataBytes, batchSize);

      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          utf8ViewVector.setNull(i);
        } else {
          utf8ViewVector.set(i, getUtf8ViewString(i).getBytes(StandardCharsets.UTF_8));
        }
      }
      utf8ViewVector.setValueCount(batchSize);
    }

    @Override
    public void validateData(VectorSchemaRoot vectorSchemaRoot) {
      ViewVarCharVector utf8ViewVector =
          (ViewVarCharVector) vectorSchemaRoot.getVector(utf8ViewField.getName());
      int rowCount = vectorSchemaRoot.getRowCount();

      for (int i = 0; i < rowCount; i++) {
        if (i % 2 == 0) {
          assertTrue(utf8ViewVector.isNull(i), "UTF8 view string should be null at index " + i);
        } else {
          String expected = getUtf8ViewString(i);
          byte[] actualBytes = utf8ViewVector.get(i);
          assertNotNull(actualBytes, "UTF8 view string should not be null at index " + i);
          String actual = new String(actualBytes, StandardCharsets.UTF_8);
          assertEquals(expected, actual, "UTF8 view string mismatch at index " + i);
        }
      }
    }

    private Field newUtf8ViewField() {
      return new Field("utf8-view-string", FieldType.nullable(new ArrowType.Utf8View()), null);
    }

    private String getUtf8ViewString(int index) {
      // Strings of length <= 12 are inlined.
      // See https://arrow.apache.org/docs/format/Columnar.html#variable-size-binary-view-layout
      if (index % 3 == 0) {
        return "short-" + index;
      } else {
        return "Utf8View-" + index + "-StringData";
      }
    }
  }

  /** Test binary view types */
  private class TestBinaryViewTypes implements DataTester {
    private final Field binaryViewField;
    private final Schema schema;

    TestBinaryViewTypes() {
      binaryViewField = newBinaryViewField();
      schema = new Schema(Collections.singletonList(binaryViewField));
    }

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void writeData(VectorSchemaRoot vectorSchemaRoot, int batchSize) {
      ViewVarBinaryVector binaryViewVector =
          (ViewVarBinaryVector) vectorSchemaRoot.getVector(binaryViewField.getName());

      // Calculate the total bytes needed for the data buffer
      long totalDataBytes = 0;
      for (int i = 0; i < batchSize; i++) {
        byte[] bytes = getBinaryViewData(i);
        // Round to nearest power of 64.
        totalDataBytes += (bytes.length + 63) & ~63;
      }

      binaryViewVector.allocateNew(totalDataBytes, batchSize);

      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          binaryViewVector.setNull(i);
        } else {
          binaryViewVector.set(i, getBinaryViewData(i));
        }
      }
      binaryViewVector.setValueCount(batchSize);
    }

    @Override
    public void validateData(VectorSchemaRoot vectorSchemaRoot) {
      ViewVarBinaryVector binaryViewVector =
          (ViewVarBinaryVector) vectorSchemaRoot.getVector(binaryViewField.getName());
      int rowCount = vectorSchemaRoot.getRowCount();

      for (int i = 0; i < rowCount; i++) {
        if (i % 2 == 0) {
          assertTrue(binaryViewVector.isNull(i), "Binary view should be null at index " + i);
        } else {
          byte[] expected = getBinaryViewData(i);
          byte[] actual = binaryViewVector.get(i);
          assertNotNull(actual, "Binary view should not be null at index " + i);
          assertArrayEquals(expected, actual, "Binary view mismatch at index " + i);
        }
      }
    }

    private Field newBinaryViewField() {
      return new Field("binary-view", FieldType.nullable(new ArrowType.BinaryView()), null);
    }

    private byte[] getBinaryViewData(int index) {
      int length = (index % 20) + 1;
      byte[] data = new byte[length];
      for (int i = 0; i < length; i++) {
        data[i] = (byte) ((index * 5 + i) % 256);
      }
      return data;
    }
  }
}
