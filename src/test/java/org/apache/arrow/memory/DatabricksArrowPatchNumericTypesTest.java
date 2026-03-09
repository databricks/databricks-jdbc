package org.apache.arrow.memory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.condition.EnabledOnJre;
import org.junit.jupiter.api.condition.JRE;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Test numeric Arrow data types (integers, floats, decimals, booleans, nulls). */
@Tag("Jvm17PlusAndArrowToNioReflectionDisabled")
@EnabledOnJre({JRE.JAVA_17, JRE.JAVA_21})
public class DatabricksArrowPatchNumericTypesTest extends AbstractDatabricksArrowPatchTypesTest {

  /** Test read and write of integer types. */
  @ParameterizedTest
  @MethodSource("getBufferAllocators")
  public void testIntegerTypes(
      BufferAllocator readAllocator, BufferAllocator writeAllocator, int totalRows)
      throws Exception {
    DataTester testFloats = new TestIntegers();
    byte[] data = writeData(testFloats, totalRows, writeAllocator);
    readAndValidate(testFloats, data, readAllocator);
  }

  /** Test read and write of float types. */
  @ParameterizedTest
  @MethodSource("getBufferAllocators")
  public void testFloatTypes(
      BufferAllocator readAllocator, BufferAllocator writeAllocator, int totalRows)
      throws Exception {
    DataTester testFloats = new TestFloats();
    byte[] data = writeData(testFloats, totalRows, writeAllocator);
    readAndValidate(testFloats, data, readAllocator);
  }

  /** Test read and write of decimal types . */
  @ParameterizedTest
  @MethodSource("getBufferAllocators")
  public void testDecimalTypes(
      BufferAllocator readAllocator, BufferAllocator writeAllocator, int totalRows)
      throws Exception {
    DataTester testFloats = new TestDecimal();
    byte[] data = writeData(testFloats, totalRows, writeAllocator);
    readAndValidate(testFloats, data, readAllocator);
  }

  /** Test read and write of decimal256 types. */
  @ParameterizedTest
  @MethodSource("getBufferAllocators")
  public void testDecimal256Types(
      BufferAllocator readAllocator, BufferAllocator writeAllocator, int totalRows)
      throws Exception {
    DataTester testDecimal256 = new TestDecimal256();
    byte[] data = writeData(testDecimal256, totalRows, writeAllocator);
    readAndValidate(testDecimal256, data, readAllocator);
  }

  /** Test read and write of boolean types. */
  @ParameterizedTest
  @MethodSource("getBufferAllocators")
  public void testBoolTypes(
      BufferAllocator readAllocator, BufferAllocator writeAllocator, int totalRows)
      throws Exception {
    DataTester testBool = new TestBoolTypes();
    byte[] data = writeData(testBool, totalRows, writeAllocator);
    readAndValidate(testBool, data, readAllocator);
  }

  /** Test read and write of null types. */
  @ParameterizedTest
  @MethodSource("getBufferAllocators")
  public void testNullTypes(
      BufferAllocator readAllocator, BufferAllocator writeAllocator, int totalRows)
      throws Exception {
    DataTester testNull = new TestNullTypes();
    byte[] data = writeData(testNull, totalRows, writeAllocator);
    readAndValidate(testNull, data, readAllocator);
  }

  /** Test integers */
  private class TestIntegers implements DataTester {
    private final Field signedByteField = newSignedByteIntField();
    private final Field signedShortField = newSignedShortIntField();
    private final Field signedIntField = newSignedIntField();
    private final Field signedLongField = newSignedLongField();
    private final Field unsignedByteField = newUnsignedByteIntField();
    private final Field unsignedShortField = newUnsignedShortIntField();
    private final Field unsignedIntField = newUnsignedIntField();
    private final Field unsignedLongField = newUnsignedLongField();
    private final Schema schema =
        new Schema(
            Arrays.asList(
                signedByteField,
                signedShortField,
                signedIntField,
                signedLongField,
                unsignedByteField,
                unsignedShortField,
                unsignedIntField,
                unsignedLongField));

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void writeData(VectorSchemaRoot vectorSchemaRoot, int batchSize) {
      TinyIntVector signedByteInt =
          (TinyIntVector) vectorSchemaRoot.getVector(signedByteField.getName());
      SmallIntVector signedShortInt =
          (SmallIntVector) vectorSchemaRoot.getVector(signedShortField.getName());
      IntVector signedInt = (IntVector) vectorSchemaRoot.getVector(signedIntField.getName());
      BigIntVector signedLong =
          (BigIntVector) vectorSchemaRoot.getVector(signedLongField.getName());
      UInt1Vector unsignedByteInt =
          (UInt1Vector) vectorSchemaRoot.getVector(unsignedByteField.getName());
      UInt2Vector unsignedShortInt =
          (UInt2Vector) vectorSchemaRoot.getVector(unsignedShortField.getName());
      UInt4Vector unsignedInt =
          (UInt4Vector) vectorSchemaRoot.getVector(unsignedIntField.getName());
      UInt8Vector unsignedLong =
          (UInt8Vector) vectorSchemaRoot.getVector(unsignedLongField.getName());
      // Set signed bytes.
      signedByteInt.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          signedByteInt.setNull(i);
        } else {
          signedByteInt.set(i, getSignedByte(i));
        }
      }

      // Set signed shorts.
      signedShortInt.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          signedShortInt.setNull(i);
        } else {
          signedShortInt.set(i, getSignedShort(i));
        }
      }

      // Set signed ints.
      signedInt.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          signedInt.setNull(i);
        } else {
          signedInt.set(i, getSignedInt(i));
        }
      }

      // Set signed longs.
      signedLong.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          signedLong.setNull(i);
        } else {
          signedLong.set(i, getSignedLong(i));
        }
      }

      // Set unsigned bytes.
      unsignedByteInt.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          unsignedByteInt.setNull(i);
        } else {
          unsignedByteInt.set(i, getUnsignedByte(i));
        }
      }

      // Set unsigned shorts.
      unsignedShortInt.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          unsignedShortInt.setNull(i);
        } else {
          unsignedShortInt.set(i, getUnsignedShort(i));
        }
      }

      // Set unsigned ints.
      unsignedInt.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          unsignedInt.setNull(i);
        } else {
          unsignedInt.set(i, getUnsignedInt(i));
        }
      }

      // Set unsigned longs.
      unsignedLong.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          unsignedLong.setNull(i);
        } else {
          unsignedLong.set(i, getUnsignedLong(i));
        }
      }
    }

    @Override
    public void validateData(VectorSchemaRoot vectorSchemaRoot) {
      TinyIntVector signedByteInt =
          (TinyIntVector) vectorSchemaRoot.getVector(signedByteField.getName());
      SmallIntVector signedShortInt =
          (SmallIntVector) vectorSchemaRoot.getVector(signedShortField.getName());
      IntVector signedInt = (IntVector) vectorSchemaRoot.getVector(signedIntField.getName());
      BigIntVector signedLong =
          (BigIntVector) vectorSchemaRoot.getVector(signedLongField.getName());
      UInt1Vector unsignedByteInt =
          (UInt1Vector) vectorSchemaRoot.getVector(unsignedByteField.getName());
      UInt2Vector unsignedShortInt =
          (UInt2Vector) vectorSchemaRoot.getVector(unsignedShortField.getName());
      UInt4Vector unsignedInt =
          (UInt4Vector) vectorSchemaRoot.getVector(unsignedIntField.getName());
      UInt8Vector unsignedLong =
          (UInt8Vector) vectorSchemaRoot.getVector(unsignedLongField.getName());

      int rowCount = vectorSchemaRoot.getRowCount();
      // Validate all rows
      for (int i = 0; i < rowCount; i++) {
        // Validate signed byte
        if (i % 2 == 0) {
          assertTrue(signedByteInt.isNull(i), "Signed byte should be null at index " + i);
        } else {
          assertEquals(
              getSignedByte(i), signedByteInt.get(i), "Signed byte mismatch at index " + i);
        }

        // Validate signed short
        if (i % 2 == 0) {
          assertTrue(signedShortInt.isNull(i), "Signed short should be null at index " + i);
        } else {
          assertEquals(
              getSignedShort(i), signedShortInt.get(i), "Signed short mismatch at index " + i);
        }

        // Validate signed int
        if (i % 2 == 0) {
          assertTrue(signedInt.isNull(i), "Signed int should be null at index " + i);
        } else {
          assertEquals(getSignedInt(i), signedInt.get(i), "Signed int mismatch at index " + i);
        }

        // Validate signed long
        if (i % 2 == 0) {
          assertTrue(signedLong.isNull(i), "Signed long should be null at index " + i);
        } else {
          assertEquals(getSignedLong(i), signedLong.get(i), "Signed long mismatch at index " + i);
        }

        // Validate unsigned byte (convert to unsigned using Byte.toUnsignedInt)
        if (i % 2 == 0) {
          assertTrue(unsignedByteInt.isNull(i), "Unsigned byte should be null at index " + i);
        } else {
          assertEquals(
              getUnsignedByte(i),
              Byte.toUnsignedInt(unsignedByteInt.get(i)),
              "Unsigned byte mismatch at index " + i);
        }

        // Validate unsigned short (char is already unsigned in Java)
        if (i % 2 == 0) {
          assertTrue(unsignedShortInt.isNull(i), "Unsigned short should be null at index " + i);
        } else {
          assertEquals(
              getUnsignedShort(i),
              unsignedShortInt.get(i),
              "Unsigned short mismatch at index " + i);
        }

        // Validate unsigned int (convert to unsigned long for comparison)
        if (i % 2 == 0) {
          assertTrue(unsignedInt.isNull(i), "Unsigned int should be null at index " + i);
        } else {
          assertEquals(
              getUnsignedInt(i), unsignedInt.get(i), "Unsigned int mismatch at index " + i);
        }

        // Validate unsigned long
        if (i % 2 == 0) {
          assertTrue(unsignedLong.isNull(i), "Unsigned long should be null at index " + i);
        } else {
          assertEquals(
              getUnsignedLong(i), unsignedLong.get(i), "Unsigned long mismatch at index " + i);
        }
      }
    }
  }

  /** Test floats */
  private class TestFloats implements DataTester {
    Field floatField = newFloatField();
    Field doubleField = newDoubleField();
    Schema schema = new Schema(Arrays.asList(floatField, doubleField));

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void writeData(VectorSchemaRoot vectorSchemaRoot, int batchSize) {
      Float4Vector floatVector = (Float4Vector) vectorSchemaRoot.getVector(floatField.getName());
      Float8Vector doubleVector = (Float8Vector) vectorSchemaRoot.getVector(doubleField.getName());

      // Set floats.
      floatVector.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          floatVector.setNull(i);
        } else {
          floatVector.set(i, getFloat(i));
        }
      }

      // Set doubles.
      doubleVector.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          doubleVector.setNull(i);
        } else {
          doubleVector.set(i, getDouble(i));
        }
      }
    }

    @Override
    public void validateData(VectorSchemaRoot vectorSchemaRoot) {
      try (Float4Vector floatVector =
              (Float4Vector) vectorSchemaRoot.getVector(floatField.getName());
          Float8Vector doubleVector =
              (Float8Vector) vectorSchemaRoot.getVector(doubleField.getName())) {
        for (int i = 0; i < vectorSchemaRoot.getRowCount(); i++) {
          // Validate float
          if (i % 2 == 0) {
            assertTrue(floatVector.isNull(i), "Float should be null at index " + i);
          } else {
            assertEquals(getFloat(i), floatVector.get(i), 0.0001f, "Float mismatch at index " + i);
          }

          // Validate double
          if (i % 2 == 0) {
            assertTrue(doubleVector.isNull(i), "Double should be null at index " + i);
          } else {
            assertEquals(
                getDouble(i), doubleVector.get(i), 0.0001, "Double mismatch at index " + i);
          }
        }
      }
    }
  }

  /** Test decimals */
  private class TestDecimal implements DataTester {
    private final Field decimalFullPrecisionField = newDecimalField(38, 0, 128);
    private final Field decimalTenPrecisionField = newDecimalField(10, 5, 128);
    private final Field decimalTwentyPrecisionField = newDecimalField(20, 10, 128);
    private final Field decimalZeroScaleField = newDecimalField(16, 0, 128);
    private final Schema schema =
        new Schema(
            Arrays.asList(
                decimalFullPrecisionField,
                decimalTenPrecisionField,
                decimalTwentyPrecisionField,
                decimalZeroScaleField));

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void writeData(VectorSchemaRoot vectorSchemaRoot, int batchSize) {
      DecimalVector decimalFullPrecisionVector =
          (DecimalVector) vectorSchemaRoot.getVector(decimalFullPrecisionField.getName());
      DecimalVector decimalTenPrecisionVector =
          (DecimalVector) vectorSchemaRoot.getVector(decimalTenPrecisionField.getName());
      DecimalVector decimalTwentyPrecisionVector =
          (DecimalVector) vectorSchemaRoot.getVector(decimalTwentyPrecisionField.getName());
      DecimalVector decimalZeroScaleVector =
          (DecimalVector) vectorSchemaRoot.getVector(decimalZeroScaleField.getName());

      writeDecimals(decimalFullPrecisionVector, batchSize);
      writeDecimals(decimalTenPrecisionVector, batchSize);
      writeDecimals(decimalTwentyPrecisionVector, batchSize);
      writeDecimals(decimalZeroScaleVector, batchSize);
    }

    private void writeDecimals(DecimalVector decimalVector, int batchSize) {
      // Set decimals.
      decimalVector.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          decimalVector.setNull(i);
        } else {
          decimalVector.set(i, getDecimal(i, decimalVector.getScale()));
        }
      }
    }

    @Override
    public void validateData(VectorSchemaRoot vectorSchemaRoot) {
      DecimalVector decimalFullPrecisionVector =
          (DecimalVector) vectorSchemaRoot.getVector(decimalFullPrecisionField.getName());
      DecimalVector decimalTenPrecisionVector =
          (DecimalVector) vectorSchemaRoot.getVector(decimalTenPrecisionField.getName());
      DecimalVector decimalTwentyPrecisionVector =
          (DecimalVector) vectorSchemaRoot.getVector(decimalTwentyPrecisionField.getName());
      DecimalVector decimalZeroScaleVector =
          (DecimalVector) vectorSchemaRoot.getVector(decimalZeroScaleField.getName());

      int rowCount = vectorSchemaRoot.getRowCount();
      validateDecimals(decimalFullPrecisionVector, rowCount);
      validateDecimals(decimalTenPrecisionVector, rowCount);
      validateDecimals(decimalTwentyPrecisionVector, rowCount);
      validateDecimals(decimalZeroScaleVector, rowCount);
    }

    private void validateDecimals(DecimalVector decimalVector, int rowCount) {
      for (int i = 0; i < rowCount; i++) {
        // Validate decimal
        BigDecimal bigDecimal = decimalVector.getObject(i);
        if (i % 2 == 0) {
          assertNull(bigDecimal, "Decimal should be null at index " + i);
        } else {
          assertNotNull(bigDecimal, "Decimal should not be null at index " + i);
          assertEquals(
              getDecimal(i, decimalVector.getScale()),
              bigDecimal,
              "Decimal mismatch at index " + i);
        }
      }
    }
  }

  /** Test decimal256 */
  private class TestDecimal256 implements DataTester {
    private final Field decimalFullPrecisionField = newDecimalField(76, 10, 256);
    private final Field decimalTenPrecisionField = newDecimalField(10, 5, 256);
    private final Field decimalTwentyPrecisionField = newDecimalField(20, 10, 256);
    private final Field decimalZeroScaleField = newDecimalField(32, 0, 256);
    private final Schema schema =
        new Schema(
            Arrays.asList(
                decimalFullPrecisionField,
                decimalTenPrecisionField,
                decimalTwentyPrecisionField,
                decimalZeroScaleField));

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void writeData(VectorSchemaRoot vectorSchemaRoot, int batchSize) {
      Decimal256Vector decimalFullPrecisionVector =
          (Decimal256Vector) vectorSchemaRoot.getVector(decimalFullPrecisionField.getName());
      Decimal256Vector decimalTenPrecisionVector =
          (Decimal256Vector) vectorSchemaRoot.getVector(decimalTenPrecisionField.getName());
      Decimal256Vector decimalTwentyPrecisionVector =
          (Decimal256Vector) vectorSchemaRoot.getVector(decimalTwentyPrecisionField.getName());
      Decimal256Vector decimalZeroScaleVector =
          (Decimal256Vector) vectorSchemaRoot.getVector(decimalZeroScaleField.getName());

      writeDecimals(decimalFullPrecisionVector, batchSize);
      writeDecimals(decimalTenPrecisionVector, batchSize);
      writeDecimals(decimalTwentyPrecisionVector, batchSize);
      writeDecimals(decimalZeroScaleVector, batchSize);
    }

    private void writeDecimals(Decimal256Vector decimalVector, int batchSize) {
      // Set decimals.
      decimalVector.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          decimalVector.setNull(i);
        } else {
          BigDecimal value = getDecimal(i, decimalVector.getScale());
          decimalVector.set(i, value);
        }
      }
    }

    @Override
    public void validateData(VectorSchemaRoot vectorSchemaRoot) {
      Decimal256Vector decimalFullPrecisionVector =
          (Decimal256Vector) vectorSchemaRoot.getVector(decimalFullPrecisionField.getName());
      Decimal256Vector decimalTenPrecisionVector =
          (Decimal256Vector) vectorSchemaRoot.getVector(decimalTenPrecisionField.getName());
      Decimal256Vector decimalTwentyPrecisionVector =
          (Decimal256Vector) vectorSchemaRoot.getVector(decimalTwentyPrecisionField.getName());
      Decimal256Vector decimalZeroScaleVector =
          (Decimal256Vector) vectorSchemaRoot.getVector(decimalZeroScaleField.getName());

      int rowCount = vectorSchemaRoot.getRowCount();
      validateDecimals(decimalFullPrecisionVector, rowCount);
      validateDecimals(decimalTenPrecisionVector, rowCount);
      validateDecimals(decimalTwentyPrecisionVector, rowCount);
      validateDecimals(decimalZeroScaleVector, rowCount);
    }

    private void validateDecimals(Decimal256Vector decimalVector, int rowCount) {
      for (int i = 0; i < rowCount; i++) {
        // Validate decimal
        BigDecimal bigDecimal = decimalVector.getObject(i);
        if (i % 2 == 0) {
          assertNull(bigDecimal, "Decimal256 should be null at index " + i);
        } else {
          assertNotNull(bigDecimal, "Decimal256 should not be null at index " + i);
          assertEquals(
              getDecimal(i, decimalVector.getScale()),
              bigDecimal,
              "Decimal256 mismatch at index " + i);
        }
      }
    }
  }

  /** Test boolean types */
  private class TestBoolTypes implements DataTester {
    private final Field boolField;
    private final Schema schema;

    TestBoolTypes() {
      boolField = newBoolField();
      schema = new Schema(Collections.singletonList(boolField));
    }

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void writeData(VectorSchemaRoot vectorSchemaRoot, int batchSize) {
      BitVector boolVector = (BitVector) vectorSchemaRoot.getVector(boolField.getName());
      boolVector.allocateNew(batchSize);

      for (int i = 0; i < batchSize; i++) {
        if (i % 3 == 0) {
          boolVector.setNull(i);
        } else if (i % 2 == 0) {
          boolVector.set(i, 0); // false
        } else {
          boolVector.set(i, 1); // true
        }
      }
      boolVector.setValueCount(batchSize);
    }

    @Override
    public void validateData(VectorSchemaRoot vectorSchemaRoot) {
      BitVector boolVector = (BitVector) vectorSchemaRoot.getVector(boolField.getName());
      int rowCount = vectorSchemaRoot.getRowCount();

      for (int i = 0; i < rowCount; i++) {
        if (i % 3 == 0) {
          assertTrue(boolVector.isNull(i), "Bool should be null at index " + i);
        } else if (i % 2 == 0) {
          assertFalse(boolVector.isNull(i), "Bool should not be null at index " + i);
          assertEquals(0, boolVector.get(i), "Bool should be false (0) at index " + i);
        } else {
          assertFalse(boolVector.isNull(i), "Bool should not be null at index " + i);
          assertEquals(1, boolVector.get(i), "Bool should be true (1) at index " + i);
        }
      }
    }

    private Field newBoolField() {
      return new Field("bool-field", FieldType.nullable(new ArrowType.Bool()), null);
    }
  }

  /** Test null types */
  private class TestNullTypes implements DataTester {
    private final Field nullField;
    private final Schema schema;

    TestNullTypes() {
      nullField = newNullField();
      schema = new Schema(Collections.singletonList(nullField));
    }

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void writeData(VectorSchemaRoot vectorSchemaRoot, int batchSize) {
      NullVector nullVector = (NullVector) vectorSchemaRoot.getVector(nullField.getName());
      nullVector.allocateNew();
      nullVector.setValueCount(batchSize);
    }

    @Override
    public void validateData(VectorSchemaRoot vectorSchemaRoot) {
      NullVector nullVector = (NullVector) vectorSchemaRoot.getVector(nullField.getName());
      int rowCount = vectorSchemaRoot.getRowCount();

      for (int i = 0; i < rowCount; i++) {
        assertTrue(nullVector.isNull(i), "Null vector should be null at index " + i);
      }
    }

    private Field newNullField() {
      return new Field("null-field", FieldType.nullable(new ArrowType.Null()), null);
    }
  }
}
