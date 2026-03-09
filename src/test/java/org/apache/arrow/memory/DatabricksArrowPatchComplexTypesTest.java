package org.apache.arrow.memory;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Period;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.LargeListViewVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.ListViewVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.RunEndEncodedVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionLargeListWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.condition.EnabledOnJre;
import org.junit.jupiter.api.condition.JRE;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Test complex Arrow data types (lists, structs, maps, unions, dictionary, REE). */
@Tag("Jvm17PlusAndArrowToNioReflectionDisabled")
@EnabledOnJre({JRE.JAVA_17, JRE.JAVA_21})
public class DatabricksArrowPatchComplexTypesTest extends AbstractDatabricksArrowPatchTypesTest {

  /** Test read and write of list types. */
  @ParameterizedTest
  @MethodSource("getBufferAllocators")
  public void testListTypes(
      BufferAllocator readAllocator, BufferAllocator writeAllocator, int totalRows)
      throws Exception {
    DataTester testList = new TestListTypes();
    byte[] data = writeData(testList, totalRows, writeAllocator);
    readAndValidate(testList, data, readAllocator);
  }

  /** Test read and write of list view types. */
  @ParameterizedTest
  @MethodSource("getBufferAllocatorsSmallRows")
  public void testListViewTypes(
      BufferAllocator readAllocator, BufferAllocator writeAllocator, int totalRows)
      throws Exception {
    DataTester testListView = new TestListViewTypes();
    byte[] data = writeData(testListView, totalRows, writeAllocator);
    readAndValidate(testListView, data, readAllocator);
  }

  /** Test read and write of large list view types. */
  @ParameterizedTest
  @MethodSource("getBufferAllocatorsSmallRows")
  public void testLargeListViewTypes(
      BufferAllocator readAllocator, BufferAllocator writeAllocator, int totalRows)
      throws Exception {
    DataTester testLargeListView = new TestLargeListViewTypes();
    byte[] data = writeData(testLargeListView, totalRows, writeAllocator);
    readAndValidate(testLargeListView, data, readAllocator);
  }

  /** Test read and write of fixed-size list types. */
  @ParameterizedTest
  @MethodSource("getBufferAllocators")
  public void testFixedSizeListTypes(
      BufferAllocator readAllocator, BufferAllocator writeAllocator, int totalRows)
      throws Exception {
    DataTester testFixedSizeList = new TestFixedSizeListTypes();
    byte[] data = writeData(testFixedSizeList, totalRows, writeAllocator);
    readAndValidate(testFixedSizeList, data, readAllocator);
  }

  /** Test read and write of struct types. */
  @ParameterizedTest
  @MethodSource("getBufferAllocators")
  public void testStructTypes(
      BufferAllocator readAllocator, BufferAllocator writeAllocator, int totalRows)
      throws Exception {
    DataTester testStruct = new TestStructTypes();
    byte[] data = writeData(testStruct, totalRows, writeAllocator);
    readAndValidate(testStruct, data, readAllocator);
  }

  /** Test read and write of map types. */
  @ParameterizedTest
  @MethodSource("getBufferAllocators")
  public void testMapTypes(
      BufferAllocator readAllocator, BufferAllocator writeAllocator, int totalRows)
      throws Exception {
    DataTester testMap = new TestMapTypes();
    byte[] data = writeData(testMap, totalRows, writeAllocator);
    readAndValidate(testMap, data, readAllocator);
  }

  /** Test read and write of union types. */
  @ParameterizedTest
  @MethodSource("getBufferAllocators")
  public void testUnionTypes(
      BufferAllocator readAllocator, BufferAllocator writeAllocator, int totalRows)
      throws Exception {
    DataTester testUnion = new TestUnionTypes();
    byte[] data = writeData(testUnion, totalRows, writeAllocator);
    readAndValidate(testUnion, data, readAllocator);
  }

  /** Test read and write of dictionary types. */
  @ParameterizedTest
  @MethodSource("getBufferAllocators")
  public void testDictionaryTypes(
      BufferAllocator readAllocator, BufferAllocator writeAllocator, int totalRows)
      throws Exception {
    DataTester testDictionary = new TestDictionaryTypes();
    byte[] data = writeData(testDictionary, totalRows, writeAllocator);
    readAndValidate(testDictionary, data, readAllocator);
  }

  /** Test read and write of run-end encoded types. */
  @ParameterizedTest
  @MethodSource("getBufferAllocators")
  public void testRunEndEncodedTypes(
      BufferAllocator readAllocator, BufferAllocator writeAllocator, int totalRows)
      throws Exception {
    DataTester testRunEndEncoded = new TestRunEndEncodedTypes();
    byte[] data = writeData(testRunEndEncoded, totalRows, writeAllocator);
    readAndValidate(testRunEndEncoded, data, readAllocator);
  }

  /** Test list types */
  private class TestListTypes implements DataTester {
    private final Field listIntField;
    private final Field largeListIntField;
    private final Schema schema;

    TestListTypes() {
      listIntField = newListIntField();
      largeListIntField = newLargeListIntField();
      schema = new Schema(Arrays.asList(listIntField, largeListIntField));
    }

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void writeData(VectorSchemaRoot vectorSchemaRoot, int batchSize) {
      ListVector listIntVector = (ListVector) vectorSchemaRoot.getVector(listIntField.getName());
      LargeListVector largeListIntVector =
          (LargeListVector) vectorSchemaRoot.getVector(largeListIntField.getName());

      // Set list of integers.
      listIntVector.allocateNew();
      UnionListWriter listWriter = listIntVector.getWriter();
      for (int i = 0; i < batchSize; i++) {
        listWriter.setPosition(i);
        if (i % 2 == 0) {
          // Null list
          listWriter.writeNull();
        } else {
          // Write list with varying number of elements
          listWriter.startList();
          int listSize = getListSize(i);
          for (int j = 0; j < listSize; j++) {
            listWriter.integer().writeInt(getListElement(i, j));
          }
          listWriter.endList();
        }
      }
      listWriter.setValueCount(batchSize);

      // Set large list of integers.
      largeListIntVector.allocateNew();
      UnionLargeListWriter largeListWriter = largeListIntVector.getWriter();
      for (int i = 0; i < batchSize; i++) {
        largeListWriter.setPosition(i);
        if (i % 2 == 0) {
          // Null list
          largeListWriter.writeNull();
        } else {
          // Write list with varying number of elements
          largeListWriter.startList();
          int listSize = getLargeListSize(i);
          for (int j = 0; j < listSize; j++) {
            largeListWriter.integer().writeInt(getLargeListElement(i, j));
          }
          largeListWriter.endList();
        }
      }
      largeListWriter.setValueCount(batchSize);
    }

    @Override
    public void validateData(VectorSchemaRoot vectorSchemaRoot) {
      ListVector listIntVector = (ListVector) vectorSchemaRoot.getVector(listIntField.getName());
      LargeListVector largeListIntVector =
          (LargeListVector) vectorSchemaRoot.getVector(largeListIntField.getName());

      int rowCount = vectorSchemaRoot.getRowCount();

      for (int i = 0; i < rowCount; i++) {
        // Validate list of integers
        if (i % 2 == 0) {
          assertTrue(listIntVector.isNull(i), "List should be null at index " + i);
        } else {
          Object listObj = listIntVector.getObject(i);
          assertNotNull(listObj, "List should not be null at index " + i);
          int expectedSize = getListSize(i);
          @SuppressWarnings("unchecked")
          java.util.List<Integer> list = (java.util.List<Integer>) listObj;
          assertEquals(expectedSize, list.size(), "List size mismatch at index " + i);
          for (int j = 0; j < expectedSize; j++) {
            Integer element = list.get(j);
            assertEquals(
                getListElement(i, j),
                element,
                "List element mismatch at index " + i + "[" + j + "]");
          }
        }

        // Validate large list of integers
        if (i % 2 == 0) {
          assertTrue(largeListIntVector.isNull(i), "Large list should be null at index " + i);
        } else {
          Object listObj = largeListIntVector.getObject(i);
          assertNotNull(listObj, "Large list should not be null at index " + i);
          int expectedSize = getLargeListSize(i);
          @SuppressWarnings("unchecked")
          java.util.List<Integer> list = (java.util.List<Integer>) listObj;
          assertEquals(expectedSize, list.size(), "Large list size mismatch at index " + i);
          for (int j = 0; j < expectedSize; j++) {
            Integer element = list.get(j);
            assertEquals(
                getLargeListElement(i, j),
                element,
                "Large list element mismatch at index " + i + "[" + j + "]");
          }
        }
      }
    }
  }

  /** Test list view types */
  private class TestListViewTypes implements DataTester {
    private final Field listViewField;
    private final Schema schema;

    TestListViewTypes() {
      listViewField = newListViewField();
      schema = new Schema(Collections.singletonList(listViewField));
    }

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void writeData(VectorSchemaRoot vectorSchemaRoot, int batchSize) {
      ListViewVector listViewVector =
          (ListViewVector) vectorSchemaRoot.getVector(listViewField.getName());
      listViewVector.allocateNew();

      IntVector childVector = (IntVector) listViewVector.getDataVector();

      // Calculate total child elements needed
      int totalElements = 0;
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 != 0) {
          totalElements += getListViewSize(i);
        }
      }
      childVector.allocateNew(totalElements);

      // Populate child vector with all data first
      int childIndex = 0;
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          listViewVector.setNull(i);
        } else {
          int startOffset = listViewVector.startNewValue(i);
          int listSize = getListViewSize(i);
          for (int j = 0; j < listSize; j++) {
            childVector.set(startOffset + j, getListViewElement(i, j));
          }
          listViewVector.endValue(i, listSize);
        }
      }
      listViewVector.setValueCount(batchSize);
      childVector.setValueCount(childIndex);
    }

    @Override
    public void validateData(VectorSchemaRoot vectorSchemaRoot) {
      ListViewVector listViewVector =
          (ListViewVector) vectorSchemaRoot.getVector(listViewField.getName());
      int rowCount = vectorSchemaRoot.getRowCount();

      for (int i = 0; i < rowCount; i++) {
        if (i % 2 == 0) {
          assertTrue(listViewVector.isNull(i), "List view should be null at index " + i);
        } else {
          assertFalse(listViewVector.isNull(i), "List view should not be null at index " + i);
          List<?> list = listViewVector.getObject(i);
          assertNotNull(list, "List view should not be null at index " + i);
          int expectedSize = getListViewSize(i);
          assertEquals(expectedSize, list.size(), "List view size mismatch at index " + i);
          for (int j = 0; j < expectedSize; j++) {
            assertEquals(
                getListViewElement(i, j),
                list.get(j),
                "List view element mismatch at index " + i + "[" + j + "]");
          }
        }
      }
    }

    private Field newListViewField() {
      return new Field(
          "list-view-int",
          FieldType.nullable(new ArrowType.ListView()),
          Collections.singletonList(
              new Field("$data$", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    }

    private int getListViewSize(int index) {
      return (index % 5) + 1;
    }

    private int getListViewElement(int rowIndex, int elementIndex) {
      return rowIndex * 50 + elementIndex;
    }
  }

  /** Test large list view types */
  private class TestLargeListViewTypes implements DataTester {
    private final Field largeListViewField;
    private final Schema schema;

    TestLargeListViewTypes() {
      largeListViewField = newLargeListViewField();
      schema = new Schema(Collections.singletonList(largeListViewField));
    }

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void writeData(VectorSchemaRoot vectorSchemaRoot, int batchSize) {
      LargeListViewVector largeListViewVector =
          (LargeListViewVector) vectorSchemaRoot.getVector(largeListViewField.getName());
      largeListViewVector.allocateNew();

      IntVector childVector = (IntVector) largeListViewVector.getDataVector();

      // Calculate total child elements needed
      int totalElements = 0;
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 != 0) {
          totalElements += getLargeListViewSize(i);
        }
      }
      childVector.allocateNew(totalElements);

      // Populate child vector with all data first
      int childIndex = 0;
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          largeListViewVector.setNull(i);
        } else {
          long startOffset = largeListViewVector.startNewValue(i);
          int listSize = getLargeListViewSize(i);
          for (int j = 0; j < listSize; j++) {
            childVector.set((int) startOffset + j, getLargeListViewElement(i, j));
          }
          largeListViewVector.endValue(i, listSize);
        }
      }
      largeListViewVector.setValueCount(batchSize);
      childVector.setValueCount(childIndex);
    }

    @Override
    public void validateData(VectorSchemaRoot vectorSchemaRoot) {
      LargeListViewVector largeListViewVector =
          (LargeListViewVector) vectorSchemaRoot.getVector(largeListViewField.getName());
      int rowCount = vectorSchemaRoot.getRowCount();

      for (int i = 0; i < rowCount; i++) {
        if (i % 2 == 0) {
          assertTrue(largeListViewVector.isNull(i), "Large list view should be null at index " + i);
        } else {
          assertFalse(
              largeListViewVector.isNull(i), "Large list view should not be null at index " + i);
          List<?> list = largeListViewVector.getObject(i);
          assertNotNull(list, "Large list view should not be null at index " + i);
          int expectedSize = getLargeListViewSize(i);
          assertEquals(expectedSize, list.size(), "Large list view size mismatch at index " + i);
          for (int j = 0; j < expectedSize; j++) {
            assertEquals(
                getLargeListViewElement(i, j),
                list.get(j),
                "Large list view element mismatch at index " + i + "[" + j + "]");
          }
        }
      }
    }

    private Field newLargeListViewField() {
      return new Field(
          "large-list-view-int",
          FieldType.nullable(new ArrowType.LargeListView()),
          Collections.singletonList(
              new Field("$data$", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    }

    private int getLargeListViewSize(int index) {
      return (index % 7) + 1;
    }

    private int getLargeListViewElement(int rowIndex, int elementIndex) {
      return rowIndex * 100 + elementIndex;
    }
  }

  /** Test fixed-size list types */
  private class TestFixedSizeListTypes implements DataTester {
    private final Field fixedSizeListField;
    private final Schema schema;
    private final int LIST_SIZE = 3;

    TestFixedSizeListTypes() {
      fixedSizeListField = newFixedSizeListField();
      schema = new Schema(Collections.singletonList(fixedSizeListField));
    }

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void writeData(VectorSchemaRoot vectorSchemaRoot, int batchSize) {
      FixedSizeListVector fixedSizeListVector =
          (FixedSizeListVector) vectorSchemaRoot.getVector(fixedSizeListField.getName());
      fixedSizeListVector.allocateNew();

      IntVector childVector = (IntVector) fixedSizeListVector.getDataVector();
      childVector.allocateNew(batchSize * LIST_SIZE);

      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          fixedSizeListVector.setNull(i);
        } else {
          for (int j = 0; j < LIST_SIZE; j++) {
            childVector.set(i * LIST_SIZE + j, i * 10 + j);
          }
          fixedSizeListVector.setNotNull(i);
        }
      }
      fixedSizeListVector.setValueCount(batchSize);
    }

    @Override
    public void validateData(VectorSchemaRoot vectorSchemaRoot) {
      FixedSizeListVector fixedSizeListVector =
          (FixedSizeListVector) vectorSchemaRoot.getVector(fixedSizeListField.getName());
      int rowCount = vectorSchemaRoot.getRowCount();

      for (int i = 0; i < rowCount; i++) {
        if (i % 2 == 0) {
          assertTrue(fixedSizeListVector.isNull(i), "Fixed-size list should be null at index " + i);
        } else {
          assertFalse(
              fixedSizeListVector.isNull(i), "Fixed-size list should not be null at index " + i);
          List<?> list = fixedSizeListVector.getObject(i);
          assertNotNull(list, "Fixed-size list should not be null at index " + i);
          assertEquals(LIST_SIZE, list.size(), "Fixed-size list size mismatch at index " + i);
          for (int j = 0; j < LIST_SIZE; j++) {
            assertEquals(
                i * 10 + j,
                list.get(j),
                "Fixed-size list element mismatch at index " + i + "[" + j + "]");
          }
        }
      }
    }

    private Field newFixedSizeListField() {
      return new Field(
          "fixed-size-list",
          FieldType.nullable(new ArrowType.FixedSizeList(LIST_SIZE)),
          Collections.singletonList(
              new Field("$data$", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    }
  }

  /** Test struct types */
  private class TestStructTypes implements DataTester {
    private final Field structField;
    private final Schema schema;
    private final int VAR_BINARY_MAX_LENGTH = 30;

    TestStructTypes() {
      structField = newStructField();
      //noinspection ArraysAsListWithZeroOrOneArgument
      schema = new Schema(Arrays.asList(structField));
    }

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void writeData(VectorSchemaRoot vectorSchemaRoot, int batchSize) {
      StructVector structVector = (StructVector) vectorSchemaRoot.getVector(structField.getName());

      // Allocate struct vector
      structVector.allocateNew();

      // Get child vectors
      IntVector structIntVector =
          structVector.addOrGet(
              "s_int", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      BigIntVector structLongVector =
          structVector.addOrGet(
              "s_long", FieldType.nullable(new ArrowType.Int(64, true)), BigIntVector.class);
      Float4Vector structFloatVector =
          structVector.addOrGet(
              "s_float",
              FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
              Float4Vector.class);
      Float8Vector structDoubleVector =
          structVector.addOrGet(
              "s_double",
              FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
              Float8Vector.class);
      DecimalVector structDecimalVector =
          structVector.addOrGet(
              "s_decimal",
              FieldType.nullable(new ArrowType.Decimal(16, 0, 128)),
              DecimalVector.class);
      DateDayVector structDateVector =
          structVector.addOrGet(
              "s_date", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), DateDayVector.class);
      TimeStampMilliTZVector structTimestampVector =
          structVector.addOrGet(
              "s_timestamp",
              FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")),
              TimeStampMilliTZVector.class);
      DurationVector structDurationVector =
          structVector.addOrGet(
              "s_duration",
              FieldType.nullable(new ArrowType.Duration(TimeUnit.SECOND)),
              DurationVector.class);
      IntervalYearVector structIntervalVector =
          structVector.addOrGet(
              "s_interval",
              FieldType.nullable(new ArrowType.Interval(IntervalUnit.YEAR_MONTH)),
              IntervalYearVector.class);
      VarBinaryVector structBinaryVector =
          structVector.addOrGet(
              "s_binary", FieldType.nullable(new ArrowType.Binary()), VarBinaryVector.class);
      VarCharVector structUtf8Vector =
          structVector.addOrGet(
              "s_utf8", FieldType.nullable(new ArrowType.Utf8()), VarCharVector.class);
      ListVector structListVector =
          structVector.addOrGet(
              "s_list", FieldType.nullable(ArrowType.List.INSTANCE), ListVector.class);

      // Allocate child vectors
      structIntVector.allocateNew(batchSize);
      structLongVector.allocateNew(batchSize);
      structFloatVector.allocateNew(batchSize);
      structDoubleVector.allocateNew(batchSize);
      structDecimalVector.allocateNew(batchSize);
      structDateVector.allocateNew(batchSize);
      structTimestampVector.allocateNew(batchSize);
      structDurationVector.allocateNew(batchSize);
      structIntervalVector.allocateNew(batchSize);
      structBinaryVector.allocateNew(batchSize * 20L, batchSize);
      structUtf8Vector.allocateNew(batchSize * 50L, batchSize);
      structListVector.allocateNew();

      // Set struct values
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          structVector.setNull(i);
        } else {
          structVector.setIndexDefined(i);
          structIntVector.set(i, getSignedInt(i));
          structLongVector.set(i, getSignedLong(i));
          structFloatVector.set(i, getFloat(i));
          structDoubleVector.set(i, getDouble(i));
          structDecimalVector.set(i, getDecimal(i, structDecimalVector.getScale()));
          structDateVector.set(i, getDateDay(i));
          structTimestampVector.set(i, getTimestampMilli(i));
          structDurationVector.set(i, getDurationSec(i));
          structIntervalVector.set(i, getIntervalYearMonth(i));
          structBinaryVector.set(i, getVarBinary(i, VAR_BINARY_MAX_LENGTH));
          structUtf8Vector.set(i, getUtf8String(i).getBytes(StandardCharsets.UTF_8));

          // Set list in struct
          UnionListWriter listWriter = structListVector.getWriter();
          listWriter.setPosition(i);
          listWriter.startList();
          int listSize = getListSize(i);
          for (int j = 0; j < listSize; j++) {
            listWriter.integer().writeInt(getListElement(i, j));
          }
          listWriter.endList();
        }
      }
      structListVector.getWriter().setValueCount(batchSize);
      structVector.setValueCount(batchSize);
    }

    @Override
    public void validateData(VectorSchemaRoot vectorSchemaRoot) {
      StructVector structVector = (StructVector) vectorSchemaRoot.getVector(structField.getName());

      int rowCount = vectorSchemaRoot.getRowCount();

      for (int i = 0; i < rowCount; i++) {
        // Validate struct
        if (i % 2 == 0) {
          assertTrue(structVector.isNull(i), "Struct should be null at index " + i);
        } else {
          java.util.Map<String, ?> structMap = structVector.getObject(i);
          assertNotNull(structMap, "Struct should not be null at index " + i);

          // Validate int
          assertEquals(
              getSignedInt(i),
              ((Integer) structMap.get("s_int")).intValue(),
              "Struct int mismatch at index " + i);

          // Validate long
          assertEquals(
              getSignedLong(i),
              ((Long) structMap.get("s_long")).longValue(),
              "Struct long mismatch at index " + i);

          // Validate float
          assertEquals(
              getFloat(i),
              (Float) structMap.get("s_float"),
              0.0001f,
              "Struct float mismatch at index " + i);

          // Validate double
          assertEquals(
              getDouble(i),
              (Double) structMap.get("s_double"),
              0.0001,
              "Struct double mismatch at index " + i);

          // Validate decimal
          BigDecimal decimalValue = (BigDecimal) structMap.get("s_decimal");
          assertEquals(
              getDecimal(i, decimalValue.scale()),
              decimalValue,
              "Struct decimal mismatch at index " + i);

          // Validate date
          assertEquals(
              getDateDay(i),
              ((Integer) structMap.get("s_date")).intValue(),
              "Struct date mismatch at index " + i);

          // Validate timestamp
          assertEquals(
              getTimestampMilli(i),
              ((Long) structMap.get("s_timestamp")).longValue(),
              "Struct timestamp mismatch at index " + i);

          // Validate duration
          Duration durationValue = (Duration) structMap.get("s_duration");
          assertEquals(
              getDurationSec(i),
              durationValue.getSeconds(),
              "Struct duration mismatch at index " + i);

          // Validate interval
          assertEquals(
              getIntervalYearMonth(i),
              ((Period) structMap.get("s_interval")).getMonths(),
              "Struct interval mismatch at index " + i);

          // Validate binary
          byte[] binaryValue = (byte[]) structMap.get("s_binary");
          assertArrayEquals(
              getVarBinary(i, VAR_BINARY_MAX_LENGTH),
              binaryValue,
              "Struct binary mismatch at index " + i);

          // Validate utf8
          String utf8Value =
              new String(((Text) structMap.get("s_utf8")).getBytes(), StandardCharsets.UTF_8);
          assertEquals(getUtf8String(i), utf8Value, "Struct utf8 mismatch at index " + i);

          // Validate list
          @SuppressWarnings("unchecked")
          List<Integer> listValue = (List<Integer>) structMap.get("s_list");
          int expectedSize = getListSize(i);
          assertEquals(expectedSize, listValue.size(), "Struct list size mismatch at index " + i);
          for (int j = 0; j < expectedSize; j++) {
            assertEquals(
                getListElement(i, j),
                listValue.get(j),
                "Struct list element mismatch at index " + i + "[" + j + "]");
          }
        }
      }
    }

    private Field newStructField() {
      return new Field(
          "struct-all-types",
          FieldType.nullable(ArrowType.Struct.INSTANCE),
          Arrays.asList(
              new Field("s_int", FieldType.nullable(new ArrowType.Int(32, true)), null),
              new Field("s_long", FieldType.nullable(new ArrowType.Int(64, true)), null),
              new Field(
                  "s_float",
                  FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                  null),
              new Field(
                  "s_double",
                  FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                  null),
              new Field("s_decimal", FieldType.nullable(new ArrowType.Decimal(16, 0, 128)), null),
              new Field(
                  "s_date",
                  FieldType.nullable(
                      new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY)),
                  null),
              new Field(
                  "s_timestamp",
                  FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")),
                  null),
              new Field(
                  "s_duration", FieldType.nullable(new ArrowType.Duration(TimeUnit.SECOND)), null),
              new Field(
                  "s_interval",
                  FieldType.nullable(new ArrowType.Interval(IntervalUnit.YEAR_MONTH)),
                  null),
              new Field("s_binary", FieldType.nullable(new ArrowType.Binary()), null),
              new Field("s_utf8", FieldType.nullable(new ArrowType.Utf8()), null),
              new Field(
                  "s_list",
                  FieldType.nullable(ArrowType.List.INSTANCE),
                  java.util.Collections.singletonList(
                      new Field(
                          "$data$", FieldType.nullable(new ArrowType.Int(32, true)), null)))));
    }
  }

  /** Test map types with all value types from struct test */
  private class TestMapTypes implements DataTester {
    private final Field mapStringIntField;
    private final Field mapStringLongField;
    private final Field mapStringFloatField;
    private final Field mapStringDoubleField;
    private final Field mapStringDecimalField;
    private final Field mapStringDateField;
    private final Field mapStringTimestampField;
    private final Field mapStringDurationField;
    private final Field mapStringIntervalField;
    private final Field mapStringBinaryField;
    private final Field mapStringUtf8Field;
    private final Field mapStringListField;
    private final Schema schema;
    private final int VAR_BINARY_MAX_LENGTH = 32;
    private final int DECIMAL_PRECISION = 16;
    private final int DECIMAL_SCALE = 0;

    TestMapTypes() {
      mapStringIntField = newMapStringIntField();
      mapStringLongField = newMapStringLongField();
      mapStringFloatField = newMapStringFloatField();
      mapStringDoubleField = newMapStringDoubleField();
      mapStringDecimalField = newMapStringDecimalField();
      mapStringDateField = newMapStringDateField();
      mapStringTimestampField = newMapStringTimestampField();
      mapStringDurationField = newMapStringDurationField();
      mapStringIntervalField = newMapStringIntervalField();
      mapStringBinaryField = newMapStringBinaryField();
      mapStringUtf8Field = newMapStringUtf8Field();
      mapStringListField = newMapStringListField();
      schema =
          new Schema(
              Arrays.asList(
                  mapStringIntField,
                  mapStringLongField,
                  mapStringFloatField,
                  mapStringDoubleField,
                  mapStringDecimalField,
                  mapStringDateField,
                  mapStringTimestampField,
                  mapStringDurationField,
                  mapStringIntervalField,
                  mapStringBinaryField,
                  mapStringUtf8Field,
                  mapStringListField));
    }

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void writeData(VectorSchemaRoot vectorSchemaRoot, int batchSize) {
      writeMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringIntField.getName()), batchSize, "int");
      writeMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringLongField.getName()), batchSize, "long");
      writeMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringFloatField.getName()),
          batchSize,
          "float");
      writeMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringDoubleField.getName()),
          batchSize,
          "double");
      writeMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringDecimalField.getName()),
          batchSize,
          "decimal");
      writeMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringDateField.getName()), batchSize, "date");
      writeMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringTimestampField.getName()),
          batchSize,
          "timestamp");
      writeMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringDurationField.getName()),
          batchSize,
          "duration");
      writeMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringIntervalField.getName()),
          batchSize,
          "interval");
      writeMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringBinaryField.getName()),
          batchSize,
          "binary");
      writeMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringUtf8Field.getName()), batchSize, "utf8");
      writeMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringListField.getName()), batchSize, "list");
    }

    private void writeMapVector(MapVector mapVector, int batchSize, String valueType) {
      mapVector.allocateNew();
      StructVector structVector = (StructVector) mapVector.getDataVector();

      VarCharVector keyVector =
          structVector.addOrGet(
              "key", FieldType.notNullable(new ArrowType.Utf8()), VarCharVector.class);

      int maxMapEntries = (batchSize / 2) * 3 + 10;
      keyVector.allocateNew((long) batchSize * 30, maxMapEntries);

      switch (valueType) {
        case "int":
          {
            IntVector valueVector =
                structVector.addOrGet(
                    "value", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
            valueVector.allocateNew(maxMapEntries);
            for (int i = 0; i < batchSize; i++) {
              if (i % 2 == 0) {
                mapVector.setNull(i);
              } else {
                int start = mapVector.startNewValue(i);
                int mapSize = getMapSize(i);
                for (int j = 0; j < mapSize; j++) {
                  structVector.setIndexDefined(start + j);
                  keyVector.set(start + j, getMapStringKey(i, j).getBytes(StandardCharsets.UTF_8));
                  valueVector.set(start + j, getSignedInt(i * 10 + j));
                }
                mapVector.endValue(i, mapSize);
              }
            }
            break;
          }
        case "long":
          {
            BigIntVector valueVector =
                structVector.addOrGet(
                    "value", FieldType.nullable(new ArrowType.Int(64, true)), BigIntVector.class);
            valueVector.allocateNew(maxMapEntries);
            for (int i = 0; i < batchSize; i++) {
              if (i % 2 == 0) {
                mapVector.setNull(i);
              } else {
                int start = mapVector.startNewValue(i);
                int mapSize = getMapSize(i);
                for (int j = 0; j < mapSize; j++) {
                  structVector.setIndexDefined(start + j);
                  keyVector.set(start + j, getMapStringKey(i, j).getBytes(StandardCharsets.UTF_8));
                  valueVector.set(start + j, getSignedLong(i * 10 + j));
                }
                mapVector.endValue(i, mapSize);
              }
            }
            break;
          }
        case "float":
          {
            Float4Vector valueVector =
                structVector.addOrGet(
                    "value",
                    FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                    Float4Vector.class);
            valueVector.allocateNew(maxMapEntries);
            for (int i = 0; i < batchSize; i++) {
              if (i % 2 == 0) {
                mapVector.setNull(i);
              } else {
                int start = mapVector.startNewValue(i);
                int mapSize = getMapSize(i);
                for (int j = 0; j < mapSize; j++) {
                  structVector.setIndexDefined(start + j);
                  keyVector.set(start + j, getMapStringKey(i, j).getBytes(StandardCharsets.UTF_8));
                  valueVector.set(start + j, getFloat(i * 10 + j));
                }
                mapVector.endValue(i, mapSize);
              }
            }
            break;
          }
        case "double":
          {
            Float8Vector valueVector =
                structVector.addOrGet(
                    "value",
                    FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                    Float8Vector.class);
            valueVector.allocateNew(maxMapEntries);
            for (int i = 0; i < batchSize; i++) {
              if (i % 2 == 0) {
                mapVector.setNull(i);
              } else {
                int start = mapVector.startNewValue(i);
                int mapSize = getMapSize(i);
                for (int j = 0; j < mapSize; j++) {
                  structVector.setIndexDefined(start + j);
                  keyVector.set(start + j, getMapStringKey(i, j).getBytes(StandardCharsets.UTF_8));
                  valueVector.set(start + j, getDouble(i * 10 + j));
                }
                mapVector.endValue(i, mapSize);
              }
            }
            break;
          }
        case "decimal":
          {
            DecimalVector valueVector =
                structVector.addOrGet(
                    "value",
                    FieldType.nullable(new ArrowType.Decimal(16, 0, 128)),
                    DecimalVector.class);
            valueVector.allocateNew(maxMapEntries);
            for (int i = 0; i < batchSize; i++) {
              if (i % 2 == 0) {
                mapVector.setNull(i);
              } else {
                int start = mapVector.startNewValue(i);
                int mapSize = getMapSize(i);
                for (int j = 0; j < mapSize; j++) {
                  structVector.setIndexDefined(start + j);
                  keyVector.set(start + j, getMapStringKey(i, j).getBytes(StandardCharsets.UTF_8));
                  valueVector.set(start + j, getDecimal(i * 10 + j, valueVector.getScale()));
                }
                mapVector.endValue(i, mapSize);
              }
            }
            break;
          }
        case "date":
          {
            DateDayVector valueVector =
                structVector.addOrGet(
                    "value",
                    FieldType.nullable(
                        new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY)),
                    DateDayVector.class);
            valueVector.allocateNew(maxMapEntries);
            for (int i = 0; i < batchSize; i++) {
              if (i % 2 == 0) {
                mapVector.setNull(i);
              } else {
                int start = mapVector.startNewValue(i);
                int mapSize = getMapSize(i);
                for (int j = 0; j < mapSize; j++) {
                  structVector.setIndexDefined(start + j);
                  keyVector.set(start + j, getMapStringKey(i, j).getBytes(StandardCharsets.UTF_8));
                  valueVector.set(start + j, getDateDay(i * 10 + j));
                }
                mapVector.endValue(i, mapSize);
              }
            }
            break;
          }
        case "timestamp":
          {
            TimeStampMilliTZVector valueVector =
                structVector.addOrGet(
                    "value",
                    FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")),
                    TimeStampMilliTZVector.class);
            valueVector.allocateNew(maxMapEntries);
            for (int i = 0; i < batchSize; i++) {
              if (i % 2 == 0) {
                mapVector.setNull(i);
              } else {
                int start = mapVector.startNewValue(i);
                int mapSize = getMapSize(i);
                for (int j = 0; j < mapSize; j++) {
                  structVector.setIndexDefined(start + j);
                  keyVector.set(start + j, getMapStringKey(i, j).getBytes(StandardCharsets.UTF_8));
                  valueVector.set(start + j, getTimestampMilli(i * 10 + j));
                }
                mapVector.endValue(i, mapSize);
              }
            }
            break;
          }
        case "duration":
          {
            DurationVector valueVector =
                structVector.addOrGet(
                    "value",
                    FieldType.nullable(new ArrowType.Duration(TimeUnit.SECOND)),
                    DurationVector.class);
            valueVector.allocateNew(maxMapEntries);
            for (int i = 0; i < batchSize; i++) {
              if (i % 2 == 0) {
                mapVector.setNull(i);
              } else {
                int start = mapVector.startNewValue(i);
                int mapSize = getMapSize(i);
                for (int j = 0; j < mapSize; j++) {
                  structVector.setIndexDefined(start + j);
                  keyVector.set(start + j, getMapStringKey(i, j).getBytes(StandardCharsets.UTF_8));
                  valueVector.set(start + j, getDurationSec(i * 10 + j));
                }
                mapVector.endValue(i, mapSize);
              }
            }
            break;
          }
        case "interval":
          {
            IntervalYearVector valueVector =
                structVector.addOrGet(
                    "value",
                    FieldType.nullable(new ArrowType.Interval(IntervalUnit.YEAR_MONTH)),
                    IntervalYearVector.class);
            valueVector.allocateNew(maxMapEntries);
            for (int i = 0; i < batchSize; i++) {
              if (i % 2 == 0) {
                mapVector.setNull(i);
              } else {
                int start = mapVector.startNewValue(i);
                int mapSize = getMapSize(i);
                for (int j = 0; j < mapSize; j++) {
                  structVector.setIndexDefined(start + j);
                  keyVector.set(start + j, getMapStringKey(i, j).getBytes(StandardCharsets.UTF_8));
                  valueVector.set(start + j, getIntervalYearMonth(i * 10 + j));
                }
                mapVector.endValue(i, mapSize);
              }
            }
            break;
          }
        case "binary":
          {
            VarBinaryVector valueVector =
                structVector.addOrGet(
                    "value", FieldType.nullable(new ArrowType.Binary()), VarBinaryVector.class);
            valueVector.allocateNew((long) batchSize * 20, maxMapEntries);
            for (int i = 0; i < batchSize; i++) {
              if (i % 2 == 0) {
                mapVector.setNull(i);
              } else {
                int start = mapVector.startNewValue(i);
                int mapSize = getMapSize(i);
                for (int j = 0; j < mapSize; j++) {
                  structVector.setIndexDefined(start + j);
                  keyVector.set(start + j, getMapStringKey(i, j).getBytes(StandardCharsets.UTF_8));
                  valueVector.set(start + j, getVarBinary(i * 10 + j, VAR_BINARY_MAX_LENGTH));
                }
                mapVector.endValue(i, mapSize);
              }
            }
            break;
          }
        case "utf8":
          {
            VarCharVector valueVector =
                structVector.addOrGet(
                    "value", FieldType.nullable(new ArrowType.Utf8()), VarCharVector.class);
            valueVector.allocateNew((long) batchSize * 50, maxMapEntries);
            for (int i = 0; i < batchSize; i++) {
              if (i % 2 == 0) {
                mapVector.setNull(i);
              } else {
                int start = mapVector.startNewValue(i);
                int mapSize = getMapSize(i);
                for (int j = 0; j < mapSize; j++) {
                  structVector.setIndexDefined(start + j);
                  keyVector.set(start + j, getMapStringKey(i, j).getBytes(StandardCharsets.UTF_8));
                  valueVector.set(
                      start + j, getUtf8String(i * 10 + j).getBytes(StandardCharsets.UTF_8));
                }
                mapVector.endValue(i, mapSize);
              }
            }
            break;
          }
        case "list":
          {
            ListVector valueVector =
                structVector.addOrGet(
                    "value", FieldType.nullable(ArrowType.List.INSTANCE), ListVector.class);
            valueVector.allocateNew();
            UnionListWriter listWriter = valueVector.getWriter();

            int entryIndex = 0;
            for (int i = 0; i < batchSize; i++) {
              if (i % 2 == 0) {
                mapVector.setNull(i);
              } else {
                int start = mapVector.startNewValue(i);
                int mapSize = getMapSize(i);
                for (int j = 0; j < mapSize; j++) {
                  structVector.setIndexDefined(start + j);
                  keyVector.set(start + j, getMapStringKey(i, j).getBytes(StandardCharsets.UTF_8));

                  listWriter.setPosition(start + j);
                  listWriter.startList();
                  int listSize = getListSize(i * 10 + j);
                  for (int k = 0; k < listSize; k++) {
                    listWriter.integer().writeInt(getListElement(i * 10 + j, k));
                  }
                  listWriter.endList();
                  entryIndex++;
                }
                mapVector.endValue(i, mapSize);
              }
            }
            listWriter.setValueCount(entryIndex);
            break;
          }
      }
      mapVector.setValueCount(batchSize);
    }

    @Override
    public void validateData(VectorSchemaRoot vectorSchemaRoot) {
      validateMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringIntField.getName()),
          vectorSchemaRoot.getRowCount(),
          "int");
      validateMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringLongField.getName()),
          vectorSchemaRoot.getRowCount(),
          "long");
      validateMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringFloatField.getName()),
          vectorSchemaRoot.getRowCount(),
          "float");
      validateMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringDoubleField.getName()),
          vectorSchemaRoot.getRowCount(),
          "double");
      validateMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringDecimalField.getName()),
          vectorSchemaRoot.getRowCount(),
          "decimal");
      validateMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringDateField.getName()),
          vectorSchemaRoot.getRowCount(),
          "date");
      validateMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringTimestampField.getName()),
          vectorSchemaRoot.getRowCount(),
          "timestamp");
      validateMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringDurationField.getName()),
          vectorSchemaRoot.getRowCount(),
          "duration");
      validateMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringIntervalField.getName()),
          vectorSchemaRoot.getRowCount(),
          "interval");
      validateMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringBinaryField.getName()),
          vectorSchemaRoot.getRowCount(),
          "binary");
      validateMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringUtf8Field.getName()),
          vectorSchemaRoot.getRowCount(),
          "utf8");
      validateMapVector(
          (MapVector) vectorSchemaRoot.getVector(mapStringListField.getName()),
          vectorSchemaRoot.getRowCount(),
          "list");
    }

    @SuppressWarnings("unchecked")
    private void validateMapVector(MapVector mapVector, int rowCount, String valueType) {
      for (int i = 0; i < rowCount; i++) {
        if (i % 2 == 0) {
          assertTrue(
              mapVector.isNull(i), "Map<String, " + valueType + "> should be null at index " + i);
        } else {
          List<?> mapList = mapVector.getObject(i);
          assertNotNull(mapList, "Map<String, " + valueType + "> should not be null at index " + i);
          int expectedSize = getMapSize(i);
          assertEquals(
              expectedSize,
              mapList.size(),
              "Map<String, " + valueType + "> size mismatch at index " + i);

          for (int j = 0; j < expectedSize; j++) {
            java.util.Map<String, ?> entry = (java.util.Map<String, ?>) mapList.get(j);
            String expectedKey = getMapStringKey(i, j);
            assertEquals(
                expectedKey,
                new String(((Text) entry.get("key")).getBytes(), StandardCharsets.UTF_8),
                "Map key mismatch at index " + i + "[" + j + "]");

            Object value = entry.get("value");
            switch (valueType) {
              case "int":
                assertEquals(
                    getSignedInt(i * 10 + j),
                    ((Integer) value).intValue(),
                    "Map int value mismatch at index " + i + "[" + j + "]");
                break;
              case "long":
                assertEquals(
                    getSignedLong(i * 10 + j),
                    ((Long) value).longValue(),
                    "Map long value mismatch at index " + i + "[" + j + "]");
                break;
              case "float":
                assertEquals(
                    getFloat(i * 10 + j),
                    (Float) value,
                    0.0001f,
                    "Map float value mismatch at index " + i + "[" + j + "]");
                break;
              case "double":
                assertEquals(
                    getDouble(i * 10 + j),
                    (Double) value,
                    0.0001,
                    "Map double value mismatch at index " + i + "[" + j + "]");
                break;
              case "decimal":
                assertEquals(
                    getDecimal(i * 10 + j, DECIMAL_SCALE),
                    value,
                    "Map decimal value mismatch at index " + i + "[" + j + "]");
                break;
              case "date":
                assertEquals(
                    getDateDay(i * 10 + j),
                    ((Integer) value).intValue(),
                    "Map date value mismatch at index " + i + "[" + j + "]");
                break;
              case "timestamp":
                assertEquals(
                    getTimestampMilli(i * 10 + j),
                    ((Long) value).longValue(),
                    "Map timestamp value mismatch at index " + i + "[" + j + "]");
                break;
              case "duration":
                assertEquals(
                    getDurationSec(i * 10 + j),
                    ((Duration) value).getSeconds(),
                    "Map duration value mismatch at index " + i + "[" + j + "]");
                break;
              case "interval":
                assertEquals(
                    getIntervalYearMonth(i * 10 + j),
                    ((Period) value).getMonths(),
                    "Map interval value mismatch at index " + i + "[" + j + "]");
                break;
              case "binary":
                assertArrayEquals(
                    getVarBinary(i * 10 + j, VAR_BINARY_MAX_LENGTH),
                    (byte[]) value,
                    "Map binary value mismatch at index " + i + "[" + j + "]");
                break;
              case "utf8":
                assertEquals(
                    getUtf8String(i * 10 + j),
                    new String(((Text) value).getBytes(), StandardCharsets.UTF_8),
                    "Map utf8 value mismatch at index " + i + "[" + j + "]");
                break;
              case "list":
                java.util.List<Integer> listValue = (java.util.List<Integer>) value;
                int expectedListSize = getListSize(i * 10 + j);
                assertEquals(
                    expectedListSize,
                    listValue.size(),
                    "Map list value size mismatch at index " + i + "[" + j + "]");
                for (int k = 0; k < expectedListSize; k++) {
                  assertEquals(
                      getListElement(i * 10 + j, k),
                      listValue.get(k),
                      "Map list element mismatch at index " + i + "[" + j + "][" + k + "]");
                }
                break;
            }
          }
        }
      }
    }

    private Field newMapStringIntField() {
      return new Field(
          "map-string-int",
          FieldType.nullable(new ArrowType.Map(false)),
          java.util.Collections.singletonList(
              new Field(
                  "entries",
                  FieldType.notNullable(ArrowType.Struct.INSTANCE),
                  Arrays.asList(
                      new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                      new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)))));
    }

    private Field newMapStringLongField() {
      return new Field(
          "map-string-long",
          FieldType.nullable(new ArrowType.Map(false)),
          java.util.Collections.singletonList(
              new Field(
                  "entries",
                  FieldType.notNullable(ArrowType.Struct.INSTANCE),
                  Arrays.asList(
                      new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                      new Field("value", FieldType.nullable(new ArrowType.Int(64, true)), null)))));
    }

    private Field newMapStringFloatField() {
      return new Field(
          "map-string-float",
          FieldType.nullable(new ArrowType.Map(false)),
          java.util.Collections.singletonList(
              new Field(
                  "entries",
                  FieldType.notNullable(ArrowType.Struct.INSTANCE),
                  Arrays.asList(
                      new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                      new Field(
                          "value",
                          FieldType.nullable(
                              new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                          null)))));
    }

    private Field newMapStringDoubleField() {
      return new Field(
          "map-string-double",
          FieldType.nullable(new ArrowType.Map(false)),
          java.util.Collections.singletonList(
              new Field(
                  "entries",
                  FieldType.notNullable(ArrowType.Struct.INSTANCE),
                  Arrays.asList(
                      new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                      new Field(
                          "value",
                          FieldType.nullable(
                              new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                          null)))));
    }

    private Field newMapStringDecimalField() {
      return new Field(
          "map-string-decimal",
          FieldType.nullable(new ArrowType.Map(false)),
          java.util.Collections.singletonList(
              new Field(
                  "entries",
                  FieldType.notNullable(ArrowType.Struct.INSTANCE),
                  Arrays.asList(
                      new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                      new Field(
                          "value",
                          FieldType.nullable(
                              new ArrowType.Decimal(DECIMAL_PRECISION, DECIMAL_SCALE, 128)),
                          null)))));
    }

    private Field newMapStringDateField() {
      return new Field(
          "map-string-date",
          FieldType.nullable(new ArrowType.Map(false)),
          java.util.Collections.singletonList(
              new Field(
                  "entries",
                  FieldType.notNullable(ArrowType.Struct.INSTANCE),
                  Arrays.asList(
                      new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                      new Field(
                          "value",
                          FieldType.nullable(
                              new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY)),
                          null)))));
    }

    private Field newMapStringTimestampField() {
      return new Field(
          "map-string-timestamp",
          FieldType.nullable(new ArrowType.Map(false)),
          java.util.Collections.singletonList(
              new Field(
                  "entries",
                  FieldType.notNullable(ArrowType.Struct.INSTANCE),
                  Arrays.asList(
                      new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                      new Field(
                          "value",
                          FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")),
                          null)))));
    }

    private Field newMapStringDurationField() {
      return new Field(
          "map-string-duration",
          FieldType.nullable(new ArrowType.Map(false)),
          java.util.Collections.singletonList(
              new Field(
                  "entries",
                  FieldType.notNullable(ArrowType.Struct.INSTANCE),
                  Arrays.asList(
                      new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                      new Field(
                          "value",
                          FieldType.nullable(new ArrowType.Duration(TimeUnit.SECOND)),
                          null)))));
    }

    private Field newMapStringIntervalField() {
      return new Field(
          "map-string-interval",
          FieldType.nullable(new ArrowType.Map(false)),
          java.util.Collections.singletonList(
              new Field(
                  "entries",
                  FieldType.notNullable(ArrowType.Struct.INSTANCE),
                  Arrays.asList(
                      new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                      new Field(
                          "value",
                          FieldType.nullable(new ArrowType.Interval(IntervalUnit.YEAR_MONTH)),
                          null)))));
    }

    private Field newMapStringBinaryField() {
      return new Field(
          "map-string-binary",
          FieldType.nullable(new ArrowType.Map(false)),
          java.util.Collections.singletonList(
              new Field(
                  "entries",
                  FieldType.notNullable(ArrowType.Struct.INSTANCE),
                  Arrays.asList(
                      new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                      new Field("value", FieldType.nullable(new ArrowType.Binary()), null)))));
    }

    private Field newMapStringUtf8Field() {
      return new Field(
          "map-string-utf8",
          FieldType.nullable(new ArrowType.Map(false)),
          java.util.Collections.singletonList(
              new Field(
                  "entries",
                  FieldType.notNullable(ArrowType.Struct.INSTANCE),
                  Arrays.asList(
                      new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                      new Field("value", FieldType.nullable(new ArrowType.Utf8()), null)))));
    }

    private Field newMapStringListField() {
      return new Field(
          "map-string-list",
          FieldType.nullable(new ArrowType.Map(false)),
          java.util.Collections.singletonList(
              new Field(
                  "entries",
                  FieldType.notNullable(ArrowType.Struct.INSTANCE),
                  Arrays.asList(
                      new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                      new Field(
                          "value",
                          FieldType.nullable(ArrowType.List.INSTANCE),
                          java.util.Collections.singletonList(
                              new Field(
                                  "$data$",
                                  FieldType.nullable(new ArrowType.Int(32, true)),
                                  null)))))));
    }

    private int getMapSize(int index) {
      return (index % 3) + 1;
    }

    private String getMapStringKey(int rowIndex, int entryIndex) {
      return "key_" + rowIndex + "_" + entryIndex;
    }
  }

  private class TestUnionTypes implements DataTester {
    private final Field unionField;
    private final Schema schema;
    private final int VAR_BINARY_LENGTH = 64;
    private final int DECIMAL_PRECISION = 16;
    private final int DECIMAL_SCALE = 0;

    TestUnionTypes() {
      unionField = newUnionField();
      schema = new Schema(java.util.Collections.singletonList(unionField));
    }

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void writeData(VectorSchemaRoot root, int totalRows) {
      DenseUnionVector unionVector = (DenseUnionVector) root.getVector(unionField.getName());

      // Set Union - cycles through all types
      unionVector.allocateNew();

      // Get child vectors by type ID (0-10)
      IntVector unionIntVector = (IntVector) unionVector.getVectorByType((byte) 0);
      BigIntVector unionLongVector = (BigIntVector) unionVector.getVectorByType((byte) 1);
      Float4Vector unionFloatVector = (Float4Vector) unionVector.getVectorByType((byte) 2);
      Float8Vector unionDoubleVector = (Float8Vector) unionVector.getVectorByType((byte) 3);
      DecimalVector unionDecimalVector = (DecimalVector) unionVector.getVectorByType((byte) 4);
      DateDayVector unionDateVector = (DateDayVector) unionVector.getVectorByType((byte) 5);
      TimeStampMilliTZVector unionTimestampVector =
          (TimeStampMilliTZVector) unionVector.getVectorByType((byte) 6);
      DurationVector unionDurationVector = (DurationVector) unionVector.getVectorByType((byte) 7);
      IntervalYearVector unionIntervalVector =
          (IntervalYearVector) unionVector.getVectorByType((byte) 8);
      VarBinaryVector unionBinaryVector = (VarBinaryVector) unionVector.getVectorByType((byte) 9);
      VarCharVector unionUtf8Vector = (VarCharVector) unionVector.getVectorByType((byte) 10);

      // Track offsets for each type (11 types: 0-10)
      int[] typeOffsets = new int[11];

      for (int i = 0; i < totalRows; i++) {
        // Cycle through different types based on index mod 11
        int typeIndex = i % 11;
        byte typeId = (byte) typeIndex;

        // Get the current offset for this type
        int offset = typeOffsets[typeIndex];

        // Set type ID and offset for this position
        unionVector.setTypeId(i, typeId);
        unionVector.setOffset(i, offset);

        switch (typeIndex) {
          case 0: // Int
            unionIntVector.setSafe(offset, getSignedInt(i));
            break;
          case 1: // Long
            unionLongVector.setSafe(offset, getSignedLong(i));
            break;
          case 2: // Float
            unionFloatVector.setSafe(offset, getFloat(i));
            break;
          case 3: // Double
            unionDoubleVector.setSafe(offset, getDouble(i));
            break;
          case 4: // Decimal
            unionDecimalVector.setSafe(offset, getDecimal(i, unionDecimalVector.getScale()));
            break;
          case 5: // Date
            unionDateVector.setSafe(offset, getDateDay(i));
            break;
          case 6: // Timestamp
            unionTimestampVector.setSafe(offset, getTimestampMilli(i));
            break;
          case 7: // Duration
            unionDurationVector.setSafe(offset, getDurationSec(i));
            break;
          case 8: // Interval Year-Month
            unionIntervalVector.setSafe(offset, getIntervalYearMonth(i));
            break;
          case 9: // Binary
            unionBinaryVector.setSafe(offset, getVarBinary(i, VAR_BINARY_LENGTH));
            break;
          case 10: // UTF8
            unionUtf8Vector.setSafe(offset, getUtf8String(i).getBytes(StandardCharsets.UTF_8));
            break;
        }

        // Increment the offset for this type
        typeOffsets[typeIndex]++;
      }
      unionVector.setValueCount(totalRows);
    }

    @Override
    public void validateData(VectorSchemaRoot root) {
      DenseUnionVector unionVector = (DenseUnionVector) root.getVector(unionField.getName());
      int rowCount = root.getRowCount();

      for (int i = 0; i < rowCount; i++) {
        // Validate Union
        assertFalse(unionVector.isNull(i), "Union should not be null at index " + i);
        int typeIndex = i % 11;
        Object unionValue = unionVector.getObject(i);
        assertNotNull(unionValue, "Union value should not be null at index " + i);

        switch (typeIndex) {
          case 0: // Int
            assertEquals(
                getSignedInt(i),
                ((Integer) unionValue).intValue(),
                "Union int mismatch at index " + i);
            break;
          case 1: // Long
            assertEquals(
                getSignedLong(i),
                ((Long) unionValue).longValue(),
                "Union long mismatch at index " + i);
            break;
          case 2: // Float
            assertEquals(
                getFloat(i), (Float) unionValue, 0.0001f, "Union float mismatch at index " + i);
            break;
          case 3: // Double
            assertEquals(
                getDouble(i), (Double) unionValue, 0.0001, "Union double mismatch at index " + i);
            break;
          case 4: // Decimal
            assertEquals(
                getDecimal(i, DECIMAL_SCALE), unionValue, "Union decimal mismatch at index " + i);
            break;
          case 5: // Date
            assertEquals(
                getDateDay(i),
                ((Integer) unionValue).intValue(),
                "Union date mismatch at index " + i);
            break;
          case 6: // Timestamp
            assertEquals(
                getTimestampMilli(i),
                ((Long) unionValue).longValue(),
                "Union timestamp mismatch at index " + i);
            break;
          case 7: // Duration
            assertEquals(
                getDurationSec(i),
                ((java.time.Duration) unionValue).getSeconds(),
                "Union duration mismatch at index " + i);
            break;
          case 8: // Interval
            assertEquals(
                getIntervalYearMonth(i),
                ((Period) unionValue).getMonths(),
                "Union interval mismatch at index " + i);
            break;
          case 9: // Binary
            assertArrayEquals(
                getVarBinary(i, VAR_BINARY_LENGTH),
                (byte[]) unionValue,
                "Union binary mismatch at index " + i);
            break;
          case 10: // UTF8
            assertEquals(
                getUtf8String(i),
                new String(((Text) unionValue).getBytes(), StandardCharsets.UTF_8),
                "Union utf8 mismatch at index " + i);
            break;
        }
      }
    }

    private Field newUnionField() {
      return new Field(
          "union-all-types",
          FieldType.nullable(
              new ArrowType.Union(UnionMode.Dense, new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10})),
          Arrays.asList(
              new Field("u_int", FieldType.nullable(new ArrowType.Int(32, true)), null),
              new Field("u_long", FieldType.nullable(new ArrowType.Int(64, true)), null),
              new Field(
                  "u_float",
                  FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                  null),
              new Field(
                  "u_double",
                  FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                  null),
              new Field(
                  "u_decimal",
                  FieldType.nullable(new ArrowType.Decimal(DECIMAL_PRECISION, DECIMAL_SCALE, 128)),
                  null),
              new Field(
                  "u_date",
                  FieldType.nullable(
                      new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY)),
                  null),
              new Field(
                  "u_timestamp",
                  FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")),
                  null),
              new Field(
                  "u_duration", FieldType.nullable(new ArrowType.Duration(TimeUnit.SECOND)), null),
              new Field(
                  "u_interval",
                  FieldType.nullable(new ArrowType.Interval(IntervalUnit.YEAR_MONTH)),
                  null),
              new Field("u_binary", FieldType.nullable(new ArrowType.Binary()), null),
              new Field("u_utf8", FieldType.nullable(new ArrowType.Utf8()), null)));
    }
  }

  /** Test dictionary types */
  private class TestDictionaryTypes implements DataTester {
    private final Field dictStringField;
    private final Field dictIntField;
    private final Schema schema;

    TestDictionaryTypes() {
      dictStringField = newDictStringField();
      dictIntField = newDictIntField();
      schema = new Schema(Arrays.asList(dictStringField, dictIntField));
    }

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void writeData(VectorSchemaRoot vectorSchemaRoot, int batchSize) {
      VarCharVector dictStringVector =
          (VarCharVector) vectorSchemaRoot.getVector(dictStringField.getName());
      IntVector dictIntVector = (IntVector) vectorSchemaRoot.getVector(dictIntField.getName());

      // Set dictionary-encoded strings (repeating pattern of values)
      dictStringVector.allocateNew(batchSize * 20L, batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          dictStringVector.setNull(i);
        } else {
          dictStringVector.set(i, getDictString(i).getBytes(StandardCharsets.UTF_8));
        }
      }

      // Set dictionary-encoded integers (repeating pattern of values)
      dictIntVector.allocateNew(batchSize);
      for (int i = 0; i < batchSize; i++) {
        if (i % 2 == 0) {
          dictIntVector.setNull(i);
        } else {
          dictIntVector.set(i, getDictInt(i));
        }
      }
    }

    @Override
    public void validateData(VectorSchemaRoot vectorSchemaRoot) {
      VarCharVector dictStringVector =
          (VarCharVector) vectorSchemaRoot.getVector(dictStringField.getName());
      IntVector dictIntVector = (IntVector) vectorSchemaRoot.getVector(dictIntField.getName());

      int rowCount = vectorSchemaRoot.getRowCount();

      for (int i = 0; i < rowCount; i++) {
        // Validate dictionary string
        if (i % 2 == 0) {
          assertTrue(dictStringVector.isNull(i), "Dictionary string should be null at index " + i);
        } else {
          String expected = getDictString(i);
          byte[] actualBytes = dictStringVector.get(i);
          assertNotNull(actualBytes, "Dictionary string should not be null at index " + i);
          String actual = new String(actualBytes, StandardCharsets.UTF_8);
          assertEquals(expected, actual, "Dictionary string mismatch at index " + i);
        }

        // Validate dictionary int
        if (i % 2 == 0) {
          assertTrue(dictIntVector.isNull(i), "Dictionary int should be null at index " + i);
        } else {
          assertEquals(
              getDictInt(i), dictIntVector.get(i), "Dictionary int mismatch at index " + i);
        }
      }
    }
  }

  /** Test run-end encoded types */
  private class TestRunEndEncodedTypes implements DataTester {
    private final Field reeIntField;
    private final Field reeLongField;
    private final Schema schema;

    TestRunEndEncodedTypes() {
      reeIntField = newRunEndEncodedIntField();
      reeLongField = newRunEndEncodedLongField();
      schema = new Schema(Arrays.asList(reeIntField, reeLongField));
    }

    @Override
    public Schema getSchema() {
      return schema;
    }

    @Override
    public void writeData(VectorSchemaRoot vectorSchemaRoot, int batchSize) {
      RunEndEncodedVector reeIntVector =
          (RunEndEncodedVector) vectorSchemaRoot.getVector(reeIntField.getName());
      RunEndEncodedVector reeLongVector =
          (RunEndEncodedVector) vectorSchemaRoot.getVector(reeLongField.getName());

      // Set run-end encoded integers
      // Calculate required capacity: runs every 3 rows means we need (batchSize + 2) / 3 runs
      int reeIntCapacity = (batchSize + 2) / 3;
      reeIntVector.allocateNew();
      IntVector reeIntRunEnds = (IntVector) reeIntVector.getRunEndsVector();
      IntVector reeIntValues = (IntVector) reeIntVector.getValuesVector();
      reeIntRunEnds.allocateNew(reeIntCapacity);
      reeIntValues.allocateNew(reeIntCapacity);

      // Create runs with repeating values (every 3 rows have the same value)
      int runCount = 0;
      for (int i = 0; i < batchSize; i += 3) {
        int runEnd = Math.min(i + 3, batchSize);
        reeIntRunEnds.set(runCount, runEnd);
        reeIntValues.set(runCount, getSignedInt(i));
        runCount++;
      }
      reeIntRunEnds.setValueCount(runCount);
      reeIntValues.setValueCount(runCount);
      reeIntVector.setValueCount(batchSize);

      // Set run-end encoded longs
      // Calculate required capacity: runs every 5 rows means we need (batchSize + 4) / 5 runs
      int reeLongCapacity = (batchSize + 4) / 5;
      reeLongVector.allocateNew();
      IntVector reeLongRunEnds = (IntVector) reeLongVector.getRunEndsVector();
      BigIntVector reeLongValues = (BigIntVector) reeLongVector.getValuesVector();
      reeLongRunEnds.allocateNew(reeLongCapacity);
      reeLongValues.allocateNew(reeLongCapacity);

      // Create runs with repeating values (every 5 rows have the same value)
      runCount = 0;
      for (int i = 0; i < batchSize; i += 5) {
        int runEnd = Math.min(i + 5, batchSize);
        reeLongRunEnds.set(runCount, runEnd);
        reeLongValues.set(runCount, getSignedLong(i));
        runCount++;
      }
      reeLongRunEnds.setValueCount(runCount);
      reeLongValues.setValueCount(runCount);
      reeLongVector.setValueCount(batchSize);
    }

    @Override
    public void validateData(VectorSchemaRoot vectorSchemaRoot) {
      RunEndEncodedVector reeIntVector =
          (RunEndEncodedVector) vectorSchemaRoot.getVector(reeIntField.getName());
      RunEndEncodedVector reeLongVector =
          (RunEndEncodedVector) vectorSchemaRoot.getVector(reeLongField.getName());

      int rowCount = vectorSchemaRoot.getRowCount();

      for (int i = 0; i < rowCount; i++) {
        // Validate run-end encoded int
        Object reeIntValue = reeIntVector.getObject(i);
        assertNotNull(reeIntValue, "REE int should not be null at index " + i);
        int expectedInt = getSignedInt((i / 3) * 3);
        assertEquals(
            expectedInt, ((Integer) reeIntValue).intValue(), "REE int mismatch at index " + i);

        // Validate run-end encoded long
        Object reeLongValue = reeLongVector.getObject(i);
        assertNotNull(reeLongValue, "REE long should not be null at index " + i);
        long expectedLong = getSignedLong((i / 5) * 5);
        assertEquals(
            expectedLong, ((Long) reeLongValue).longValue(), "REE long mismatch at index " + i);
      }
    }

    private Field newRunEndEncodedIntField() {
      Field runEndField =
          new Field("run_ends", FieldType.notNullable(new ArrowType.Int(32, true)), null);
      Field valueField = new Field("values", FieldType.nullable(new ArrowType.Int(32, true)), null);
      return new Field(
          "ree-int",
          FieldType.nullable(new ArrowType.RunEndEncoded()),
          Arrays.asList(runEndField, valueField));
    }

    private Field newRunEndEncodedLongField() {
      Field runEndField =
          new Field("run_ends", FieldType.notNullable(new ArrowType.Int(32, true)), null);
      Field valueField = new Field("values", FieldType.nullable(new ArrowType.Int(64, true)), null);
      return new Field(
          "ree-long",
          FieldType.nullable(new ArrowType.RunEndEncoded()),
          Arrays.asList(runEndField, valueField));
    }
  }
}
