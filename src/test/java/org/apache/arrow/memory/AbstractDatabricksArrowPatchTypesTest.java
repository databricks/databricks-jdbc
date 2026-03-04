package org.apache.arrow.memory;

import com.databricks.jdbc.api.impl.arrow.ArrowBufferAllocator;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Collections;
import java.util.stream.Stream;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.provider.Arguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for Arrow data type read/write tests with the patched Arrow classes.
 *
 * <p>Provides shared infrastructure including parameter sources, core test utilities, and data
 * generation helpers.
 */
public abstract class AbstractDatabricksArrowPatchTypesTest {
  protected static final Logger logger =
      LoggerFactory.getLogger(AbstractDatabricksArrowPatchTypesTest.class);

  /** Provide different buffer allocators. */
  protected static Stream<Arguments> getBufferAllocators() {
    // Large enough value which fits within the heap space for tests.
    int totalRows = (int) Math.pow(2, 17); // A large enough value.
    return Stream.of(
        Arguments.of(new DatabricksBufferAllocator(), new DatabricksBufferAllocator(), totalRows));
  }

  /** Provide different buffer allocators with smaller row count. */
  protected static Stream<Arguments> getBufferAllocatorsSmallRows() {
    int totalRows = 100_000; // A large enough value.
    return Stream.of(
        Arguments.of(new DatabricksBufferAllocator(), new DatabricksBufferAllocator(), totalRows));
  }

  @BeforeAll
  public static void logDetails() {
    logger.info("Using allocator: {}", ArrowBufferAllocator.getBufferAllocator().getName());
  }

  protected byte[] writeData(DataTester dataTester, int totalRowCount, BufferAllocator allocator)
      throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(dataTester.getSchema(), allocator);
    ArrowStreamWriter streamWriter =
        new ArrowStreamWriter(vectorSchemaRoot, null, byteArrayOutputStream);

    streamWriter.start();
    for (int batchSize = 1; batchSize <= totalRowCount; batchSize *= 2) {
      dataTester.writeData(vectorSchemaRoot, batchSize);

      // Write batch.
      vectorSchemaRoot.setRowCount(batchSize);
      streamWriter.writeBatch();
      vectorSchemaRoot.clear();
    }

    streamWriter.end();
    streamWriter.close();
    vectorSchemaRoot.close();
    allocator.close();

    return byteArrayOutputStream.toByteArray();
  }

  protected void readAndValidate(DataTester dataTester, byte[] data, BufferAllocator allocator)
      throws IOException {
    try (ArrowStreamReader reader =
        new ArrowStreamReader(new ByteArrayInputStream(data), allocator)) {
      while (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        logger.info("Validating {} rows", root.getRowCount());
        dataTester.validateData(root);
      }
    } finally {
      allocator.close();
    }
  }

  /** Interface for test data writers and validators. */
  protected interface DataTester {
    Schema getSchema();

    void writeData(VectorSchemaRoot vectorSchemaRoot, int batchSize);

    void validateData(VectorSchemaRoot vectorSchemaRoot);
  }

  // ============================================================================================
  // Field creation helper methods
  // ============================================================================================

  protected Field newSignedByteIntField() {
    return new Field("signed-byte-int", FieldType.nullable(new ArrowType.Int(8, true)), null);
  }

  protected Field newSignedShortIntField() {
    return new Field("signed-short-int", FieldType.nullable(new ArrowType.Int(16, true)), null);
  }

  protected Field newSignedIntField() {
    return new Field("signed-int", FieldType.nullable(new ArrowType.Int(32, true)), null);
  }

  protected Field newSignedLongField() {
    return new Field("signed-long", FieldType.nullable(new ArrowType.Int(64, true)), null);
  }

  protected Field newUnsignedByteIntField() {
    return new Field("unsigned-byte-int", FieldType.nullable(new ArrowType.Int(8, false)), null);
  }

  protected Field newUnsignedShortIntField() {
    return new Field("unsigned-short-int", FieldType.nullable(new ArrowType.Int(16, false)), null);
  }

  protected Field newUnsignedIntField() {
    return new Field("unsigned-int", FieldType.nullable(new ArrowType.Int(32, false)), null);
  }

  protected Field newUnsignedLongField() {
    return new Field("unsigned-long", FieldType.nullable(new ArrowType.Int(64, false)), null);
  }

  protected Field newFloatField() {
    return new Field(
        "float",
        FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
        null);
  }

  protected Field newDoubleField() {
    return new Field(
        "double",
        FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
        null);
  }

  protected Field newDecimalField(int precision, int scale, int bitWidth) {
    String name = "decimal-" + precision + "-" + scale + "-" + bitWidth;
    return new Field(
        name, FieldType.nullable(new ArrowType.Decimal(precision, scale, bitWidth)), null);
  }

  protected Field newDateDayField() {
    return new Field("date-day", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null);
  }

  protected Field newTimestampMilliField() {
    return new Field(
        "timestamp-milli",
        FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")),
        null);
  }

  protected Field newTimestampMicroField() {
    return new Field(
        "timestamp-micro",
        FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")),
        null);
  }

  protected Field newTimeSecField() {
    return new Field("time-sec", FieldType.nullable(new ArrowType.Time(TimeUnit.SECOND, 32)), null);
  }

  protected Field newTimeNanoField() {
    return new Field(
        "time-nano", FieldType.nullable(new ArrowType.Time(TimeUnit.NANOSECOND, 64)), null);
  }

  protected Field newDurationMicrosecondField() {
    return new Field(
        "duration-micro-sec",
        FieldType.nullable(new ArrowType.Duration(TimeUnit.MICROSECOND)),
        null);
  }

  protected Field newIntervalYearField() {
    return new Field(
        "interval-year", FieldType.nullable(new ArrowType.Interval(IntervalUnit.YEAR_MONTH)), null);
  }

  protected Field newIntervalDayField() {
    return new Field(
        "interval-day", FieldType.nullable(new ArrowType.Interval(IntervalUnit.DAY_TIME)), null);
  }

  protected Field newIntervalMonthDayNanoField() {
    return new Field(
        "interval-month-day-nano",
        FieldType.nullable(new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO)),
        null);
  }

  protected Field newFixedSizeBinaryField() {
    return new Field(
        "fixed-size-binary", FieldType.nullable(new ArrowType.FixedSizeBinary(16)), null);
  }

  protected Field newVarBinaryField() {
    return new Field("variable-binary", FieldType.nullable(new ArrowType.Binary()), null);
  }

  protected Field newLargeVarBinaryField() {
    return new Field(
        "large-variable-binary", FieldType.nullable(new ArrowType.LargeBinary()), null);
  }

  protected Field newUtf8Field() {
    return new Field("utf8-string", FieldType.nullable(new ArrowType.Utf8()), null);
  }

  protected Field newLargeUtf8Field() {
    return new Field("large-utf8-string", FieldType.nullable(new ArrowType.LargeUtf8()), null);
  }

  protected Field newListIntField() {
    return new Field(
        "list-int",
        FieldType.nullable(ArrowType.List.INSTANCE),
        Collections.singletonList(
            new Field("$data$", FieldType.nullable(new ArrowType.Int(32, true)), null)));
  }

  protected Field newLargeListIntField() {
    return new Field(
        "large-list-int",
        FieldType.nullable(ArrowType.LargeList.INSTANCE),
        Collections.singletonList(
            new Field("$data$", FieldType.nullable(new ArrowType.Int(32, true)), null)));
  }

  protected Field newDictStringField() {
    return new Field("dict-string", FieldType.nullable(new ArrowType.Utf8()), null);
  }

  protected Field newDictIntField() {
    return new Field("dict-int", FieldType.nullable(new ArrowType.Int(32, true)), null);
  }

  // ============================================================================================
  // Data generation helper methods
  // ============================================================================================

  protected int getSignedByte(int index) {
    return (index % 256) - 128;
  }

  protected int getSignedShort(int index) {
    return (index % 65536) - 32768;
  }

  protected int getSignedInt(int index) {
    return index * (index % 3 == 0 ? -1 : 1);
  }

  protected long getSignedLong(int index) {
    return (long) index + Integer.MAX_VALUE * (index % 3 == 0 ? -1 : 1);
  }

  protected int getUnsignedByte(int index) {
    return index % 256;
  }

  protected int getUnsignedShort(int index) {
    return index % 65536;
  }

  protected int getUnsignedInt(int index) {
    return index;
  }

  protected long getUnsignedLong(int index) {
    return (long) index * 2;
  }

  protected float getFloat(int index) {
    return (float) index * (float) Math.PI * (index % 3 == 0 ? -1 : 1);
  }

  protected double getDouble(int index) {
    return index * Math.PI * (index % 3 == 0 ? -1 : 1);
  }

  protected BigDecimal getDecimal(int index, int scale) {
    BigDecimal bigDecimal = new BigDecimal(index % 100 * (index % 3 == 0 ? -1 : 1));
    return bigDecimal.setScale(scale, RoundingMode.HALF_DOWN);
  }

  protected int getDateDay(int index) {
    return 18000 + (index % 10000);
  }

  protected long getTimestampMilli(int index) {
    return 1577836800000L + ((long) index * 1000L);
  }

  protected long getTimestampMicro(int index) {
    return getTimestampMilli(index) * 1000L;
  }

  protected int getTimeSec(int index) {
    return index % 86400;
  }

  protected long getTimeNano(int index) {
    return (long) (index % 86400) * 1_000_000_000L;
  }

  protected long getDurationSec(int index) {
    return (long) index * 3600L;
  }

  protected long getDurationMicroseconds(int index) {
    return (long) index % 1000;
  }

  protected int getIntervalYearMonth(int index) {
    return index % 600;
  }

  protected int getIntervalDayDays(int index) {
    return index % 365;
  }

  protected int getIntervalDayMillis(int index) {
    return (index * 1000) % 86400000;
  }

  protected byte[] getFixedSizeBinary(int index, int size) {
    byte[] data = new byte[size];
    for (int i = 0; i < size; i++) {
      data[i] = (byte) ((index + i) % 256);
    }
    return data;
  }

  protected byte[] getVarBinary(int index, int maxLength) {
    int length = (index % maxLength) + 1;
    byte[] data = new byte[length];
    for (int i = 0; i < length; i++) {
      data[i] = (byte) ((index * 3 + i) % 256);
    }
    return data;
  }

  protected byte[] getLargeVarBinary(int index, int maxLength) {
    int length = (index % maxLength) + 1;
    byte[] data = new byte[length];
    for (int i = 0; i < length; i++) {
      data[i] = (byte) ((index * 7 + i) % 256);
    }
    return data;
  }

  protected String getUtf8String(int index) {
    return "UTF8-String-" + index + "-Data";
  }

  protected String getLargeUtf8String(int index) {
    return "LargeUTF8-String-" + index + "-DataWithMoreContent-" + (index * 3);
  }

  protected int getListSize(int index) {
    return index % 32 + 1;
  }

  protected int getListElement(int rowIndex, int elementIndex) {
    return rowIndex * 100 + elementIndex;
  }

  protected int getLargeListSize(int index) {
    return (index % 128) + 1;
  }

  protected int getLargeListElement(int rowIndex, int elementIndex) {
    return rowIndex * 1000 + elementIndex;
  }

  protected String getDictString(int index) {
    // Return repeating values to simulate dictionary-encoded data
    String[] dictValues = {"Red", "Green", "Blue", "Yellow", "Orange"};
    return dictValues[index % dictValues.length];
  }

  protected int getDictInt(int index) {
    // Return repeating values to simulate dictionary-encoded data
    return (index % 10) * 100;
  }
}
