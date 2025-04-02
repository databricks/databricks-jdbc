package com.databricks.jdbc.api.impl.converters;

import static com.databricks.jdbc.api.impl.converters.ArrowToJavaObjectConverter.getZoneIdFromTimeZoneOpt;
import static com.databricks.jdbc.common.util.DatabricksTypeUtil.VARIANT;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.impl.DatabricksArray;
import com.databricks.jdbc.api.impl.DatabricksStruct;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ArrowToJavaObjectConverterTest {
  @Mock IDatabricksConnectionContext connectionContext;
  private final BufferAllocator bufferAllocator;

  ArrowToJavaObjectConverterTest() {
    this.bufferAllocator = new RootAllocator();
  }

  @Test
  public void testNullObjectConversion() throws SQLException {
    TinyIntVector tinyIntVector = new TinyIntVector("tinyIntVector", this.bufferAllocator);
    tinyIntVector.allocateNew(1);
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(tinyIntVector, 0, ColumnInfoTypeName.BYTE, "BYTE");
    assertNull(convertedObject);
  }

  @Test
  public void testByteConversion() throws SQLException {
    TinyIntVector tinyIntVector = new TinyIntVector("tinyIntVector", this.bufferAllocator);
    tinyIntVector.allocateNew(1);
    tinyIntVector.set(0, 65);
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(tinyIntVector, 0, ColumnInfoTypeName.BYTE, "BYTE");

    assertInstanceOf(Byte.class, convertedObject);
    assertEquals((byte) 65, convertedObject);
  }

  @Test
  public void testVariantConversion() throws SQLException, JsonProcessingException {
    VarCharVector varCharVector = new VarCharVector("varCharVector", this.bufferAllocator);
    varCharVector.allocateNew(3);

    // Test null
    Object nullObject = ArrowToJavaObjectConverter.convert(varCharVector, 0, null, VARIANT);
    assertNull(nullObject);

    // Test integer
    varCharVector.set(1, "1".getBytes());
    Object intObject = ArrowToJavaObjectConverter.convert(varCharVector, 1, null, VARIANT);
    assertNotNull(intObject);
    assertInstanceOf(String.class, intObject, "Expected result to be a String");
    assertEquals("1", intObject, "The integer should be converted to a string.");

    // Test map
    Map<String, String> map = new HashMap<>();
    map.put("key", "value");
    varCharVector.set(2, map.toString().getBytes());
    Object mapObject = ArrowToJavaObjectConverter.convert(varCharVector, 2, null, VARIANT);
    assertNotNull(mapObject);
    assertInstanceOf(String.class, mapObject, "Expected result to be a String");
    assertEquals(mapObject.toString(), mapObject, "The map should be converted to a JSON string.");
  }

  @Test
  public void testShortConversion() throws SQLException {
    SmallIntVector smallIntVector = new SmallIntVector("smallIntVector", this.bufferAllocator);
    smallIntVector.allocateNew(1);
    smallIntVector.set(0, 4);
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(smallIntVector, 0, ColumnInfoTypeName.SHORT, "SHORT");

    assertInstanceOf(Short.class, convertedObject);
    assertEquals((short) 4, convertedObject);
  }

  @Test
  public void testTimestampNTZConversion() throws SQLException {
    long timestamp = 1704054600000000L;

    TimeStampMicroVector timestampMicroVector =
        new TimeStampMicroVector("timestampMicroVector", this.bufferAllocator);
    timestampMicroVector.allocateNew(1);
    timestampMicroVector.set(0, timestamp);
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(
            timestampMicroVector, 0, ColumnInfoTypeName.TIMESTAMP, "TIMESTAMP_NTZ");

    assertInstanceOf(Timestamp.class, convertedObject);
    assertEquals(getTimestampAdjustedToTimeZone(timestamp, "UTC"), convertedObject);
  }

  @Test
  public void testIntConversion() throws SQLException {
    IntVector intVector = new IntVector("intVector", this.bufferAllocator);
    intVector.allocateNew(1);
    intVector.set(0, 1111111111);
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(intVector, 0, ColumnInfoTypeName.INT, "INT");

    assertInstanceOf(Integer.class, convertedObject);
    assertEquals(1111111111, convertedObject);
  }

  @Test
  public void testLongConversion() throws SQLException {
    BigIntVector bigIntVector = new BigIntVector("bigIntVector", this.bufferAllocator);
    bigIntVector.allocateNew(1);
    bigIntVector.set(0, 1111111111111111111L);
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(bigIntVector, 0, ColumnInfoTypeName.LONG, "LONG");

    assertInstanceOf(Long.class, convertedObject);
    assertEquals(1111111111111111111L, convertedObject);
  }

  @Test
  public void testFloatConversion() throws SQLException {
    Float4Vector float4Vector = new Float4Vector("float4Vector", this.bufferAllocator);
    float4Vector.allocateNew(1);
    float4Vector.set(0, 4.2f);
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(float4Vector, 0, ColumnInfoTypeName.FLOAT, "FLOAT");

    assertInstanceOf(Float.class, convertedObject);
    assertEquals(4.2f, convertedObject);
  }

  @Test
  public void testDoubleConversion() throws SQLException {
    Float8Vector float8Vector = new Float8Vector("float8Vector", this.bufferAllocator);
    float8Vector.allocateNew(1);
    float8Vector.set(0, 4.11111111);
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(float8Vector, 0, ColumnInfoTypeName.DOUBLE, "DOUBLE");

    assertInstanceOf(Double.class, convertedObject);
    assertEquals(4.11111111, convertedObject);
  }

  @Test
  public void testBigDecimalConversion() throws SQLException {
    DecimalVector decimalVector = new DecimalVector("decimalVector", this.bufferAllocator, 30, 10);
    decimalVector.allocateNew(1);
    decimalVector.set(0, BigDecimal.valueOf(4.1111111111));
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(
            decimalVector, 0, ColumnInfoTypeName.DECIMAL, "DECIMAL(30,10)");

    assertInstanceOf(BigDecimal.class, convertedObject);
    assertEquals(BigDecimal.valueOf(4.1111111111), convertedObject);
  }

  @Test
  public void testByteArrayConversion() throws SQLException {
    VarBinaryVector varBinaryVector = new VarBinaryVector("varBinaryVector", this.bufferAllocator);
    varBinaryVector.allocateNew(1);
    varBinaryVector.set(0, new byte[] {65, 66, 67});
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(varBinaryVector, 0, ColumnInfoTypeName.BINARY, "BINARY");

    assertInstanceOf(byte[].class, convertedObject);
    assertArrayEquals("ABC".getBytes(), (byte[]) convertedObject);
  }

  @Test
  public void testBooleanConversion() throws SQLException {
    BitVector bitVector = new BitVector("bitVector", this.bufferAllocator);
    bitVector.allocateNew(2);
    bitVector.set(0, 0);
    bitVector.set(1, 1);
    Object convertedFalseObject =
        ArrowToJavaObjectConverter.convert(bitVector, 0, ColumnInfoTypeName.BOOLEAN, "BOOLEAN");
    Object convertedTrueObject =
        ArrowToJavaObjectConverter.convert(bitVector, 1, ColumnInfoTypeName.BOOLEAN, "BOOLEAN");

    assertInstanceOf(Boolean.class, convertedTrueObject);
    assertInstanceOf(Boolean.class, convertedFalseObject);
    assertEquals(false, convertedFalseObject);
    assertEquals(true, convertedTrueObject);
  }

  @Test
  public void testCharConversion() throws SQLException {
    VarCharVector varCharVector = new VarCharVector("varCharVector", this.bufferAllocator);
    varCharVector.allocateNew(1);
    varCharVector.set(0, new byte[] {65});
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(varCharVector, 0, ColumnInfoTypeName.CHAR, "CHAR");

    assertInstanceOf(Character.class, convertedObject);
    assertEquals('A', convertedObject);
  }

  @Test
  public void testStringConversion() throws SQLException {
    VarCharVector varCharVector = new VarCharVector("varCharVector", this.bufferAllocator);
    varCharVector.allocateNew(1);
    varCharVector.set(0, new byte[] {65, 66, 67});
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(varCharVector, 0, ColumnInfoTypeName.STRING, "STRING");

    assertInstanceOf(String.class, convertedObject);
    assertEquals("ABC", convertedObject);
  }

  @Test
  public void testDateConversion() throws SQLException {
    DateDayVector dateDayVector = new DateDayVector("dateDayVector", this.bufferAllocator);
    dateDayVector.allocateNew(1);
    dateDayVector.set(0, 19598); // 29th August 2023
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(dateDayVector, 0, ColumnInfoTypeName.DATE, "DATE");

    assertInstanceOf(Date.class, convertedObject);
    assertEquals(Date.valueOf("2023-08-29"), convertedObject);
  }

  @Test
  public void testTimestampConversion() throws SQLException {
    long timestamp = 1704054600000000L;
    String timeZone = "Asia/Tokyo";
    TimeStampMicroTZVector timeStampMicroTZVector =
        new TimeStampMicroTZVector("timeStampMicroTzVector", this.bufferAllocator, timeZone);
    timeStampMicroTZVector.allocateNew(1);
    timeStampMicroTZVector.set(0, timestamp);
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(
            timeStampMicroTZVector, 0, ColumnInfoTypeName.TIMESTAMP, "TIMESTAMP");

    assertInstanceOf(Timestamp.class, convertedObject);
    assertEquals(getTimestampAdjustedToTimeZone(timestamp, timeZone), convertedObject);
  }

  private static Timestamp getTimestampAdjustedToTimeZone(long timestampMicro, String timeZone) {
    Instant instant = Instant.ofEpochMilli(timestampMicro / 1000);
    LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.of(timeZone));
    return Timestamp.valueOf(localDateTime);
  }

  @Test
  public void testStructConversion() throws SQLException {
    VarCharVector varCharVector = new VarCharVector("varCharVector", this.bufferAllocator);
    varCharVector.allocateNew(1);
    varCharVector.set(0, "{\"k\": 10}".getBytes());
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(
            varCharVector, 0, ColumnInfoTypeName.STRUCT, "STRUCT<key: STRING, value: INT>");
    assertInstanceOf(DatabricksStruct.class, convertedObject);
  }

  @Test
  public void testArrayConversion() throws SQLException {
    VarCharVector varCharVector = new VarCharVector("varCharVector", this.bufferAllocator);
    varCharVector.allocateNew(1);
    varCharVector.set(0, "[\"A\", \"B\"]".getBytes());
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(
            varCharVector, 0, ColumnInfoTypeName.STRING, "ARRAY<STRING>");
    assertInstanceOf(DatabricksArray.class, convertedObject);
  }

  @Test
  public void testConvertToDecimal() throws DatabricksValidationException {
    // Test with Text object
    Text textObject = new Text("123.456");
    String arrowMetadata = "DECIMAL(10,3)";
    BigDecimal result = ArrowToJavaObjectConverter.convertToDecimal(textObject, arrowMetadata);
    assertEquals(new BigDecimal("123.456"), result);

    // Test with Number object and valid metadata
    Double numberObject = 123.456;
    arrowMetadata = "DECIMAL(10,2)";
    result = ArrowToJavaObjectConverter.convertToDecimal(numberObject, arrowMetadata);
    assertEquals(new BigDecimal("123.46"), result); // Rounded to 2 decimal places

    numberObject = 123.45;
    result = ArrowToJavaObjectConverter.convertToDecimal(numberObject, arrowMetadata);
    assertEquals(new BigDecimal("123.45"), result); // No rounding

    // Test with Number object and invalid metadata
    arrowMetadata = "DECIMAL(10,invalid)";
    result = ArrowToJavaObjectConverter.convertToDecimal(numberObject, arrowMetadata);
    assertEquals(new BigDecimal("123"), result); // Default scale to 0

    // Test with unsupported object type
    assertThrows(
        DatabricksValidationException.class,
        () -> {
          ArrowToJavaObjectConverter.convertToDecimal(new Object(), "DECIMAL(10,2)");
        });

    // Test with rounding
    numberObject = 123.456789;
    arrowMetadata = "DECIMAL(10,4)";
    result = ArrowToJavaObjectConverter.convertToDecimal(numberObject, arrowMetadata);
    assertEquals(new BigDecimal("123.4568"), result); // Rounded to 4 decimal places
  }

  @Test
  public void testGetZoneIdFromTimeZoneOpt_StandardTimeZones() {
    assertEquals(
        ZoneId.of("America/New_York"), getZoneIdFromTimeZoneOpt(Optional.of("America/New_York")));

    assertEquals(
        ZoneId.of("Europe/London"), getZoneIdFromTimeZoneOpt(Optional.of("Europe/London")));

    assertEquals(ZoneId.of("Asia/Kolkata"), getZoneIdFromTimeZoneOpt(Optional.of("Asia/Kolkata")));

    assertEquals(
        ZoneId.of("Australia/Sydney"), getZoneIdFromTimeZoneOpt(Optional.of("Australia/Sydney")));
  }

  @Test
  public void testGetZoneIdFromTimeZoneOpt_PositiveOffsets() {
    ZoneId expected = ZoneOffset.ofHoursMinutes(4, 30);
    assertEquals(expected, getZoneIdFromTimeZoneOpt(Optional.of("+4:30")));

    expected = ZoneOffset.ofHoursMinutes(1, 0);
    assertEquals(expected, getZoneIdFromTimeZoneOpt(Optional.of("+1:00")));

    expected = ZoneOffset.ofHoursMinutes(5, 45);
    assertEquals(expected, getZoneIdFromTimeZoneOpt(Optional.of("+5:45")));

    expected = ZoneOffset.ofHoursMinutes(12, 0);
    assertEquals(expected, getZoneIdFromTimeZoneOpt(Optional.of("+12:00")));
  }

  @Test
  public void testGetZoneIdFromTimeZoneOpt_NegativeOffsets() {
    ZoneId expected = ZoneOffset.ofHoursMinutes(-3, 0);
    assertEquals(expected, getZoneIdFromTimeZoneOpt(Optional.of("-3:00")));

    expected = ZoneOffset.ofHoursMinutes(-9, -30);
    assertEquals(expected, getZoneIdFromTimeZoneOpt(Optional.of("-9:30")));

    expected = ZoneOffset.ofHoursMinutes(-11, -45);
    assertEquals(expected, getZoneIdFromTimeZoneOpt(Optional.of("-11:45")));
  }

  @Test
  public void testGetZoneIdFromTimeZoneOpt_EmptyOptional() {
    assertEquals(ZoneId.systemDefault(), getZoneIdFromTimeZoneOpt(Optional.empty()));
  }

  @Test
  public void testGetZoneIdFromTimeZoneOpt_InvalidTimeZones() {
    assertThrows(
        DateTimeException.class, () -> getZoneIdFromTimeZoneOpt(Optional.of("Invalid/TimeZone")));

    assertThrows(
        DateTimeException.class,
        () -> getZoneIdFromTimeZoneOpt(Optional.of("+25:00"))); // Hours out of range
    assertThrows(
        DateTimeException.class,
        () -> getZoneIdFromTimeZoneOpt(Optional.of("+12:60"))); // Minutes out of range
    assertThrows(
        DateTimeException.class,
        () -> getZoneIdFromTimeZoneOpt(Optional.of("5:30"))); // Missing sign
  }
}
