package com.databricks.jdbc.api.impl.converters;

import static com.databricks.jdbc.common.util.DatabricksTypeUtil.VARIANT;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.impl.DatabricksArray;
import com.databricks.jdbc.api.impl.DatabricksStruct;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
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
    Object unconvertedObject = null;
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(unconvertedObject, ColumnInfoTypeName.BYTE, "BYTE");
    assertNull(convertedObject);
  }

  @Test
  public void testByteConversion() throws SQLException {
    TinyIntVector tinyIntVector = new TinyIntVector("tinyIntVector", this.bufferAllocator);
    tinyIntVector.allocateNew(1);
    tinyIntVector.set(0, 65);
    Object unconvertedObject = tinyIntVector.getObject(0);
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(unconvertedObject, ColumnInfoTypeName.BYTE, "BYTE");

    assertInstanceOf(Byte.class, convertedObject);
    assertEquals(convertedObject, (byte) 65);
  }

  @Test
  public void testVariantConversion() throws SQLException, JsonProcessingException {
    Object nullObject = ArrowToJavaObjectConverter.convert(null, null, VARIANT);
    assertNull(nullObject);

    Object intObject = ArrowToJavaObjectConverter.convert(1, null, VARIANT);
    assertNotNull(intObject);
    assertInstanceOf(String.class, intObject, "Expected result to be a String");
    assertEquals("1", intObject, "The integer should be converted to a string.");

    Map map = new HashMap();
    map.put("key", "value");
    Object mapObject = ArrowToJavaObjectConverter.convert(map, null, VARIANT);
    assertNotNull(mapObject);
    assertInstanceOf(String.class, mapObject, "Expected result to be a String");
    assertEquals(mapObject.toString(), mapObject, "The map should be converted to a JSON string.");
  }

  @Test
  public void testShortConversion() throws SQLException {
    SmallIntVector smallIntVector = new SmallIntVector("smallIntVector", this.bufferAllocator);
    smallIntVector.allocateNew(1);
    smallIntVector.set(0, 4);
    Object unconvertedObject = smallIntVector.getObject(0);
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(unconvertedObject, ColumnInfoTypeName.SHORT, "SHORT");

    assertInstanceOf(Short.class, convertedObject);
    assertEquals(convertedObject, (short) 4);
  }

  @Test
  public void testIntConversion() throws SQLException {
    IntVector intVector = new IntVector("intVector", this.bufferAllocator);
    intVector.allocateNew(1);
    intVector.set(0, 1111111111);
    Object unconvertedObject = intVector.getObject(0);
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(unconvertedObject, ColumnInfoTypeName.INT, "INT");

    assertInstanceOf(Integer.class, convertedObject);
    assertEquals(convertedObject, 1111111111);
  }

  @Test
  public void testLongConversion() throws SQLException {
    BigIntVector bigIntVector = new BigIntVector("bigIntVector", this.bufferAllocator);
    bigIntVector.allocateNew(1);
    bigIntVector.set(0, 1111111111111111111L);
    Object unconvertedObject = bigIntVector.getObject(0);
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(unconvertedObject, ColumnInfoTypeName.LONG, "LONG");

    assertInstanceOf(Long.class, convertedObject);
    assertEquals(convertedObject, 1111111111111111111L);
  }

  @Test
  public void testFloatConversion() throws SQLException {
    Float4Vector float4Vector = new Float4Vector("float4Vector", this.bufferAllocator);
    float4Vector.allocateNew(1);
    float4Vector.set(0, 4.2f);
    Object unconvertedObject = float4Vector.getObject(0);
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(unconvertedObject, ColumnInfoTypeName.FLOAT, "FLOAT");

    assertInstanceOf(Float.class, convertedObject);
    assertEquals(convertedObject, 4.2f);
  }

  @Test
  public void testDoubleConversion() throws SQLException {
    Float8Vector float8Vector = new Float8Vector("float8Vector", this.bufferAllocator);
    float8Vector.allocateNew(1);
    float8Vector.set(0, 4.11111111);
    Object unconvertedObject = float8Vector.getObject(0);
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(unconvertedObject, ColumnInfoTypeName.DOUBLE, "DOUBLE");

    assertInstanceOf(Double.class, convertedObject);
    assertEquals(convertedObject, 4.11111111);
  }

  @Test
  public void testBigDecimalConversion() throws SQLException {
    DecimalVector decimalVector = new DecimalVector("decimalVector", this.bufferAllocator, 30, 10);
    decimalVector.allocateNew(1);
    decimalVector.set(0, BigDecimal.valueOf(4.1111111111));
    Object unconvertedObject = decimalVector.getObject(0);
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(
            unconvertedObject, ColumnInfoTypeName.DECIMAL, "DECIMAL(30,10)");

    assertInstanceOf(BigDecimal.class, convertedObject);
    assertEquals(convertedObject, BigDecimal.valueOf(4.1111111111));
  }

  @Test
  public void testByteArrayConversion() throws SQLException {
    VarBinaryVector varBinaryVector = new VarBinaryVector("varBinaryVector", this.bufferAllocator);
    varBinaryVector.allocateNew(1);
    varBinaryVector.set(0, new byte[] {65, 66, 67});
    Object unconvertedObject = varBinaryVector.getObject(0);
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(unconvertedObject, ColumnInfoTypeName.BINARY, "BINARY");

    assertInstanceOf(byte[].class, convertedObject);
    assertArrayEquals((byte[]) convertedObject, "ABC".getBytes());
  }

  @Test
  public void testBooleanConversion() throws SQLException {
    BitVector bitVector = new BitVector("bitVector", this.bufferAllocator);
    bitVector.allocateNew(2);
    bitVector.set(0, 0);
    bitVector.set(1, 1);
    Object unconvertedFalseObject = bitVector.getObject(0);
    Object convertedFalseObject =
        ArrowToJavaObjectConverter.convert(
            unconvertedFalseObject, ColumnInfoTypeName.BOOLEAN, "BOOLEAN");
    Object unconvertedTrueObject = bitVector.getObject(1);
    Object convertedTrueObject =
        ArrowToJavaObjectConverter.convert(
            unconvertedTrueObject, ColumnInfoTypeName.BOOLEAN, "BOOLEAN");

    assertInstanceOf(Boolean.class, unconvertedTrueObject);
    assertInstanceOf(Boolean.class, unconvertedFalseObject);
    assertEquals(convertedFalseObject, false);
    assertEquals(convertedTrueObject, true);
  }

  @Test
  public void testCharConversion() throws SQLException {
    VarCharVector varCharVector = new VarCharVector("varCharVector", this.bufferAllocator);
    varCharVector.allocateNew(1);
    varCharVector.set(0, new byte[] {65});
    Object unconvertedObject = varCharVector.getObject(0);
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(unconvertedObject, ColumnInfoTypeName.CHAR, "CHAR");

    assertInstanceOf(Character.class, convertedObject);
    assertEquals(convertedObject, 'A');
  }

  @Test
  public void testStringConversion() throws SQLException {
    VarCharVector varCharVector = new VarCharVector("varCharVector", this.bufferAllocator);
    varCharVector.allocateNew(1);
    varCharVector.set(0, new byte[] {65, 66, 67});
    Object unconvertedObject = varCharVector.getObject(0);
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(unconvertedObject, ColumnInfoTypeName.STRING, "STRING");

    assertInstanceOf(String.class, convertedObject);
    assertEquals(convertedObject, "ABC");
  }

  @Test
  public void testDateConversion() throws SQLException {
    DateDayVector dateDayVector = new DateDayVector("dateDayVector", this.bufferAllocator);
    dateDayVector.allocateNew(1);
    dateDayVector.set(0, 19598); // 29th August 2023
    Object unconvertedObject = dateDayVector.getObject(0);
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(unconvertedObject, ColumnInfoTypeName.DATE, "DATE");

    assertInstanceOf(Date.class, convertedObject);
    assertEquals(convertedObject, Date.valueOf("2023-08-29"));
  }

  @Test
  public void testTimestampConversion() throws SQLException {
    IntVector intVector = new IntVector("intVector", this.bufferAllocator);
    intVector.allocateNew(1);
    intVector.set(0, 4000);
    Object unconvertedIntObject = intVector.getObject(0);
    Object convertedFromIntObject =
        ArrowToJavaObjectConverter.convert(
            unconvertedIntObject, ColumnInfoTypeName.TIMESTAMP, "TIMESTAMP");
    BigIntVector bigIntVector = new BigIntVector("bigIntVector", this.bufferAllocator);
    bigIntVector.allocateNew(1);
    bigIntVector.set(0, 1693312639000000L);
    Object unconvertedBigIntObject = bigIntVector.getObject(0);
    Object convertedFromBigIntObject =
        ArrowToJavaObjectConverter.convert(
            unconvertedBigIntObject, ColumnInfoTypeName.TIMESTAMP, "TIMESTAMP");

    assertInstanceOf(Timestamp.class, convertedFromIntObject);
    assertEquals(((Timestamp) convertedFromIntObject).toInstant(), Instant.ofEpochMilli(4));
    assertInstanceOf(Timestamp.class, convertedFromBigIntObject);
    assertEquals(
        ((Timestamp) convertedFromBigIntObject).toInstant(), Instant.ofEpochMilli(1693312639000L));
  }

  @Test
  public void testStructConversion() throws SQLException {
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(
            "{\"k\": 10}", ColumnInfoTypeName.STRUCT, "STRUCT<key: STRING, value: INT>");
    assertInstanceOf(DatabricksStruct.class, convertedObject);
  }

  @Test
  public void testArrayConversion() throws SQLException {
    ArrayList<String> list = new ArrayList<>();
    list.add("A");
    list.add("B");
    VarCharVector varCharVector = new VarCharVector("varCharVector", this.bufferAllocator);
    varCharVector.allocateNew(1);
    varCharVector.set(0, list.toString().getBytes());
    Object unconvertedObject = varCharVector.getObject(0);
    Object convertedObject =
        ArrowToJavaObjectConverter.convert(
            "[\"A\", \"B\"]", ColumnInfoTypeName.STRING, "ARRAY<STRING>");
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
}
