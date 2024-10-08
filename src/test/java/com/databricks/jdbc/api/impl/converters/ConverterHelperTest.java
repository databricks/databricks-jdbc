package com.databricks.jdbc.api.impl.converters;

import static com.databricks.jdbc.TestConstants.TEST_BYTES;
import static com.databricks.jdbc.api.impl.converters.ConverterHelper.convertSqlTypeToSpecificJavaType;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.Year;
import java.util.Calendar;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ConverterHelperTest {
  ConverterHelper converterHelper;

  private static Stream<Arguments> provideParametersForGetConvertedObject() {
    return Stream.of(
        Arguments.of(Types.TINYINT, 127, (byte) 127),
        Arguments.of(Types.SMALLINT, 32767, (short) 32767),
        Arguments.of(Types.INTEGER, 123456, 123456),
        Arguments.of(Types.BIGINT, 123456789012345L, 123456789012345L),
        Arguments.of(Types.FLOAT, 1.23f, 1.23f),
        Arguments.of(Types.DOUBLE, 1.234567, 1.234567),
        Arguments.of(Types.DECIMAL, new BigDecimal("123.45"), new BigDecimal("123.45")),
        Arguments.of(Types.BOOLEAN, true, true),
        Arguments.of(Types.DATE, Date.valueOf("2024-01-01"), Date.valueOf("2024-01-01")),
        Arguments.of(
            Types.TIMESTAMP,
            Timestamp.valueOf("2024-01-01 01:01:01"),
            Timestamp.valueOf("2024-01-01 01:01:01")),
        Arguments.of(Types.BINARY, TEST_BYTES, TEST_BYTES),
        Arguments.of(Types.VARCHAR, "test", "test"),
        Arguments.of(Types.CHAR, 'c', "c"));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForGetConvertedObject")
  public void testGetConvertedObject(int columnType, Object input, Object expected)
      throws DatabricksSQLException {
    assertEquals(expected, ConverterHelper.convertSqlTypeToJavaType(columnType, input));
  }

  @Test
  void testConvertToString() throws DatabricksSQLException {
    assertEquals(
        "Test String",
        convertSqlTypeToSpecificJavaType(String.class, Types.VARCHAR, "Test String"));
  }

  @Test
  void testConvertToBigDecimal() throws DatabricksSQLException {
    BigDecimal expected = new BigDecimal("123.456");
    assertEquals(
        expected, convertSqlTypeToSpecificJavaType(BigDecimal.class, Types.DECIMAL, "123.456"));
  }

  @Test
  void testConvertToBoolean() throws DatabricksSQLException {
    assertEquals(true, convertSqlTypeToSpecificJavaType(Boolean.class, Types.BOOLEAN, true));
    assertEquals(true, convertSqlTypeToSpecificJavaType(boolean.class, Types.BOOLEAN, true));
  }

  @Test
  void testConvertToInt() throws DatabricksSQLException {
    assertEquals(123, convertSqlTypeToSpecificJavaType(Integer.class, Types.INTEGER, "123"));
    assertEquals(123, convertSqlTypeToSpecificJavaType(int.class, Types.INTEGER, "123"));
  }

  @Test
  void testConvertToLong() throws DatabricksSQLException {
    assertEquals(123L, convertSqlTypeToSpecificJavaType(Long.class, Types.BIGINT, "123"));
    assertEquals(123L, convertSqlTypeToSpecificJavaType(long.class, Types.BIGINT, "123"));
  }

  @Test
  void testConvertToFloat() throws DatabricksSQLException {
    assertEquals(1.23f, convertSqlTypeToSpecificJavaType(Float.class, Types.FLOAT, "1.23"));
    assertEquals(1.23f, convertSqlTypeToSpecificJavaType(float.class, Types.FLOAT, "1.23"));
  }

  @Test
  void testConvertToDouble() throws DatabricksSQLException {
    assertEquals(1.234, convertSqlTypeToSpecificJavaType(Double.class, Types.DOUBLE, "1.234"));
    assertEquals(1.234, convertSqlTypeToSpecificJavaType(double.class, Types.DOUBLE, 1.234));
  }

  @Test
  void testConvertToDate() throws DatabricksSQLException {
    Date current = new Date(System.currentTimeMillis());
    assertEquals(current, convertSqlTypeToSpecificJavaType(Date.class, Types.DATE, current));
  }

  @Test
  void testConvertToLocalDate() throws DatabricksSQLException {
    LocalDate current = LocalDate.now();
    assertEquals(
        current.toString(),
        convertSqlTypeToSpecificJavaType(LocalDate.class, Types.DATE, current.toString())
            .toString());
  }

  @Test
  void testConvertToBigInteger() throws DatabricksSQLException {
    BigInteger expected = BigInteger.ONE;
    assertEquals(expected, convertSqlTypeToSpecificJavaType(BigInteger.class, Types.BIGINT, "1"));
  }

  @Test
  void testConvertToTimestamp() throws DatabricksSQLException {
    Timestamp current = new Timestamp(System.currentTimeMillis());
    assertEquals(
        current, convertSqlTypeToSpecificJavaType(Timestamp.class, Types.TIMESTAMP, current));
    assertEquals(
        current, convertSqlTypeToSpecificJavaType(Calendar.class, Types.TIMESTAMP, current));
  }

  @Test
  void testConvertToShort() throws DatabricksSQLException {
    assertEquals((byte) 123, convertSqlTypeToSpecificJavaType(Byte.class, Types.TINYINT, "123"));
    assertEquals((byte) 123, convertSqlTypeToSpecificJavaType(byte.class, Types.TINYINT, "123"));
  }

  @Test
  void testConvertToOther() throws DatabricksSQLException {
    assertEquals(
        "otherString", convertSqlTypeToSpecificJavaType(Year.class, Types.VARCHAR, "otherString"));
  }

  @Test
  void getObjectConverterForInt() {
    assertInstanceOf(IntConverter.class, converterHelper.getConverterForSqlType(Types.INTEGER));
  }

  @Test
  void getObjectConverterForString() {
    assertInstanceOf(StringConverter.class, converterHelper.getConverterForSqlType(Types.VARCHAR));
  }

  @Test
  void getObjectConverterForBigDecimal() {
    assertInstanceOf(
        BigDecimalConverter.class, converterHelper.getConverterForSqlType(Types.DECIMAL));
  }

  @Test
  void getObjectConverterForBoolean() {
    assertInstanceOf(BooleanConverter.class, converterHelper.getConverterForSqlType(Types.BOOLEAN));
  }

  @Test
  void getObjectConverterForDate() {
    assertInstanceOf(DateConverter.class, converterHelper.getConverterForSqlType(Types.DATE));
  }

  @Test
  void getObjectConverterForTimestamp() {
    assertInstanceOf(
        TimestampConverter.class, converterHelper.getConverterForSqlType(Types.TIMESTAMP));
  }

  @Test
  void whenColumnTypeIsFloat_thenGetFloatConverter() {
    ObjectConverter converter = converterHelper.getConverterForSqlType(Types.FLOAT);
    assertInstanceOf(FloatConverter.class, converter);
  }

  @Test
  void whenColumnTypeIsDouble_thenGetDoubleConverter() {
    ObjectConverter converter = converterHelper.getConverterForSqlType(Types.DOUBLE);
    assertInstanceOf(DoubleConverter.class, converter);
  }

  @Test
  void whenColumnTypeIsDecimal_thenGetBigDecimalConverter() {
    ObjectConverter converter = converterHelper.getConverterForSqlType(Types.DECIMAL);
    assertInstanceOf(BigDecimalConverter.class, converter);
  }

  @Test
  void whenColumnType_Other() {
    ObjectConverter converter = converterHelper.getConverterForSqlType(Types.DECIMAL);
    assertInstanceOf(BigDecimalConverter.class, converter);
  }
}
