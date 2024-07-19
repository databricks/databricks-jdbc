package com.databricks.jdbc.core.converters;

import static com.databricks.jdbc.TestConstants.TEST_BYTES;
import static com.databricks.jdbc.core.converters.ConverterHelper.getConvertedObject;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.core.DatabricksSQLException;
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
        Arguments.of(Types.BIGINT, 123456789012345L, BigInteger.valueOf(123456789012345L)),
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
    assertEquals(expected, getConvertedObject(columnType, input));
  }

  @Test
  void testConvertToString() throws DatabricksSQLException {
    AbstractObjectConverter converter = mock(AbstractObjectConverter.class);
    when(converter.convertToString()).thenReturn("Test String");
    assertEquals("Test String", getConvertedObject(String.class, converter));
  }

  @Test
  void testConvertToBigDecimal() throws DatabricksSQLException {
    AbstractObjectConverter converter = mock(AbstractObjectConverter.class);
    BigDecimal expected = new BigDecimal("123.456");
    when(converter.convertToBigDecimal()).thenReturn(expected);
    assertEquals(expected, getConvertedObject(BigDecimal.class, converter));
  }

  @Test
  void testConvertToBoolean() throws DatabricksSQLException {
    AbstractObjectConverter converter = mock(AbstractObjectConverter.class);
    when(converter.convertToBoolean()).thenReturn(true);
    assertEquals(true, getConvertedObject(Boolean.class, converter));
    assertEquals(true, getConvertedObject(boolean.class, converter));
  }

  @Test
  void testConvertToInt() throws DatabricksSQLException {
    AbstractObjectConverter converter = mock(AbstractObjectConverter.class);
    when(converter.convertToInt()).thenReturn(123);
    assertEquals(123, getConvertedObject(Integer.class, converter));
    assertEquals(123, getConvertedObject(int.class, converter));
  }

  @Test
  void testConvertToLong() throws DatabricksSQLException {
    AbstractObjectConverter converter = mock(AbstractObjectConverter.class);
    when(converter.convertToLong()).thenReturn(123L);
    assertEquals(123L, getConvertedObject(Long.class, converter));
    assertEquals(123L, getConvertedObject(long.class, converter));
  }

  @Test
  void testConvertToFloat() throws DatabricksSQLException {
    AbstractObjectConverter converter = mock(AbstractObjectConverter.class);
    when(converter.convertToFloat()).thenReturn(1.23f);
    assertEquals(1.23f, getConvertedObject(Float.class, converter));
    assertEquals(1.23f, getConvertedObject(float.class, converter));
  }

  @Test
  void testConvertToDouble() throws DatabricksSQLException {
    AbstractObjectConverter converter = mock(AbstractObjectConverter.class);
    when(converter.convertToDouble()).thenReturn(1.234);
    assertEquals(1.234, getConvertedObject(Double.class, converter));
    assertEquals(1.234, getConvertedObject(double.class, converter));
  }

  @Test
  void testConvertToDate() throws DatabricksSQLException {
    AbstractObjectConverter converter = mock(AbstractObjectConverter.class);
    Date expected = new Date(System.currentTimeMillis());
    when(converter.convertToDate()).thenReturn(expected);
    assertEquals(expected, getConvertedObject(Date.class, converter));
  }

  @Test
  void testConvertToLocalDate() throws DatabricksSQLException {
    AbstractObjectConverter converter = mock(AbstractObjectConverter.class);
    LocalDate expected = LocalDate.now();
    when(converter.convertToLocalDate()).thenReturn(expected);
    assertEquals(expected, getConvertedObject(LocalDate.class, converter));
  }

  @Test
  void testConvertToBigInteger() throws DatabricksSQLException {
    AbstractObjectConverter converter = mock(AbstractObjectConverter.class);
    BigInteger expected = BigInteger.ONE;
    when(converter.convertToBigInteger()).thenReturn(expected);
    assertEquals(expected, getConvertedObject(BigInteger.class, converter));
  }

  @Test
  void testConvertToTimestamp() throws DatabricksSQLException {
    AbstractObjectConverter converter = mock(AbstractObjectConverter.class);
    Timestamp expected = new Timestamp(System.currentTimeMillis());
    when(converter.convertToTimestamp()).thenReturn(expected);
    assertEquals(expected, getConvertedObject(Timestamp.class, converter));
    assertEquals(expected, getConvertedObject(Calendar.class, converter));
  }

  @Test
  void testConvertToShort() throws DatabricksSQLException {
    AbstractObjectConverter converter = mock(AbstractObjectConverter.class);
    when(converter.convertToShort()).thenReturn((short) 123);
    assertEquals((short) 123, getConvertedObject(Byte.class, converter));
    assertEquals((short) 123, getConvertedObject(byte.class, converter));
  }

  @Test
  void testConvertToOther() throws DatabricksSQLException {
    AbstractObjectConverter converter = mock(AbstractObjectConverter.class);
    when(converter.convertToString()).thenReturn("otherString");
    assertEquals("otherString", getConvertedObject(Year.class, converter));
  }

  @Test
  void getObjectConverterForInt() throws DatabricksSQLException {
    assertTrue(converterHelper.getObjectConverter(123, Types.INTEGER) instanceof IntConverter);
  }

  @Test
  void getObjectConverterForString() throws DatabricksSQLException {
    assertTrue(converterHelper.getObjectConverter("abc", Types.VARCHAR) instanceof StringConverter);
  }

  @Test
  void getObjectConverterForBigDecimal() throws DatabricksSQLException {
    assertTrue(
        converterHelper.getObjectConverter(BigDecimal.ONE, Types.DECIMAL)
            instanceof BigDecimalConverter);
  }

  @Test
  void getObjectConverterForBoolean() throws DatabricksSQLException {
    assertTrue(converterHelper.getObjectConverter(true, Types.BOOLEAN) instanceof BooleanConverter);
  }

  @Test
  void getObjectConverterForDate() throws DatabricksSQLException {
    assertTrue(
        converterHelper.getObjectConverter(new Date(System.currentTimeMillis()), Types.DATE)
            instanceof DateConverter);
  }

  @Test
  void getObjectConverterForTimestamp() throws DatabricksSQLException {
    assertTrue(
        converterHelper.getObjectConverter(
                new Timestamp(System.currentTimeMillis()), Types.TIMESTAMP)
            instanceof TimestampConverter);
  }

  @Test
  void whenColumnTypeIsFloat_thenGetFloatConverter() throws DatabricksSQLException {
    Object object = 1.23f;
    AbstractObjectConverter converter = converterHelper.getObjectConverter(object, Types.FLOAT);
    assertTrue(converter instanceof FloatConverter);
  }

  @Test
  void whenColumnTypeIsDouble_thenGetDoubleConverter() throws DatabricksSQLException {
    Object object = 1.23d;
    AbstractObjectConverter converter = converterHelper.getObjectConverter(object, Types.DOUBLE);
    assertTrue(converter instanceof DoubleConverter);
  }

  @Test
  void whenColumnTypeIsDecimal_thenGetBigDecimalConverter() throws DatabricksSQLException {
    Object object = new BigDecimal("123.45");
    AbstractObjectConverter converter = converterHelper.getObjectConverter(object, Types.DECIMAL);
    assertTrue(converter instanceof BigDecimalConverter);
  }

  @Test
  void whenColumnType_Other() throws DatabricksSQLException {
    Object object = new BigDecimal("123.45");
    AbstractObjectConverter converter = converterHelper.getObjectConverter(object, Types.DECIMAL);
    assertTrue(converter instanceof BigDecimalConverter);
  }
}
