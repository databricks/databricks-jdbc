package com.databricks.jdbc.api.impl.converters;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import org.junit.jupiter.api.Test;

public class LongConverterTest {
  private final long NON_ZERO_OBJECT = 10L;
  private final long ZERO_OBJECT = 0;

  @Test
  public void testConvertToByte() throws DatabricksSQLException {
    assertEquals(new LongConverter().toByte(NON_ZERO_OBJECT), (byte) 10);
    assertEquals(new LongConverter().toByte(ZERO_OBJECT), (byte) 0);

    long longThatDoesNotFitInByte = 257L;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new LongConverter().toByte(longThatDoesNotFitInByte));
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToShort() throws DatabricksSQLException {
    assertEquals(new LongConverter().toShort(NON_ZERO_OBJECT), (short) 10);
    assertEquals(new LongConverter().toShort(ZERO_OBJECT), (short) 0);

    long longThatDoesNotFitInShort = 32768L;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new LongConverter().toShort(longThatDoesNotFitInShort));
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToInt() throws DatabricksSQLException {
    assertEquals(new LongConverter().toInt(NON_ZERO_OBJECT), 10);
    assertEquals(new LongConverter().toInt(ZERO_OBJECT), 0);

    long longThatDoesNotFitInInt = 2147483648L;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new LongConverter().toShort(longThatDoesNotFitInInt));
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToLong() throws DatabricksSQLException {
    assertEquals(new LongConverter().toLong(NON_ZERO_OBJECT), 10L);
    assertEquals(new LongConverter().toLong(ZERO_OBJECT), 0L);
  }

  @Test
  public void testConvertToFloat() throws DatabricksSQLException {
    assertEquals(new LongConverter().toFloat(NON_ZERO_OBJECT), 10f);
    assertEquals(new LongConverter().toFloat(ZERO_OBJECT), 0f);
  }

  @Test
  public void testConvertToDouble() throws DatabricksSQLException {
    assertEquals(new LongConverter().toDouble(NON_ZERO_OBJECT), 10);
    assertEquals(new LongConverter().toDouble(ZERO_OBJECT), 0);
  }

  @Test
  public void testConvertToBigDecimal() throws DatabricksSQLException {
    assertEquals(new LongConverter().toBigDecimal(NON_ZERO_OBJECT), BigDecimal.valueOf(10));
    assertEquals(new LongConverter().toBigDecimal(ZERO_OBJECT), BigDecimal.valueOf(0));
  }

  @Test
  public void testConvertToBoolean() throws DatabricksSQLException {
    assertTrue(new LongConverter().toBoolean(NON_ZERO_OBJECT));
    assertFalse(new LongConverter().toBoolean(ZERO_OBJECT));
  }

  @Test
  public void testConvertToByteArray() throws DatabricksSQLException {
    assertArrayEquals(
        new LongConverter().toByteArray(NON_ZERO_OBJECT),
        ByteBuffer.allocate(8).putLong(10L).array());
    assertArrayEquals(
        new LongConverter().toByteArray(ZERO_OBJECT), ByteBuffer.allocate(8).putLong(0).array());
  }

  @Test
  public void testConvertToChar() {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new LongConverter().toChar(NON_ZERO_OBJECT));
    assertTrue(exception.getMessage().contains("Unsupported char conversion operation"));
  }

  @Test
  public void testConvertToString() throws DatabricksSQLException {
    assertEquals(new LongConverter().toString(NON_ZERO_OBJECT), "10");
    assertEquals(new LongConverter().toString(ZERO_OBJECT), "0");
  }

  @Test
  public void testConvertToTimestamp() throws DatabricksSQLException {
    assertEquals(
        new LongConverter().toTimestamp(NON_ZERO_OBJECT).toInstant().toString(),
        "1970-01-01T00:00:00.010Z");
    assertEquals(
        new LongConverter().toTimestamp(ZERO_OBJECT).toInstant().toString(),
        "1970-01-01T00:00:00Z");
  }

  @Test
  public void testConvertToDate() throws DatabricksSQLException {
    assertEquals(new LongConverter().toDate(NON_ZERO_OBJECT), Date.valueOf("1970-01-11"));
    assertEquals(new LongConverter().toDate(ZERO_OBJECT), Date.valueOf("1970-01-01"));
  }

  @Test
  public void testExceptions() {
    LongConverter longConverter = new LongConverter();
    assertThrows(DatabricksSQLException.class, () -> longConverter.toInt(Long.MAX_VALUE));
    assertThrows(DatabricksSQLException.class, () -> longConverter.toTimestamp(Long.MAX_VALUE, 10));
  }

  @Test
  public void testStringConversion() throws DatabricksSQLException {
    LongConverter longConverter = new LongConverter();
    assertEquals(longConverter.toInt("123"), 123);
  }

  @Test
  public void testConvertToBigInteger() throws DatabricksSQLException {
    assertEquals(new LongConverter().toBigInteger(NON_ZERO_OBJECT), BigInteger.valueOf(10));
    assertEquals(new LongConverter().toBigInteger(ZERO_OBJECT), BigInteger.valueOf(0));
  }
}
