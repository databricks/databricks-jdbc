package com.databricks.jdbc.api.impl.converters;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IntConverterTest {
  private final int NON_ZERO_OBJECT = 10;
  private final int ZERO_OBJECT = 0;

  @Test
  public void testConvertToByte() throws DatabricksSQLException {
    assertEquals(new IntConverter().toByte(NON_ZERO_OBJECT), (byte) 10);
    assertEquals(new IntConverter().toByte(ZERO_OBJECT), (byte) 0);

    int intThatDoesNotFitInByte = 257;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new IntConverter().toByte(intThatDoesNotFitInByte));
    assertTrue(exception.getMessage().contains("Invalid conversion"));

    assertThrows(DatabricksSQLException.class, () -> new IntConverter().toByte(Byte.MIN_VALUE - 1));
  }

  @Test
  public void testConvertToShort() throws DatabricksSQLException {
    assertEquals(new IntConverter().toShort(NON_ZERO_OBJECT), (short) 10);
    assertEquals(new IntConverter().toShort(ZERO_OBJECT), (short) 0);

    int intThatDoesNotFitInShort = 32768;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new IntConverter().toShort(intThatDoesNotFitInShort));
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToInt() throws DatabricksSQLException {
    assertEquals(new IntConverter().toInt(NON_ZERO_OBJECT), 10);
    assertEquals(new IntConverter().toInt(ZERO_OBJECT), 0);
  }

  @Test
  public void testConvertToLong() throws DatabricksSQLException {
    assertEquals(new IntConverter().toLong(NON_ZERO_OBJECT), 10L);
    assertEquals(new IntConverter().toLong(ZERO_OBJECT), 0L);
  }

  @Test
  public void testConvertToFloat() throws DatabricksSQLException {
    assertEquals(new IntConverter().toFloat(NON_ZERO_OBJECT), 10f);
    assertEquals(new IntConverter().toFloat(ZERO_OBJECT), 0f);
  }

  @Test
  public void testConvertToDouble() throws DatabricksSQLException {
    assertEquals(new IntConverter().toDouble(NON_ZERO_OBJECT), 10);
    assertEquals(new IntConverter().toDouble(ZERO_OBJECT), 0);
  }

  @Test
  public void testConvertToBigDecimal() throws DatabricksSQLException {
    assertEquals(new IntConverter().toBigDecimal(NON_ZERO_OBJECT), BigDecimal.valueOf(10));
    assertEquals(new IntConverter().toBigDecimal(ZERO_OBJECT), BigDecimal.valueOf(0));
  }

  @Test
  public void testConvertToBoolean() throws DatabricksSQLException {
    assertTrue(new IntConverter().toBoolean(NON_ZERO_OBJECT));
    assertFalse(new IntConverter().toBoolean(ZERO_OBJECT));
  }

  @Test
  public void testConvertToByteArray() throws DatabricksSQLException {
    assertArrayEquals(
        new IntConverter().toByteArray(NON_ZERO_OBJECT), ByteBuffer.allocate(4).putInt(10).array());
    assertArrayEquals(
        new IntConverter().toByteArray(ZERO_OBJECT), ByteBuffer.allocate(4).putInt(0).array());
  }

  @Test
  public void testConvertToChar() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new IntConverter().toChar(NON_ZERO_OBJECT));
    assertTrue(exception.getMessage().contains("Unsupported char conversion operation"));
  }

  @Test
  public void testConvertToString() throws DatabricksSQLException {
    assertEquals(new IntConverter().toString(NON_ZERO_OBJECT), "10");
    assertEquals(new IntConverter().toString(ZERO_OBJECT), "0");
  }

  @Test
  public void testConvertFromString() throws DatabricksSQLException {
    assertEquals(new IntConverter().toInt("65"), 65);
  }

  @Test
  public void testConvertToTimestamp() throws DatabricksSQLException {
    assertEquals(
        new IntConverter().toTimestamp(NON_ZERO_OBJECT).toInstant().toString(),
        "1970-01-01T00:00:00.010Z");
    assertEquals(
        new IntConverter().toTimestamp(ZERO_OBJECT).toInstant().toString(), "1970-01-01T00:00:00Z");
  }

  @Test
  public void testConvertToTimestampWithScale() {
    assertThrows(
        DatabricksSQLException.class, () -> new IntConverter().toTimestamp(NON_ZERO_OBJECT, 10));
    Assertions.assertDoesNotThrow(() -> new IntConverter().toTimestamp(NON_ZERO_OBJECT, 5));
  }

  @Test
  public void testConvertToDate() throws DatabricksSQLException {
    assertEquals(new IntConverter().toDate(NON_ZERO_OBJECT), Date.valueOf("1970-01-11"));
    assertEquals(new IntConverter().toDate(ZERO_OBJECT), Date.valueOf("1970-01-01"));
  }

  @Test
  public void testConvertToBigInteger() throws DatabricksSQLException {
    assertEquals(new IntConverter().toBigInteger(NON_ZERO_OBJECT), BigInteger.valueOf(10L));
    assertEquals(new IntConverter().toBigInteger(ZERO_OBJECT), BigInteger.valueOf(0L));
  }
}
