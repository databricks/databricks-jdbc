package com.databricks.jdbc.api.impl.converters;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

public class ShortConverterTest {

  private final short NON_ZERO_OBJECT = 10;
  private final short ZERO_OBJECT = 0;

  @Test
  public void testConvertToByte() throws DatabricksSQLException {
    assertEquals(new ShortConverter().toByte(NON_ZERO_OBJECT), (byte) 10);
    assertEquals(new ShortConverter().toByte(ZERO_OBJECT), (byte) 0);

    short shortThatDoesNotFitInByte = 257;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new ShortConverter().toByte(shortThatDoesNotFitInByte));
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToShort() throws DatabricksSQLException {
    assertEquals(new ShortConverter().toShort(NON_ZERO_OBJECT), (short) 10);
    assertEquals(new ShortConverter().toShort(ZERO_OBJECT), (short) 0);
  }

  @Test
  public void testConvertToInt() throws DatabricksSQLException {
    assertEquals(new ShortConverter().toInt(NON_ZERO_OBJECT), 10);
    assertEquals(new ShortConverter().toInt(ZERO_OBJECT), 0);
  }

  @Test
  public void testConvertToLong() throws DatabricksSQLException {
    assertEquals(new ShortConverter().toLong(NON_ZERO_OBJECT), 10L);
    assertEquals(new ShortConverter().toLong(ZERO_OBJECT), 0L);
  }

  @Test
  public void testConvertToFloat() throws DatabricksSQLException {
    assertEquals(new ShortConverter().toFloat(NON_ZERO_OBJECT), 10f);
    assertEquals(new ShortConverter().toFloat(ZERO_OBJECT), 0f);
  }

  @Test
  public void testConvertToDouble() throws DatabricksSQLException {
    assertEquals(new ShortConverter().toDouble(NON_ZERO_OBJECT), 10);
    assertEquals(new ShortConverter().toDouble(ZERO_OBJECT), 0);
  }

  @Test
  public void testConvertToBigDecimal() throws DatabricksSQLException {
    assertEquals(new ShortConverter().toBigDecimal(NON_ZERO_OBJECT), BigDecimal.valueOf(10));
    assertEquals(new ShortConverter().toBigDecimal(ZERO_OBJECT), BigDecimal.valueOf(0));
  }

  @Test
  public void testConvertToBoolean() throws DatabricksSQLException {
    assertTrue(new ShortConverter().toBoolean(NON_ZERO_OBJECT));
    assertFalse(new ShortConverter().toBoolean(ZERO_OBJECT));
  }

  @Test
  public void testConvertToByteArray() throws DatabricksSQLException {
    assertArrayEquals(
        new ShortConverter().toByteArray(NON_ZERO_OBJECT),
        ByteBuffer.allocate(2).putShort((short) 10).array());
    assertArrayEquals(
        new ShortConverter().toByteArray(ZERO_OBJECT),
        ByteBuffer.allocate(2).putShort((short) 0).array());
  }

  @Test
  public void testConvertToChar() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new ShortConverter().toChar(NON_ZERO_OBJECT));
    assertTrue(exception.getMessage().contains("Unsupported char conversion operation"));
  }

  @Test
  public void testConvertToString() throws DatabricksSQLException {
    assertEquals(new ShortConverter().toString(NON_ZERO_OBJECT), "10");
    assertEquals(new ShortConverter().toString(ZERO_OBJECT), "0");
  }

  @Test
  public void testConvertFromString() throws DatabricksSQLException {
    assertEquals(new ShortConverter().toInt("65"), 65);
  }

  @Test
  public void testConvertToTimestamp() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new ShortConverter().toTimestamp(NON_ZERO_OBJECT));
    assertTrue(exception.getMessage().contains("Unsupported Timestamp conversion operation"));
  }

  @Test
  public void testConvertToDate() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new ShortConverter().toDate(NON_ZERO_OBJECT));
    assertTrue(exception.getMessage().contains("Unsupported Date conversion operation"));
  }

  @Test
  public void testConvertToBigInteger() throws DatabricksSQLException {
    assertEquals(new ShortConverter().toBigInteger(NON_ZERO_OBJECT), BigInteger.valueOf(10));
    assertEquals(new ShortConverter().toBigInteger(ZERO_OBJECT), BigInteger.valueOf(0));
  }
}
