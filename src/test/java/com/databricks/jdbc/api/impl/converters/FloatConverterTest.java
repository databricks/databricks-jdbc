package com.databricks.jdbc.api.impl.converters;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

public class FloatConverterTest {
  private final float NON_ZERO_OBJECT = 10.2f;
  private final float ZERO_OBJECT = 0f;

  @Test
  public void testConvertToByte() throws DatabricksSQLException {
    assertEquals(new FloatConverter().toByte(NON_ZERO_OBJECT), (byte) 10);
    assertEquals(new FloatConverter().toByte(ZERO_OBJECT), (byte) 0);

    float floatThatDoesNotFitInByte = 128.5f;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new FloatConverter().toByte(floatThatDoesNotFitInByte));
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToShort() throws DatabricksSQLException {
    assertEquals(new FloatConverter().toShort(NON_ZERO_OBJECT), (short) 10);
    assertEquals(new FloatConverter().toShort(ZERO_OBJECT), (short) 0);

    float floatThatDoesNotFitInShort = 32768.1f;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new FloatConverter().toShort(floatThatDoesNotFitInShort));
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToInt() throws DatabricksSQLException {
    assertEquals(new FloatConverter().toInt(NON_ZERO_OBJECT), 10);
    assertEquals(new FloatConverter().toInt(ZERO_OBJECT), 0);

    float floatThatDoesNotFitInInt = 2147483648.5f;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new FloatConverter().toInt(floatThatDoesNotFitInInt));
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToLong() throws DatabricksSQLException {
    assertEquals(new FloatConverter().toLong(NON_ZERO_OBJECT), 10L);
    assertEquals(new FloatConverter().toLong(ZERO_OBJECT), 0L);

    float floatThatDoesNotFitInLong = 1.5E20f;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new FloatConverter().toLong(floatThatDoesNotFitInLong));
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToFloat() throws DatabricksSQLException {
    assertEquals(new FloatConverter().toFloat(NON_ZERO_OBJECT), 10.2f);
    assertEquals(new FloatConverter().toFloat(ZERO_OBJECT), 0f);
  }

  @Test
  public void testConvertToDouble() throws DatabricksSQLException {
    assertEquals(new FloatConverter().toDouble(NON_ZERO_OBJECT), 10.2f);
    assertEquals(new FloatConverter().toDouble(ZERO_OBJECT), 0);
  }

  @Test
  public void testConvertToBigDecimal() throws DatabricksSQLException {
    assertEquals(
        new FloatConverter().toBigDecimal(NON_ZERO_OBJECT), new BigDecimal(Float.toString(10.2f)));
    assertEquals(
        new FloatConverter().toBigDecimal(ZERO_OBJECT), new BigDecimal(Float.toString(0f)));
  }

  @Test
  public void testConvertToBoolean() throws DatabricksSQLException {
    assertTrue(new FloatConverter().toBoolean(NON_ZERO_OBJECT));
    assertFalse(new FloatConverter().toBoolean(ZERO_OBJECT));
  }

  @Test
  public void testConvertToByteArray() throws DatabricksSQLException {
    assertArrayEquals(
        new FloatConverter().toByteArray(NON_ZERO_OBJECT),
        ByteBuffer.allocate(4).putFloat(10.2f).array());
    assertArrayEquals(
        new FloatConverter().toByteArray(ZERO_OBJECT), ByteBuffer.allocate(4).putFloat(0f).array());
  }

  @Test
  public void testConvertToChar() {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new FloatConverter().toChar(NON_ZERO_OBJECT));
    assertTrue(exception.getMessage().contains("Unsupported char conversion operation"));
  }

  @Test
  public void testConvertToString() throws DatabricksSQLException {
    assertEquals(new FloatConverter().toString(NON_ZERO_OBJECT), "10.2");
    assertEquals(new FloatConverter().toString(ZERO_OBJECT), "0.0");
  }

  @Test
  public void testConvertToTimestamp() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new FloatConverter().toTimestamp(NON_ZERO_OBJECT));
    assertTrue(exception.getMessage().contains("Unsupported Timestamp conversion operation"));
  }

  @Test
  public void testConvertToDate() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new FloatConverter().toDate(NON_ZERO_OBJECT));
    assertTrue(exception.getMessage().contains("Unsupported Date conversion operation"));
  }

  @Test
  public void testConvertToBigInteger() throws DatabricksSQLException {
    assertEquals(
        new FloatConverter().toBigInteger(NON_ZERO_OBJECT), new BigDecimal("10.2").toBigInteger());
    assertEquals(
        new FloatConverter().toBigInteger(ZERO_OBJECT), new BigDecimal("0").toBigInteger());
  }
}
