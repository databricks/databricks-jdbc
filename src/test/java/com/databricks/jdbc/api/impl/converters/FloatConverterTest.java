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
    assertEquals(new FloatConverter(null).toByte(NON_ZERO_OBJECT), (byte) 10);
    assertEquals(new FloatConverter(null).toByte(ZERO_OBJECT), (byte) 0);

    float floatThatDoesNotFitInByte = 128.5f;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new FloatConverter(null).toByte(floatThatDoesNotFitInByte));
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToShort() throws DatabricksSQLException {
    assertEquals(new FloatConverter(null).toShort(NON_ZERO_OBJECT), (short) 10);
    assertEquals(new FloatConverter(null).toShort(ZERO_OBJECT), (short) 0);

    float floatThatDoesNotFitInShort = 32768.1f;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new FloatConverter(null).toShort(floatThatDoesNotFitInShort));
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToInt() throws DatabricksSQLException {
    assertEquals(new FloatConverter(null).toInt(NON_ZERO_OBJECT), 10);
    assertEquals(new FloatConverter(null).toInt(ZERO_OBJECT), 0);

    float floatThatDoesNotFitInInt = 2147483648.5f;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new FloatConverter(null).toInt(floatThatDoesNotFitInInt));
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToLong() throws DatabricksSQLException {
    assertEquals(new FloatConverter(null).toLong(NON_ZERO_OBJECT), 10L);
    assertEquals(new FloatConverter(null).toLong(ZERO_OBJECT), 0L);

    float floatThatDoesNotFitInLong = 1.5E20f;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new FloatConverter(null).toLong(floatThatDoesNotFitInLong));
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToFloat() throws DatabricksSQLException {
    assertEquals(new FloatConverter(null).toFloat(NON_ZERO_OBJECT), 10.2f);
    assertEquals(new FloatConverter(null).toFloat(ZERO_OBJECT), 0f);
  }

  @Test
  public void testConvertToDouble() throws DatabricksSQLException {
    assertEquals(new FloatConverter(null).toDouble(NON_ZERO_OBJECT), 10.2f);
    assertEquals(new FloatConverter(null).toDouble(ZERO_OBJECT), 0);
  }

  @Test
  public void testConvertToBigDecimal() throws DatabricksSQLException {
    assertEquals(
        new FloatConverter(null).toBigDecimal(NON_ZERO_OBJECT),
        new BigDecimal(Float.toString(10.2f)));
    assertEquals(
        new FloatConverter(null).toBigDecimal(ZERO_OBJECT), new BigDecimal(Float.toString(0f)));
  }

  @Test
  public void testConvertToBoolean() throws DatabricksSQLException {
    assertTrue(new FloatConverter(null).toBoolean(NON_ZERO_OBJECT));
    assertFalse(new FloatConverter(null).toBoolean(ZERO_OBJECT));
  }

  @Test
  public void testConvertToByteArray() throws DatabricksSQLException {
    assertArrayEquals(
        new FloatConverter(null).toByteArray(NON_ZERO_OBJECT),
        ByteBuffer.allocate(4).putFloat(10.2f).array());
    assertArrayEquals(
        new FloatConverter(null).toByteArray(ZERO_OBJECT),
        ByteBuffer.allocate(4).putFloat(0f).array());
  }

  @Test
  public void testConvertToChar() {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new FloatConverter(null).toChar(NON_ZERO_OBJECT));
    assertTrue(exception.getMessage().contains("Unsupported char conversion operation"));
  }

  @Test
  public void testConvertToString() throws DatabricksSQLException {
    assertEquals(new FloatConverter(null).toString(NON_ZERO_OBJECT), "10.2");
    assertEquals(new FloatConverter(null).toString(ZERO_OBJECT), "0.0");
  }

  @Test
  public void testConvertToTimestamp() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new FloatConverter(null).toTimestamp(NON_ZERO_OBJECT));
    assertTrue(exception.getMessage().contains("Unsupported Timestamp conversion operation"));
  }

  @Test
  public void testConvertToDate() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new FloatConverter(null).toDate(NON_ZERO_OBJECT));
    assertTrue(exception.getMessage().contains("Unsupported Date conversion operation"));
  }

  @Test
  public void testConvertToBigInteger() throws DatabricksSQLException {
    assertEquals(
        new FloatConverter(null).toBigInteger(NON_ZERO_OBJECT),
        new BigDecimal("10.2").toBigInteger());
    assertEquals(
        new FloatConverter(null).toBigInteger(ZERO_OBJECT), new BigDecimal("0").toBigInteger());
  }
}
