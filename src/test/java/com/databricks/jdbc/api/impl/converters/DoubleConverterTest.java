package com.databricks.jdbc.api.impl.converters;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

public class DoubleConverterTest {
  private final double NON_ZERO_OBJECT = 10.2;
  private final double ZERO_OBJECT = 0;

  @Test
  public void testConvertToByte() throws DatabricksSQLException {
    assertEquals(new DoubleConverter(null).toByte(NON_ZERO_OBJECT), (byte) 10);
    assertEquals(new DoubleConverter(null).toByte(ZERO_OBJECT), (byte) 0);

    double doubleThatDoesNotFitInByte = 128.5;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new DoubleConverter(null).toByte(doubleThatDoesNotFitInByte));
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToShort() throws DatabricksSQLException {
    assertEquals(new DoubleConverter(null).toShort(NON_ZERO_OBJECT), (short) 10);
    assertEquals(new DoubleConverter(null).toShort(ZERO_OBJECT), (short) 0);

    double doubleThatDoesNotFitInShort = 32768.1;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new DoubleConverter(null).toShort(doubleThatDoesNotFitInShort));
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToInt() throws DatabricksSQLException {
    assertEquals(new DoubleConverter(null).toInt(NON_ZERO_OBJECT), 10);
    assertEquals(new DoubleConverter(null).toInt(ZERO_OBJECT), 0);

    double doubleThatDoesNotFitInInt = 2147483648.5;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new DoubleConverter(null).toInt(doubleThatDoesNotFitInInt));
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToLong() throws DatabricksSQLException {
    assertEquals(new DoubleConverter(null).toLong(NON_ZERO_OBJECT), 10L);
    assertEquals(new DoubleConverter(null).toLong(ZERO_OBJECT), 0L);

    double doubleThatDoesNotFitInLong = 1.5E20;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new DoubleConverter(null).toLong(doubleThatDoesNotFitInLong));
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToFloat() throws DatabricksSQLException {
    assertEquals(new DoubleConverter(null).toFloat(NON_ZERO_OBJECT), 10.2f);
    assertEquals(new DoubleConverter(null).toFloat(ZERO_OBJECT), 0f);

    double doubleThatDoesNotFitInFloat = 1.5E40;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new DoubleConverter(null).toFloat(doubleThatDoesNotFitInFloat));
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToDouble() throws DatabricksSQLException {
    assertEquals(new DoubleConverter(null).toDouble(NON_ZERO_OBJECT), 10.2);
    assertEquals(new DoubleConverter(null).toDouble(ZERO_OBJECT), 0);
  }

  @Test
  public void testConvertToBigDecimal() throws DatabricksSQLException {
    assertEquals(
        new DoubleConverter(null).toBigDecimal(NON_ZERO_OBJECT),
        new BigDecimal(Double.toString(10.2)));
    assertEquals(
        new DoubleConverter(null).toBigDecimal(ZERO_OBJECT), new BigDecimal(Double.toString(0)));
  }

  @Test
  public void testConvertToBoolean() throws DatabricksSQLException {
    assertTrue(new DoubleConverter(null).toBoolean(NON_ZERO_OBJECT));
    assertFalse(new DoubleConverter(null).toBoolean(ZERO_OBJECT));
  }

  @Test
  public void testConvertToByteArray() throws DatabricksSQLException {
    assertArrayEquals(
        new DoubleConverter(null).toByteArray(NON_ZERO_OBJECT),
        ByteBuffer.allocate(8).putDouble(10.2).array());
    assertArrayEquals(
        new DoubleConverter(null).toByteArray(ZERO_OBJECT),
        ByteBuffer.allocate(8).putDouble(0).array());
  }

  @Test
  public void testConvertToChar() {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new DoubleConverter(null).toChar(NON_ZERO_OBJECT));
    assertTrue(exception.getMessage().contains("Unsupported char conversion operation"));
  }

  @Test
  public void testConvertToString() throws DatabricksSQLException {
    assertEquals(new DoubleConverter(null).toString(NON_ZERO_OBJECT), "10.2");
    assertEquals(new DoubleConverter(null).toString(ZERO_OBJECT), "0.0");
  }

  @Test
  public void testConvertToTimestamp() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new DoubleConverter(null).toTimestamp(NON_ZERO_OBJECT));
    assertTrue(exception.getMessage().contains("Unsupported Timestamp conversion operation"));
  }

  @Test
  public void testConvertToDate() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new DoubleConverter(null).toDate(NON_ZERO_OBJECT));
    assertTrue(exception.getMessage().contains("Unsupported Date conversion operation"));
  }

  @Test
  public void testConvertToBigInteger() throws DatabricksSQLException {
    assertEquals(
        new DoubleConverter(null).toBigInteger(NON_ZERO_OBJECT),
        new BigDecimal("10.2").toBigInteger());
    assertEquals(
        new DoubleConverter(null).toBigInteger(ZERO_OBJECT), new BigDecimal(0).toBigInteger());
  }
}
