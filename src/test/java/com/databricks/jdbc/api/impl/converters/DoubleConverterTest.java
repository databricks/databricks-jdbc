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
    assertEquals(new DoubleConverter().toByte(NON_ZERO_OBJECT), (byte) 10);
    assertEquals(new DoubleConverter().toByte(ZERO_OBJECT), (byte) 0);

    double doubleThatDoesNotFitInByte = 128.5;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new DoubleConverter().toByte(doubleThatDoesNotFitInByte));
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToShort() throws DatabricksSQLException {
    assertEquals(new DoubleConverter().toShort(NON_ZERO_OBJECT), (short) 10);
    assertEquals(new DoubleConverter().toShort(ZERO_OBJECT), (short) 0);

    double doubleThatDoesNotFitInShort = 32768.1;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new DoubleConverter().toShort(doubleThatDoesNotFitInShort));
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToInt() throws DatabricksSQLException {
    assertEquals(new DoubleConverter().toInt(NON_ZERO_OBJECT), 10);
    assertEquals(new DoubleConverter().toInt(ZERO_OBJECT), 0);

    double doubleThatDoesNotFitInInt = 2147483648.5;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new DoubleConverter().toInt(doubleThatDoesNotFitInInt));
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToLong() throws DatabricksSQLException {
    assertEquals(new DoubleConverter().toLong(NON_ZERO_OBJECT), 10L);
    assertEquals(new DoubleConverter().toLong(ZERO_OBJECT), 0L);

    double doubleThatDoesNotFitInLong = 1.5E20;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new DoubleConverter().toLong(doubleThatDoesNotFitInLong));
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToFloat() throws DatabricksSQLException {
    assertEquals(new DoubleConverter().toFloat(NON_ZERO_OBJECT), 10.2f);
    assertEquals(new DoubleConverter().toFloat(ZERO_OBJECT), 0f);

    double doubleThatDoesNotFitInFloat = 1.5E40;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new DoubleConverter().toFloat(doubleThatDoesNotFitInFloat));
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToDouble() throws DatabricksSQLException {
    assertEquals(new DoubleConverter().toDouble(NON_ZERO_OBJECT), 10.2);
    assertEquals(new DoubleConverter().toDouble(ZERO_OBJECT), 0);
  }

  @Test
  public void testConvertToBigDecimal() throws DatabricksSQLException {
    assertEquals(
        new DoubleConverter().toBigDecimal(NON_ZERO_OBJECT), new BigDecimal(Double.toString(10.2)));
    assertEquals(
        new DoubleConverter().toBigDecimal(ZERO_OBJECT), new BigDecimal(Double.toString(0)));
  }

  @Test
  public void testConvertToBoolean() throws DatabricksSQLException {
    assertTrue(new DoubleConverter().toBoolean(NON_ZERO_OBJECT));
    assertFalse(new DoubleConverter().toBoolean(ZERO_OBJECT));
  }

  @Test
  public void testConvertToByteArray() throws DatabricksSQLException {
    assertArrayEquals(
        new DoubleConverter().toByteArray(NON_ZERO_OBJECT),
        ByteBuffer.allocate(8).putDouble(10.2).array());
    assertArrayEquals(
        new DoubleConverter().toByteArray(ZERO_OBJECT),
        ByteBuffer.allocate(8).putDouble(0).array());
  }

  @Test
  public void testConvertToChar() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new DoubleConverter().toChar(NON_ZERO_OBJECT));
    assertTrue(exception.getMessage().contains("Unsupported char conversion operation"));
  }

  @Test
  public void testConvertToString() throws DatabricksSQLException {
    assertEquals(new DoubleConverter().toString(NON_ZERO_OBJECT), "10.2");
    assertEquals(new DoubleConverter().toString(ZERO_OBJECT), "0.0");
  }

  @Test
  public void testConvertToTimestamp() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new DoubleConverter().toTimestamp(NON_ZERO_OBJECT));
    assertTrue(exception.getMessage().contains("Unsupported Timestamp conversion operation"));
  }

  @Test
  public void testConvertToDate() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new DoubleConverter().toDate(NON_ZERO_OBJECT));
    assertTrue(exception.getMessage().contains("Unsupported Date conversion operation"));
  }

  @Test
  public void testConvertToBigInteger() throws DatabricksSQLException {
    assertEquals(
        new DoubleConverter().toBigInteger(NON_ZERO_OBJECT), new BigDecimal("10.2").toBigInteger());
    assertEquals(new DoubleConverter().toBigInteger(ZERO_OBJECT), new BigDecimal(0).toBigInteger());
  }
}
