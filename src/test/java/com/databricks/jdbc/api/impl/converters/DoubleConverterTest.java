package com.databricks.jdbc.api.impl.converters;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

public class DoubleConverterTest {

  private final double NON_ZERO_OBJECT = 10.2;
  private final double ZERO_OBJECT = 0;

  @Test
  public void testConvertToByte() throws DatabricksSQLException {
    assertEquals(new DoubleConverter(NON_ZERO_OBJECT).convertToByte(), (byte) 10);
    assertEquals(new DoubleConverter(ZERO_OBJECT).convertToByte(), (byte) 0);

    double doubleThatDoesNotFitInByte = 128.5;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new DoubleConverter(doubleThatDoesNotFitInByte).convertToByte());
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToShort() throws DatabricksSQLException {
    assertEquals(new DoubleConverter(NON_ZERO_OBJECT).convertToShort(), (short) 10);
    assertEquals(new DoubleConverter(ZERO_OBJECT).convertToShort(), (short) 0);

    double doubleThatDoesNotFitInShort = 32768.1;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new DoubleConverter(doubleThatDoesNotFitInShort).convertToShort());
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToInt() throws DatabricksSQLException {
    assertEquals(new DoubleConverter(NON_ZERO_OBJECT).convertToInt(), 10);
    assertEquals(new DoubleConverter(ZERO_OBJECT).convertToInt(), 0);

    double doubleThatDoesNotFitInInt = 2147483648.5;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new DoubleConverter(doubleThatDoesNotFitInInt).convertToInt());
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToLong() throws DatabricksSQLException {
    assertEquals(new DoubleConverter(NON_ZERO_OBJECT).convertToLong(), 10L);
    assertEquals(new DoubleConverter(ZERO_OBJECT).convertToLong(), 0L);

    double doubleThatDoesNotFitInLong = 1.5E20;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new DoubleConverter(doubleThatDoesNotFitInLong).convertToLong());
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToFloat() throws DatabricksSQLException {
    assertEquals(new DoubleConverter(NON_ZERO_OBJECT).convertToFloat(), 10.2f);
    assertEquals(new DoubleConverter(ZERO_OBJECT).convertToFloat(), 0f);

    double doubleThatDoesNotFitInFloat = 1.5E40;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new DoubleConverter(doubleThatDoesNotFitInFloat).convertToFloat());
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToDouble() throws DatabricksSQLException {
    assertEquals(new DoubleConverter(NON_ZERO_OBJECT).convertToDouble(), 10.2);
    assertEquals(new DoubleConverter(ZERO_OBJECT).convertToDouble(), 0);
  }

  @Test
  public void testConvertToBigDecimal() throws DatabricksSQLException {
    assertEquals(
        new DoubleConverter(NON_ZERO_OBJECT).convertToBigDecimal(),
        new BigDecimal(Double.toString(10.2)));
    assertEquals(
        new DoubleConverter(ZERO_OBJECT).convertToBigDecimal(), new BigDecimal(Double.toString(0)));
  }

  @Test
  public void testConvertToBoolean() throws DatabricksSQLException {
    assertTrue(new DoubleConverter(NON_ZERO_OBJECT).convertToBoolean());
    assertFalse(new DoubleConverter(ZERO_OBJECT).convertToBoolean());
  }

  @Test
  public void testConvertToByteArray() throws DatabricksSQLException {
    assertArrayEquals(
        new DoubleConverter(NON_ZERO_OBJECT).convertToByteArray(),
        ByteBuffer.allocate(8).putDouble(10.2).array());
    assertArrayEquals(
        new DoubleConverter(ZERO_OBJECT).convertToByteArray(),
        ByteBuffer.allocate(8).putDouble(0).array());
  }

  @Test
  public void testConvertToChar() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new DoubleConverter(NON_ZERO_OBJECT).convertToChar());
    assertTrue(exception.getMessage().contains("Unsupported conversion operation"));
  }

  @Test
  public void testConvertToString() throws DatabricksSQLException {
    assertEquals(new DoubleConverter(NON_ZERO_OBJECT).convertToString(), "10.2");
    assertEquals(new DoubleConverter(ZERO_OBJECT).convertToString(), "0.0");
  }

  @Test
  public void testConvertToTimestamp() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new DoubleConverter(NON_ZERO_OBJECT).convertToTimestamp());
    assertTrue(exception.getMessage().contains("Unsupported conversion operation"));
  }

  @Test
  public void testConvertToDate() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new DoubleConverter(NON_ZERO_OBJECT).convertToDate());
    assertTrue(exception.getMessage().contains("Unsupported conversion operation"));
  }

  @Test
  public void testConvertToBigInteger() throws DatabricksSQLException {
    assertEquals(
        new DoubleConverter(NON_ZERO_OBJECT).convertToBigInteger(),
        BigDecimal.valueOf(10.2).toBigInteger());
    assertEquals(
        new DoubleConverter(ZERO_OBJECT).convertToBigInteger(), new BigDecimal(0).toBigInteger());
  }
}
