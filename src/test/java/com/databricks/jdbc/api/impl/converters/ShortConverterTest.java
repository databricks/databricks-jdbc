package com.databricks.jdbc.api.impl.converters;

import static org.junit.jupiter.api.Assertions.*;

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
    assertEquals(new ShortConverter(NON_ZERO_OBJECT).convertToByte(), (byte) 10);
    assertEquals(new ShortConverter(ZERO_OBJECT).convertToByte(), (byte) 0);

    short shortThatDoesNotFitInByte = 257;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new ShortConverter(shortThatDoesNotFitInByte).convertToByte());
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToShort() throws DatabricksSQLException {
    assertEquals(new ShortConverter(NON_ZERO_OBJECT).convertToShort(), (short) 10);
    assertEquals(new ShortConverter(ZERO_OBJECT).convertToShort(), (short) 0);
  }

  @Test
  public void testConvertToInt() throws DatabricksSQLException {
    assertEquals(new ShortConverter(NON_ZERO_OBJECT).convertToInt(), 10);
    assertEquals(new ShortConverter(ZERO_OBJECT).convertToInt(), 0);
  }

  @Test
  public void testConvertToLong() throws DatabricksSQLException {
    assertEquals(new ShortConverter(NON_ZERO_OBJECT).convertToLong(), 10L);
    assertEquals(new ShortConverter(ZERO_OBJECT).convertToLong(), 0L);
  }

  @Test
  public void testConvertToFloat() throws DatabricksSQLException {
    assertEquals(new ShortConverter(NON_ZERO_OBJECT).convertToFloat(), 10f);
    assertEquals(new ShortConverter(ZERO_OBJECT).convertToFloat(), 0f);
  }

  @Test
  public void testConvertToDouble() throws DatabricksSQLException {
    assertEquals(new ShortConverter(NON_ZERO_OBJECT).convertToDouble(), 10);
    assertEquals(new ShortConverter(ZERO_OBJECT).convertToDouble(), 0);
  }

  @Test
  public void testConvertToBigDecimal() throws DatabricksSQLException {
    assertEquals(new ShortConverter(NON_ZERO_OBJECT).convertToBigDecimal(), BigDecimal.valueOf(10));
    assertEquals(new ShortConverter(ZERO_OBJECT).convertToBigDecimal(), BigDecimal.valueOf(0));
  }

  @Test
  public void testConvertToBoolean() throws DatabricksSQLException {
    assertTrue(new ShortConverter(NON_ZERO_OBJECT).convertToBoolean());
    assertFalse(new ShortConverter(ZERO_OBJECT).convertToBoolean());
  }

  @Test
  public void testConvertToByteArray() throws DatabricksSQLException {
    assertArrayEquals(
        new ShortConverter(NON_ZERO_OBJECT).convertToByteArray(),
        ByteBuffer.allocate(2).putShort((short) 10).array());
    assertArrayEquals(
        new ShortConverter(ZERO_OBJECT).convertToByteArray(),
        ByteBuffer.allocate(2).putShort((short) 0).array());
  }

  @Test
  public void testConvertToChar() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new ShortConverter(NON_ZERO_OBJECT).convertToChar());
    assertTrue(exception.getMessage().contains("Unsupported conversion operation"));
  }

  @Test
  public void testConvertToString() throws DatabricksSQLException {
    assertEquals(new ShortConverter(NON_ZERO_OBJECT).convertToString(), "10");
    assertEquals(new ShortConverter(ZERO_OBJECT).convertToString(), "0");
  }

  @Test
  public void testConvertFromString() throws DatabricksSQLException {
    assertEquals(new ShortConverter("65").convertToInt(), 65);
  }

  @Test
  public void testConvertToTimestamp() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new ShortConverter(NON_ZERO_OBJECT).convertToTimestamp());
    assertTrue(exception.getMessage().contains("Unsupported conversion operation"));
  }

  @Test
  public void testConvertToDate() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new ShortConverter(NON_ZERO_OBJECT).convertToDate());
    assertTrue(exception.getMessage().contains("Unsupported conversion operation"));
  }

  @Test
  public void testConvertToBigInteger() throws DatabricksSQLException {
    assertEquals(new ShortConverter(NON_ZERO_OBJECT).convertToBigInteger(), BigInteger.valueOf(10));
    assertEquals(new ShortConverter(ZERO_OBJECT).convertToBigInteger(), BigInteger.valueOf(0));
  }
}
