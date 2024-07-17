package com.databricks.jdbc.core.converters;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.core.DatabricksSQLException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

public class IntConverterTest {

  private int NON_ZERO_OBJECT = 10;
  private int ZERO_OBJECT = 0;

  @Test
  public void testConvertToByte() throws DatabricksSQLException {
    assertEquals(new IntConverter(NON_ZERO_OBJECT).convertToByte(), (byte) 10);
    assertEquals(new IntConverter(ZERO_OBJECT).convertToByte(), (byte) 0);

    int intThatDoesNotFitInByte = 257;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new IntConverter(intThatDoesNotFitInByte).convertToByte());
    assertTrue(exception.getMessage().contains("Invalid conversion"));

    assertThrows(
        DatabricksSQLException.class, () -> new IntConverter(Byte.MIN_VALUE - 1).convertToByte());
  }

  @Test
  public void testConvertToShort() throws DatabricksSQLException {
    assertEquals(new IntConverter(NON_ZERO_OBJECT).convertToShort(), (short) 10);
    assertEquals(new IntConverter(ZERO_OBJECT).convertToShort(), (short) 0);

    int intThatDoesNotFitInShort = 32768;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new IntConverter(intThatDoesNotFitInShort).convertToShort());
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToInt() throws DatabricksSQLException {
    assertEquals(new IntConverter(NON_ZERO_OBJECT).convertToInt(), (int) 10);
    assertEquals(new IntConverter(ZERO_OBJECT).convertToInt(), (int) 0);
  }

  @Test
  public void testConvertToLong() throws DatabricksSQLException {
    assertEquals(new IntConverter(NON_ZERO_OBJECT).convertToLong(), 10L);
    assertEquals(new IntConverter(ZERO_OBJECT).convertToLong(), 0L);
  }

  @Test
  public void testConvertToFloat() throws DatabricksSQLException {
    assertEquals(new IntConverter(NON_ZERO_OBJECT).convertToFloat(), 10f);
    assertEquals(new IntConverter(ZERO_OBJECT).convertToFloat(), 0f);
  }

  @Test
  public void testConvertToDouble() throws DatabricksSQLException {
    assertEquals(new IntConverter(NON_ZERO_OBJECT).convertToDouble(), (double) 10);
    assertEquals(new IntConverter(ZERO_OBJECT).convertToDouble(), (double) 0);
  }

  @Test
  public void testConvertToBigDecimal() throws DatabricksSQLException {
    assertEquals(new IntConverter(NON_ZERO_OBJECT).convertToBigDecimal(), BigDecimal.valueOf(10));
    assertEquals(new IntConverter(ZERO_OBJECT).convertToBigDecimal(), BigDecimal.valueOf(0));
  }

  @Test
  public void testConvertToBoolean() throws DatabricksSQLException {
    assertEquals(new IntConverter(NON_ZERO_OBJECT).convertToBoolean(), true);
    assertEquals(new IntConverter(ZERO_OBJECT).convertToBoolean(), false);
  }

  @Test
  public void testConvertToByteArray() throws DatabricksSQLException {
    assertTrue(
        Arrays.equals(
            new IntConverter(NON_ZERO_OBJECT).convertToByteArray(),
            ByteBuffer.allocate(4).putInt(10).array()));
    assertTrue(
        Arrays.equals(
            new IntConverter(ZERO_OBJECT).convertToByteArray(),
            ByteBuffer.allocate(4).putInt(0).array()));
  }

  @Test
  public void testConvertToChar() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new IntConverter(NON_ZERO_OBJECT).convertToChar());
    assertTrue(exception.getMessage().contains("Unsupported conversion operation"));
  }

  @Test
  public void testConvertToString() throws DatabricksSQLException {
    assertEquals(new IntConverter(NON_ZERO_OBJECT).convertToString(), "10");
    assertEquals(new IntConverter(ZERO_OBJECT).convertToString(), "0");
  }

  @Test
  public void testConvertFromString() throws DatabricksSQLException {
    assertEquals(new IntConverter("65").convertToInt(), 65);
  }

  @Test
  public void testConvertToTimestamp() throws DatabricksSQLException {
    assertEquals(
        new IntConverter(NON_ZERO_OBJECT).convertToTimestamp().toInstant().toString(),
        "1970-01-01T00:00:00.010Z");
    assertEquals(
        new IntConverter(ZERO_OBJECT).convertToTimestamp().toInstant().toString(),
        "1970-01-01T00:00:00Z");
  }

  @Test
  public void testConvertToTimestampWithScale() throws DatabricksSQLException {
    assertThrows(
        DatabricksSQLException.class,
        () -> new IntConverter(NON_ZERO_OBJECT).convertToTimestamp(10));
    assertDoesNotThrow(() -> new IntConverter(NON_ZERO_OBJECT).convertToTimestamp(5));
  }

  @Test
  public void testConvertToDate() throws DatabricksSQLException {
    assertEquals(new IntConverter(NON_ZERO_OBJECT).convertToDate(), Date.valueOf("1970-01-11"));
    assertEquals(new IntConverter(ZERO_OBJECT).convertToDate(), Date.valueOf("1970-01-01"));
  }

  @Test
  public void testConvertToBigInteger() throws DatabricksSQLException {
    assertEquals(new IntConverter(NON_ZERO_OBJECT).convertToBigInteger(), BigInteger.valueOf(10L));
    assertEquals(new IntConverter(ZERO_OBJECT).convertToBigInteger(), BigInteger.valueOf(0L));
  }
}
