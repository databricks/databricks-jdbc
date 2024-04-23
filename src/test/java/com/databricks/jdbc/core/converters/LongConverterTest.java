package com.databricks.jdbc.core.converters;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.core.DatabricksSQLException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

public class LongConverterTest {

  private long NON_ZERO_OBJECT = 10L;
  private long ZERO_OBJECT = 0;

  @Test
  public void testConvertToByte() throws DatabricksSQLException {
    assertEquals(new LongConverter(NON_ZERO_OBJECT).convertToByte(), (byte) 10);
    assertEquals(new LongConverter(ZERO_OBJECT).convertToByte(), (byte) 0);

    long longThatDoesNotFitInByte = 257L;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new LongConverter(longThatDoesNotFitInByte).convertToByte());
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToShort() throws DatabricksSQLException {
    assertEquals(new LongConverter(NON_ZERO_OBJECT).convertToShort(), (short) 10);
    assertEquals(new LongConverter(ZERO_OBJECT).convertToShort(), (short) 0);

    long longThatDoesNotFitInShort = 32768L;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new LongConverter(longThatDoesNotFitInShort).convertToShort());
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToInt() throws DatabricksSQLException {
    assertEquals(new LongConverter(NON_ZERO_OBJECT).convertToInt(), (int) 10);
    assertEquals(new LongConverter(ZERO_OBJECT).convertToInt(), (int) 0);

    long longThatDoesNotFitInInt = 2147483648L;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new LongConverter(longThatDoesNotFitInInt).convertToShort());
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToLong() throws DatabricksSQLException {
    assertEquals(new LongConverter(NON_ZERO_OBJECT).convertToLong(), 10L);
    assertEquals(new LongConverter(ZERO_OBJECT).convertToLong(), 0L);
  }

  @Test
  public void testConvertToFloat() throws DatabricksSQLException {
    assertEquals(new LongConverter(NON_ZERO_OBJECT).convertToFloat(), 10f);
    assertEquals(new LongConverter(ZERO_OBJECT).convertToFloat(), 0f);
  }

  @Test
  public void testConvertToDouble() throws DatabricksSQLException {
    assertEquals(new LongConverter(NON_ZERO_OBJECT).convertToDouble(), (double) 10);
    assertEquals(new LongConverter(ZERO_OBJECT).convertToDouble(), (double) 0);
  }

  @Test
  public void testConvertToBigDecimal() throws DatabricksSQLException {
    assertEquals(new LongConverter(NON_ZERO_OBJECT).convertToBigDecimal(), BigDecimal.valueOf(10));
    assertEquals(new LongConverter(ZERO_OBJECT).convertToBigDecimal(), BigDecimal.valueOf(0));
  }

  @Test
  public void testConvertToBoolean() throws DatabricksSQLException {
    assertEquals(new LongConverter(NON_ZERO_OBJECT).convertToBoolean(), true);
    assertEquals(new LongConverter(ZERO_OBJECT).convertToBoolean(), false);
  }

  @Test
  public void testConvertToByteArray() throws DatabricksSQLException {
    assertTrue(
        Arrays.equals(
            new LongConverter(NON_ZERO_OBJECT).convertToByteArray(),
            ByteBuffer.allocate(8).putLong(10L).array()));
    assertTrue(
        Arrays.equals(
            new LongConverter(ZERO_OBJECT).convertToByteArray(),
            ByteBuffer.allocate(8).putLong(0).array()));
  }

  @Test
  public void testConvertToChar() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new LongConverter(NON_ZERO_OBJECT).convertToChar());
    assertTrue(exception.getMessage().contains("Unsupported conversion operation"));
  }

  @Test
  public void testConvertToString() throws DatabricksSQLException {
    assertEquals(new LongConverter(NON_ZERO_OBJECT).convertToString(), "10");
    assertEquals(new LongConverter(ZERO_OBJECT).convertToString(), "0");
  }

  @Test
  public void testConvertToTimestamp() throws DatabricksSQLException {
    assertEquals(
        new LongConverter(NON_ZERO_OBJECT).convertToTimestamp().toInstant().toString(),
        "1970-01-01T00:00:00.010Z");
    assertEquals(
        new LongConverter(ZERO_OBJECT).convertToTimestamp().toInstant().toString(),
        "1970-01-01T00:00:00Z");
  }

  @Test
  public void testConvertToDate() throws DatabricksSQLException {
    assertEquals(new LongConverter(NON_ZERO_OBJECT).convertToDate(), Date.valueOf("1970-01-11"));
    assertEquals(new LongConverter(ZERO_OBJECT).convertToDate(), Date.valueOf("1970-01-01"));
  }

  @Test
  public void testExceptions() throws DatabricksSQLException {
    LongConverter longConverter = new LongConverter(Long.MAX_VALUE);
    assertThrows(DatabricksSQLException.class, longConverter::convertToInt);
    assertThrows(DatabricksSQLException.class, () -> longConverter.convertToTimestamp(10));
  }

  @Test
  public void testStringConversion() throws DatabricksSQLException {
    LongConverter longConverter = new LongConverter("123");
    assertEquals(longConverter.convertToInt(), 123);
  }
}
