package com.databricks.jdbc.api.impl.converters;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.junit.jupiter.api.Test;

public class ByteConverterTest {

  private final byte NON_ZERO_OBJECT = 65;
  private final byte ZERO_OBJECT = 0;

  @Test
  public void testConvertToByte() throws DatabricksSQLException {
    assertEquals(new ByteConverter(NON_ZERO_OBJECT).convertToByte(), (byte) 65);
    assertEquals(new ByteConverter(ZERO_OBJECT).convertToByte(), (byte) 0);
  }

  @Test
  public void testConvertToShort() throws DatabricksSQLException {
    assertEquals(new ByteConverter(NON_ZERO_OBJECT).convertToShort(), (short) 65);
    assertEquals(new ByteConverter(ZERO_OBJECT).convertToShort(), (short) 0);
  }

  @Test
  public void testConvertToInt() throws DatabricksSQLException {
    assertEquals(new ByteConverter(NON_ZERO_OBJECT).convertToInt(), 65);
    assertEquals(new ByteConverter(ZERO_OBJECT).convertToInt(), 0);
  }

  @Test
  public void testConvertToLong() throws DatabricksSQLException {
    assertEquals(new ByteConverter(NON_ZERO_OBJECT).convertToLong(), 65L);
    assertEquals(new ByteConverter(ZERO_OBJECT).convertToLong(), 0L);
  }

  @Test
  public void testConvertToFloat() throws DatabricksSQLException {
    assertEquals(new ByteConverter(NON_ZERO_OBJECT).convertToFloat(), 65f);
    assertEquals(new ByteConverter(ZERO_OBJECT).convertToFloat(), 0f);
  }

  @Test
  public void testConvertToDouble() throws DatabricksSQLException {
    assertEquals(new ByteConverter(NON_ZERO_OBJECT).convertToDouble(), 65);
    assertEquals(new ByteConverter(ZERO_OBJECT).convertToDouble(), 0);
  }

  @Test
  public void testConvertToBigDecimal() throws DatabricksSQLException {
    assertEquals(new ByteConverter(NON_ZERO_OBJECT).convertToBigDecimal(), BigDecimal.valueOf(65));
    assertEquals(new ByteConverter(ZERO_OBJECT).convertToBigDecimal(), BigDecimal.valueOf(0));
  }

  @Test
  public void testConvertToBoolean() throws DatabricksSQLException {
    assertTrue(new ByteConverter(NON_ZERO_OBJECT).convertToBoolean());
    assertFalse(new ByteConverter(ZERO_OBJECT).convertToBoolean());
  }

  @Test
  public void testConvertToByteArray() throws DatabricksSQLException {
    assertArrayEquals(new ByteConverter(NON_ZERO_OBJECT).convertToByteArray(), new byte[] {65});
    assertArrayEquals(new ByteConverter(ZERO_OBJECT).convertToByteArray(), new byte[] {0});
  }

  @Test
  public void testConvertToChar() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new ByteConverter(NON_ZERO_OBJECT).convertToChar());
    assertTrue(exception.getMessage().contains("Unsupported conversion operation"));
  }

  @Test
  public void testConvertToString() throws DatabricksSQLException {
    assertEquals(new ByteConverter(NON_ZERO_OBJECT).convertToString(), "A");
  }

  @Test
  public void testConvertFromString() throws DatabricksSQLException {
    assertEquals(new ByteConverter("65").convertToInt(), 65);
  }

  @Test
  public void testConvertToTimestamp() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new ByteConverter(NON_ZERO_OBJECT).convertToTimestamp());
    assertTrue(exception.getMessage().contains("Unsupported conversion operation"));
  }

  @Test
  public void testConvertToDate() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new ByteConverter(NON_ZERO_OBJECT).convertToDate());
    assertTrue(exception.getMessage().contains("Unsupported conversion operation"));
  }

  @Test
  public void testConvertToBigInteger() throws DatabricksSQLException {
    assertEquals(new ByteConverter(NON_ZERO_OBJECT).convertToBigInteger(), BigInteger.valueOf(65));
    assertEquals(new ByteConverter(ZERO_OBJECT).convertToBigInteger(), BigInteger.valueOf(0));
  }
}
