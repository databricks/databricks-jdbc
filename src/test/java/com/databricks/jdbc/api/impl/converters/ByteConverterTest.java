package com.databricks.jdbc.api.impl.converters;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.junit.jupiter.api.Test;

public class ByteConverterTest {
  private final byte NON_ZERO_OBJECT = 65;
  private final byte ZERO_OBJECT = 0;

  @Test
  public void testConvertToByte() throws DatabricksSQLException {
    assertEquals(new ByteConverter(null).toByte(NON_ZERO_OBJECT), (byte) 65);
    assertEquals(new ByteConverter(null).toByte(ZERO_OBJECT), (byte) 0);
  }

  @Test
  public void testConvertToShort() throws DatabricksSQLException {
    assertEquals(new ByteConverter(null).toShort(NON_ZERO_OBJECT), (short) 65);
    assertEquals(new ByteConverter(null).toShort(ZERO_OBJECT), (short) 0);
  }

  @Test
  public void testConvertToInt() throws DatabricksSQLException {
    assertEquals(new ByteConverter(null).toInt(NON_ZERO_OBJECT), 65);
    assertEquals(new ByteConverter(null).toInt(ZERO_OBJECT), 0);
  }

  @Test
  public void testConvertToLong() throws DatabricksSQLException {
    assertEquals(new ByteConverter(null).toLong(NON_ZERO_OBJECT), 65L);
    assertEquals(new ByteConverter(null).toLong(ZERO_OBJECT), 0L);
  }

  @Test
  public void testConvertToFloat() throws DatabricksSQLException {
    assertEquals(new ByteConverter(null).toFloat(NON_ZERO_OBJECT), 65f);
    assertEquals(new ByteConverter(null).toFloat(ZERO_OBJECT), 0f);
  }

  @Test
  public void testConvertToDouble() throws DatabricksSQLException {
    assertEquals(new ByteConverter(null).toDouble(NON_ZERO_OBJECT), 65);
    assertEquals(new ByteConverter(null).toDouble(ZERO_OBJECT), 0);
  }

  @Test
  public void testConvertToBigDecimal() throws DatabricksSQLException {
    assertEquals(new ByteConverter(null).toBigDecimal(NON_ZERO_OBJECT), BigDecimal.valueOf(65));
    assertEquals(new ByteConverter(null).toBigDecimal(ZERO_OBJECT), BigDecimal.valueOf(0));
  }

  @Test
  public void testConvertToBoolean() throws DatabricksSQLException {
    assertTrue(new ByteConverter(null).toBoolean(NON_ZERO_OBJECT));
    assertFalse(new ByteConverter(null).toBoolean(ZERO_OBJECT));
  }

  @Test
  public void testConvertToByteArray() throws DatabricksSQLException {
    assertArrayEquals(new ByteConverter(null).toByteArray(NON_ZERO_OBJECT), new byte[] {65});
    assertArrayEquals(new ByteConverter(null).toByteArray(ZERO_OBJECT), new byte[] {0});
  }

  @Test
  public void testConvertToChar() {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new ByteConverter(null).toChar(NON_ZERO_OBJECT));
    assertTrue(exception.getMessage().contains("Unsupported char conversion operation"));
  }

  @Test
  public void testConvertToString() throws DatabricksSQLException {
    assertEquals(new ByteConverter(null).toString(NON_ZERO_OBJECT), "A");
  }

  @Test
  public void testConvertFromString() throws DatabricksSQLException {
    assertEquals(new ByteConverter(null).toInt("65"), 65);
  }

  @Test
  public void testConvertToTimestamp() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new ByteConverter(null).toTimestamp(NON_ZERO_OBJECT));
    assertTrue(exception.getMessage().contains("Unsupported Timestamp conversion operation"));
  }

  @Test
  public void testConvertToDate() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new ByteConverter(null).toDate(NON_ZERO_OBJECT));
    assertTrue(exception.getMessage().contains("Unsupported Date conversion operation"));
  }

  @Test
  public void testConvertToBigInteger() throws DatabricksSQLException {
    assertEquals(new ByteConverter(null).toBigInteger(NON_ZERO_OBJECT), BigInteger.valueOf(65));
    assertEquals(new ByteConverter(null).toBigInteger(ZERO_OBJECT), BigInteger.valueOf(0));
  }
}
