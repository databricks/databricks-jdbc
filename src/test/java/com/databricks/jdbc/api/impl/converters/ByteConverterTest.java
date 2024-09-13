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
    assertEquals(new ByteConverter().toByte(NON_ZERO_OBJECT), (byte) 65);
    assertEquals(new ByteConverter().toByte(ZERO_OBJECT), (byte) 0);
  }

  @Test
  public void testConvertToShort() throws DatabricksSQLException {
    assertEquals(new ByteConverter().toShort(NON_ZERO_OBJECT), (short) 65);
    assertEquals(new ByteConverter().toShort(ZERO_OBJECT), (short) 0);
  }

  @Test
  public void testConvertToInt() throws DatabricksSQLException {
    assertEquals(new ByteConverter().toInt(NON_ZERO_OBJECT), 65);
    assertEquals(new ByteConverter().toInt(ZERO_OBJECT), 0);
  }

  @Test
  public void testConvertToLong() throws DatabricksSQLException {
    assertEquals(new ByteConverter().toLong(NON_ZERO_OBJECT), 65L);
    assertEquals(new ByteConverter().toLong(ZERO_OBJECT), 0L);
  }

  @Test
  public void testConvertToFloat() throws DatabricksSQLException {
    assertEquals(new ByteConverter().toFloat(NON_ZERO_OBJECT), 65f);
    assertEquals(new ByteConverter().toFloat(ZERO_OBJECT), 0f);
  }

  @Test
  public void testConvertToDouble() throws DatabricksSQLException {
    assertEquals(new ByteConverter().toDouble(NON_ZERO_OBJECT), 65);
    assertEquals(new ByteConverter().toDouble(ZERO_OBJECT), 0);
  }

  @Test
  public void testConvertToBigDecimal() throws DatabricksSQLException {
    assertEquals(new ByteConverter().toBigDecimal(NON_ZERO_OBJECT), BigDecimal.valueOf(65));
    assertEquals(new ByteConverter().toBigDecimal(ZERO_OBJECT), BigDecimal.valueOf(0));
  }

  @Test
  public void testConvertToBoolean() throws DatabricksSQLException {
    assertTrue(new ByteConverter().toBoolean(NON_ZERO_OBJECT));
    assertFalse(new ByteConverter().toBoolean(ZERO_OBJECT));
  }

  @Test
  public void testConvertToByteArray() throws DatabricksSQLException {
    assertArrayEquals(new ByteConverter().toByteArray(NON_ZERO_OBJECT), new byte[] {65});
    assertArrayEquals(new ByteConverter().toByteArray(ZERO_OBJECT), new byte[] {0});
  }

  @Test
  public void testConvertToChar() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new ByteConverter().toChar(NON_ZERO_OBJECT));
    assertTrue(exception.getMessage().contains("Unsupported char conversion operation"));
  }

  @Test
  public void testConvertToString() throws DatabricksSQLException {
    assertEquals(new ByteConverter().toString(NON_ZERO_OBJECT), "A");
  }

  @Test
  public void testConvertFromString() throws DatabricksSQLException {
    assertEquals(new ByteConverter().toInt("65"), 65);
  }

  @Test
  public void testConvertToTimestamp() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new ByteConverter().toTimestamp(NON_ZERO_OBJECT));
    assertTrue(exception.getMessage().contains("Unsupported Timestamp conversion operation"));
  }

  @Test
  public void testConvertToDate() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new ByteConverter().toDate(NON_ZERO_OBJECT));
    assertTrue(exception.getMessage().contains("Unsupported Date conversion operation"));
  }

  @Test
  public void testConvertToBigInteger() throws DatabricksSQLException {
    assertEquals(new ByteConverter().toBigInteger(NON_ZERO_OBJECT), BigInteger.valueOf(65));
    assertEquals(new ByteConverter().toBigInteger(ZERO_OBJECT), BigInteger.valueOf(0));
  }
}
