package com.databricks.jdbc.api.impl.converters;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.junit.jupiter.api.Test;

public class BooleanConverterTest {

  private final boolean TRUE_OBJECT = true;
  private final boolean FALSE_OBJECT = false;

  @Test
  public void testConvertToByte() throws DatabricksSQLException {
    assertEquals(new BooleanConverter().toByte(TRUE_OBJECT), (byte) 1);
    assertEquals(new BooleanConverter().toByte(FALSE_OBJECT), (byte) 0);
  }

  @Test
  public void testConvertToShort() throws DatabricksSQLException {
    assertEquals(new BooleanConverter().toShort(TRUE_OBJECT), (short) 1);
    assertEquals(new BooleanConverter().toShort(FALSE_OBJECT), (short) 0);
  }

  @Test
  public void testConvertToInt() throws DatabricksSQLException {
    assertEquals(new BooleanConverter().toInt(TRUE_OBJECT), 1);
    assertEquals(new BooleanConverter().toInt(FALSE_OBJECT), 0);
  }

  @Test
  public void testConvertToLong() throws DatabricksSQLException {
    assertEquals(new BooleanConverter().toLong(TRUE_OBJECT), 1L);
    assertEquals(new BooleanConverter().toLong(FALSE_OBJECT), 0L);
  }

  @Test
  public void testConvertToFloat() throws DatabricksSQLException {
    assertEquals(new BooleanConverter().toFloat(TRUE_OBJECT), 1f);
    assertEquals(new BooleanConverter().toFloat(FALSE_OBJECT), 0f);
  }

  @Test
  public void testConvertToDouble() throws DatabricksSQLException {
    assertEquals(new BooleanConverter().toDouble(TRUE_OBJECT), 1);
    assertEquals(new BooleanConverter().toDouble(FALSE_OBJECT), 0);
  }

  @Test
  public void testConvertToBigDecimal() throws DatabricksSQLException {
    assertEquals(new BooleanConverter().toBigDecimal(TRUE_OBJECT), BigDecimal.valueOf(1));
    assertEquals(new BooleanConverter().toBigDecimal(FALSE_OBJECT), BigDecimal.valueOf(0));
  }

  @Test
  public void testConvertToBoolean() throws DatabricksSQLException {
    assertTrue(new BooleanConverter().toBoolean(TRUE_OBJECT));
    assertFalse(new BooleanConverter().toBoolean(FALSE_OBJECT));
  }

  @Test
  public void testConvertToByteArray() throws DatabricksSQLException {
    assertArrayEquals(new BooleanConverter().toByteArray(TRUE_OBJECT), new byte[] {1});
    assertArrayEquals(new BooleanConverter().toByteArray(FALSE_OBJECT), new byte[] {0});
  }

  @Test
  public void testConvertToChar() throws DatabricksSQLException {
    assertEquals(new BooleanConverter().toChar(TRUE_OBJECT), '1');
    assertEquals(new BooleanConverter().toChar(FALSE_OBJECT), '0');
  }

  @Test
  public void testConvertToString() throws DatabricksSQLException {
    assertEquals(new BooleanConverter().toString(TRUE_OBJECT), "true");
    assertEquals(new BooleanConverter().toString(FALSE_OBJECT), "false");
  }

  @Test
  public void testConvertToTimestamp() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new BooleanConverter().toTimestamp(TRUE_OBJECT));
    assertTrue(exception.getMessage().contains("Unsupported Timestamp conversion operation"));
  }

  @Test
  public void testConvertToDate() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new BooleanConverter().toDate(TRUE_OBJECT));
    assertTrue(exception.getMessage().contains("Unsupported Date conversion operation"));
  }

  @Test
  public void testConvertToBigInteger() throws DatabricksSQLException {
    assertEquals(new BooleanConverter().toBigInteger(TRUE_OBJECT), BigInteger.valueOf(1));
    assertEquals(new BooleanConverter().toBigInteger(FALSE_OBJECT), BigInteger.valueOf(0));
  }
}
