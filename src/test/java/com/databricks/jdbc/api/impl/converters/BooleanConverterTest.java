package com.databricks.jdbc.api.impl.converters;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

public class BooleanConverterTest {

  private boolean TRUE_OBJECT = true;
  private boolean FALSE_OBJECT = false;

  @Test
  public void testConvertToByte() throws DatabricksSQLException {
    assertEquals(new BooleanConverter(TRUE_OBJECT).convertToByte(), (byte) 1);
    assertEquals(new BooleanConverter(FALSE_OBJECT).convertToByte(), (byte) 0);
  }

  @Test
  public void testConvertToShort() throws DatabricksSQLException {
    assertEquals(new BooleanConverter(TRUE_OBJECT).convertToShort(), (short) 1);
    assertEquals(new BooleanConverter(FALSE_OBJECT).convertToShort(), (short) 0);
  }

  @Test
  public void testConvertToInt() throws DatabricksSQLException {
    assertEquals(new BooleanConverter(TRUE_OBJECT).convertToInt(), (int) 1);
    assertEquals(new BooleanConverter(FALSE_OBJECT).convertToInt(), (int) 0);
  }

  @Test
  public void testConvertToLong() throws DatabricksSQLException {
    assertEquals(new BooleanConverter(TRUE_OBJECT).convertToLong(), 1L);
    assertEquals(new BooleanConverter(FALSE_OBJECT).convertToLong(), 0L);
  }

  @Test
  public void testConvertToFloat() throws DatabricksSQLException {
    assertEquals(new BooleanConverter(TRUE_OBJECT).convertToFloat(), 1f);
    assertEquals(new BooleanConverter(FALSE_OBJECT).convertToFloat(), 0f);
  }

  @Test
  public void testConvertToDouble() throws DatabricksSQLException {
    assertEquals(new BooleanConverter(TRUE_OBJECT).convertToDouble(), (double) 1);
    assertEquals(new BooleanConverter(FALSE_OBJECT).convertToDouble(), (double) 0);
  }

  @Test
  public void testConvertToBigDecimal() throws DatabricksSQLException {
    assertEquals(new BooleanConverter(TRUE_OBJECT).convertToBigDecimal(), BigDecimal.valueOf(1));
    assertEquals(new BooleanConverter(FALSE_OBJECT).convertToBigDecimal(), BigDecimal.valueOf(0));
  }

  @Test
  public void testConvertToBoolean() throws DatabricksSQLException {
    assertEquals(new BooleanConverter(TRUE_OBJECT).convertToBoolean(), true);
    assertEquals(new BooleanConverter(FALSE_OBJECT).convertToBoolean(), false);
  }

  @Test
  public void testConvertToByteArray() throws DatabricksSQLException {
    assertTrue(
        Arrays.equals(new BooleanConverter(TRUE_OBJECT).convertToByteArray(), new byte[] {1}));
    assertTrue(
        Arrays.equals(new BooleanConverter(FALSE_OBJECT).convertToByteArray(), new byte[] {0}));
  }

  @Test
  public void testConvertToChar() throws DatabricksSQLException {
    assertEquals(new BooleanConverter(TRUE_OBJECT).convertToChar(), '1');
    assertEquals(new BooleanConverter(FALSE_OBJECT).convertToChar(), '0');
  }

  @Test
  public void testConvertToString() throws DatabricksSQLException {
    assertEquals(new BooleanConverter(TRUE_OBJECT).convertToString(), "true");
    assertEquals(new BooleanConverter(FALSE_OBJECT).convertToString(), "false");
  }

  @Test
  public void testConvertToTimestamp() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new BooleanConverter(TRUE_OBJECT).convertToTimestamp());
    assertTrue(exception.getMessage().contains("Unsupported conversion operation"));
  }

  @Test
  public void testConvertToDate() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new BooleanConverter(TRUE_OBJECT).convertToDate());
    assertTrue(exception.getMessage().contains("Unsupported conversion operation"));
  }

  @Test
  public void testConvertToBigInteger() throws DatabricksSQLException {
    assertEquals(new BooleanConverter(TRUE_OBJECT).convertToBigInteger(), BigInteger.valueOf(1));
    assertEquals(new BooleanConverter(FALSE_OBJECT).convertToBigInteger(), BigInteger.valueOf(0));
  }
}
