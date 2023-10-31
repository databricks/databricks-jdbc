package com.databricks.jdbc.core.converters;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.core.DatabricksSQLException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

public class FloatConverterTest {

  private float NON_ZERO_OBJECT = 10.2f;
  private float ZERO_OBJECT = 0f;

  @Test
  public void testConvertToByte() throws DatabricksSQLException {
    assertEquals(new FloatConverter(NON_ZERO_OBJECT).convertToByte(), (byte) 10);
    assertEquals(new FloatConverter(ZERO_OBJECT).convertToByte(), (byte) 0);

    float floatThatDoesNotFitInByte = 128.5f;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new FloatConverter(floatThatDoesNotFitInByte).convertToByte());
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToShort() throws DatabricksSQLException {
    assertEquals(new FloatConverter(NON_ZERO_OBJECT).convertToShort(), (short) 10);
    assertEquals(new FloatConverter(ZERO_OBJECT).convertToShort(), (short) 0);

    float floatThatDoesNotFitInShort = 32768.1f;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new FloatConverter(floatThatDoesNotFitInShort).convertToShort());
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToInt() throws DatabricksSQLException {
    assertEquals(new FloatConverter(NON_ZERO_OBJECT).convertToInt(), (int) 10);
    assertEquals(new FloatConverter(ZERO_OBJECT).convertToInt(), (int) 0);

    float floatThatDoesNotFitInInt = 2147483648.5f;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new FloatConverter(floatThatDoesNotFitInInt).convertToInt());
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToLong() throws DatabricksSQLException {
    assertEquals(new FloatConverter(NON_ZERO_OBJECT).convertToLong(), 10L);
    assertEquals(new FloatConverter(ZERO_OBJECT).convertToLong(), 0L);

    float floatThatDoesNotFitInLong = 1.5E20f;
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new FloatConverter(floatThatDoesNotFitInLong).convertToLong());
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToFloat() throws DatabricksSQLException {
    assertEquals(new FloatConverter(NON_ZERO_OBJECT).convertToFloat(), 10.2f);
    assertEquals(new FloatConverter(ZERO_OBJECT).convertToFloat(), 0f);
  }

  @Test
  public void testConvertToDouble() throws DatabricksSQLException {
    assertEquals(new FloatConverter(NON_ZERO_OBJECT).convertToDouble(), (double) 10.2f);
    assertEquals(new FloatConverter(ZERO_OBJECT).convertToDouble(), (double) 0);
  }

  @Test
  public void testConvertToBigDecimal() throws DatabricksSQLException {
    assertEquals(
        new FloatConverter(NON_ZERO_OBJECT).convertToBigDecimal(),
        new BigDecimal(Float.toString(10.2f)));
    assertEquals(
        new FloatConverter(ZERO_OBJECT).convertToBigDecimal(), new BigDecimal(Float.toString(0f)));
  }

  @Test
  public void testConvertToBoolean() throws DatabricksSQLException {
    assertEquals(new FloatConverter(NON_ZERO_OBJECT).convertToBoolean(), true);
    assertEquals(new FloatConverter(ZERO_OBJECT).convertToBoolean(), false);
  }

  @Test
  public void testConvertToByteArray() throws DatabricksSQLException {
    assertTrue(
        Arrays.equals(
            new FloatConverter(NON_ZERO_OBJECT).convertToByteArray(),
            ByteBuffer.allocate(4).putFloat(10.2f).array()));
    assertTrue(
        Arrays.equals(
            new FloatConverter(ZERO_OBJECT).convertToByteArray(),
            ByteBuffer.allocate(4).putFloat(0f).array()));
  }

  @Test
  public void testConvertToChar() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new FloatConverter(NON_ZERO_OBJECT).convertToChar());
    assertTrue(exception.getMessage().contains("Unsupported conversion operation"));
  }

  @Test
  public void testConvertToString() throws DatabricksSQLException {
    assertEquals(new FloatConverter(NON_ZERO_OBJECT).convertToString(), "10.2");
    assertEquals(new FloatConverter(ZERO_OBJECT).convertToString(), "0.0");
  }

  @Test
  public void testConvertToTimestamp() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new FloatConverter(NON_ZERO_OBJECT).convertToTimestamp());
    assertTrue(exception.getMessage().contains("Unsupported conversion operation"));
  }

  @Test
  public void testConvertToDate() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new FloatConverter(NON_ZERO_OBJECT).convertToDate());
    assertTrue(exception.getMessage().contains("Unsupported conversion operation"));
  }
}
