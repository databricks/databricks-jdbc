package com.databricks.jdbc.api.impl.converters;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

public class BigDecimalConverterTest {
  private final BigDecimal NON_ZERO_OBJECT = BigDecimal.valueOf(10.2);
  private final BigDecimal ZERO_OBJECT = BigDecimal.valueOf(0);

  @Test
  public void testConvertToByte() throws DatabricksSQLException {
    assertEquals(new BigDecimalConverter().toByte(NON_ZERO_OBJECT), (byte) 10);
    assertEquals(new BigDecimalConverter().toByte(ZERO_OBJECT), (byte) 0);

    BigDecimal bigDecimalThatDoesNotFitInByte = BigDecimal.valueOf(257.1);
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new BigDecimalConverter().toByte(bigDecimalThatDoesNotFitInByte));
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToShort() throws DatabricksSQLException {
    assertEquals(new BigDecimalConverter().toShort(NON_ZERO_OBJECT), (short) 10);
    assertEquals(new BigDecimalConverter().toShort(ZERO_OBJECT), (short) 0);

    BigDecimal bigDecimalThatDoesNotFitInInt = BigDecimal.valueOf(32768.1);
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new BigDecimalConverter().toShort(bigDecimalThatDoesNotFitInInt));
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToInt() throws DatabricksSQLException {
    assertEquals(new BigDecimalConverter().toInt(NON_ZERO_OBJECT), 10);
    assertEquals(new BigDecimalConverter().toInt(ZERO_OBJECT), 0);

    BigDecimal bigDecimalThatDoesNotFitInInt = BigDecimal.valueOf(2147483648.1);
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new BigDecimalConverter().toInt(bigDecimalThatDoesNotFitInInt));
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToLong() throws DatabricksSQLException {
    assertEquals(new BigDecimalConverter().toLong(NON_ZERO_OBJECT), 10L);
    assertEquals(new BigDecimalConverter().toLong(ZERO_OBJECT), 0L);

    BigDecimal bigDecimalThatDoesNotFitInInt = BigDecimal.valueOf(9223372036854775808.1);
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new BigDecimalConverter().toLong(bigDecimalThatDoesNotFitInInt));
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToFloat() throws DatabricksSQLException {
    assertEquals(new BigDecimalConverter().toFloat(NON_ZERO_OBJECT), 10.2f);
    assertEquals(new BigDecimalConverter().toFloat(ZERO_OBJECT), 0f);
  }

  @Test
  public void testConvertToDouble() throws DatabricksSQLException {
    assertEquals(new BigDecimalConverter().toDouble(NON_ZERO_OBJECT), 10.2);
    assertEquals(new BigDecimalConverter().toDouble(ZERO_OBJECT), 0);
  }

  @Test
  public void testConvertToBigDecimal() throws DatabricksSQLException {
    assertEquals(new BigDecimalConverter().toBigDecimal(NON_ZERO_OBJECT), BigDecimal.valueOf(10.2));
    assertEquals(new BigDecimalConverter().toBigDecimal(ZERO_OBJECT), BigDecimal.valueOf(0));
    assertEquals(
        new BigDecimalConverter().toBigDecimal(NON_ZERO_OBJECT.toString()),
        BigDecimal.valueOf(10.2));
  }

  @Test
  public void testConvertToBoolean() throws DatabricksSQLException {
    assertTrue(new BigDecimalConverter().toBoolean(NON_ZERO_OBJECT));
    assertFalse(new BigDecimalConverter().toBoolean(ZERO_OBJECT));
  }

  @Test
  public void testConvertToByteArray() throws DatabricksSQLException {
    assertArrayEquals(
        new BigDecimalConverter().toByteArray(NON_ZERO_OBJECT),
        BigDecimal.valueOf(10.2).toBigInteger().toByteArray());
    assertArrayEquals(
        new BigDecimalConverter().toByteArray(ZERO_OBJECT),
        BigDecimal.valueOf(0).toBigInteger().toByteArray());
  }

  @Test
  public void testConvertToChar() {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new BigDecimalConverter().toChar(NON_ZERO_OBJECT));
    assertTrue(exception.getMessage().contains("Unsupported char conversion operation"));
  }

  @Test
  public void testConvertToString() throws DatabricksSQLException {
    assertEquals(new BigDecimalConverter().toString(NON_ZERO_OBJECT), "10.2");
    assertEquals(new BigDecimalConverter().toString(ZERO_OBJECT), "0");
  }

  @Test
  public void testConvertToTimestamp() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new BigDecimalConverter().toTimestamp(NON_ZERO_OBJECT));
    assertTrue(exception.getMessage().contains("Unsupported Timestamp conversion operation"));
  }

  @Test
  public void testConvertToDate() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new BigDecimalConverter().toDate(NON_ZERO_OBJECT));
    assertTrue(exception.getMessage().contains("Unsupported Date conversion operation"));
  }

  @Test
  public void testConvertToUnicodeStream() throws DatabricksSQLException, IOException {
    InputStream unicodeStream = new BigDecimalConverter().toUnicodeStream(NON_ZERO_OBJECT);
    BufferedReader reader = new BufferedReader(new InputStreamReader(unicodeStream));
    String result = reader.readLine();
    assertEquals(NON_ZERO_OBJECT.toString(), result);
  }

  @Test
  public void testConvertToBinaryStream()
      throws DatabricksSQLException, IOException, ClassNotFoundException {
    InputStream binaryStream = new BigDecimalConverter().toBinaryStream(NON_ZERO_OBJECT);
    ObjectInputStream objectInputStream = new ObjectInputStream(binaryStream);
    assertEquals(objectInputStream.readObject().toString(), NON_ZERO_OBJECT.toString());
  }

  @Test
  public void testConvertToAsciiStream() throws DatabricksSQLException, IOException {
    InputStream asciiStream = new BigDecimalConverter().toAsciiStream(NON_ZERO_OBJECT);
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(asciiStream, StandardCharsets.US_ASCII));
    String result = reader.readLine();
    assertEquals(NON_ZERO_OBJECT.toString(), result);
  }

  @Test
  public void testConvertToCharacterStream() throws DatabricksSQLException, IOException {
    BufferedReader reader =
        new BufferedReader(new BigDecimalConverter().toCharacterStream(NON_ZERO_OBJECT));
    String result = reader.readLine();
    assertEquals(NON_ZERO_OBJECT.toString(), result);
  }

  @Test
  public void testConvertToBigInteger() throws DatabricksSQLException {
    assertEquals(new BigDecimalConverter().toBigInteger(NON_ZERO_OBJECT), BigInteger.valueOf(10));
    assertEquals(new BigDecimalConverter().toBigInteger(ZERO_OBJECT), BigInteger.valueOf(0));
  }
}
