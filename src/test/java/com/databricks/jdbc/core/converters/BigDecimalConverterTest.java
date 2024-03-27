package com.databricks.jdbc.core.converters;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.core.DatabricksSQLException;
import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

public class BigDecimalConverterTest {

  private BigDecimal NON_ZERO_OBJECT = BigDecimal.valueOf(10.2);
  private BigDecimal ZERO_OBJECT = BigDecimal.valueOf(0);

  @Test
  public void testConvertToByte() throws DatabricksSQLException {
    assertEquals(new BigDecimalConverter(NON_ZERO_OBJECT).convertToByte(), (byte) 10);
    assertEquals(new BigDecimalConverter(ZERO_OBJECT).convertToByte(), (byte) 0);

    BigDecimal bigDecimalThatDoesNotFitInByte = BigDecimal.valueOf(257.1);
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new BigDecimalConverter(bigDecimalThatDoesNotFitInByte).convertToByte());
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToShort() throws DatabricksSQLException {
    assertEquals(new BigDecimalConverter(NON_ZERO_OBJECT).convertToShort(), (short) 10);
    assertEquals(new BigDecimalConverter(ZERO_OBJECT).convertToShort(), (short) 0);

    BigDecimal bigDecimalThatDoesNotFitInInt = BigDecimal.valueOf(32768.1);
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new BigDecimalConverter(bigDecimalThatDoesNotFitInInt).convertToShort());
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToInt() throws DatabricksSQLException {
    assertEquals(new BigDecimalConverter(NON_ZERO_OBJECT).convertToInt(), (int) 10);
    assertEquals(new BigDecimalConverter(ZERO_OBJECT).convertToInt(), (int) 0);

    BigDecimal bigDecimalThatDoesNotFitInInt = BigDecimal.valueOf(2147483648.1);
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new BigDecimalConverter(bigDecimalThatDoesNotFitInInt).convertToInt());
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToLong() throws DatabricksSQLException {
    assertEquals(new BigDecimalConverter(NON_ZERO_OBJECT).convertToLong(), 10L);
    assertEquals(new BigDecimalConverter(ZERO_OBJECT).convertToLong(), 0L);

    BigDecimal bigDecimalThatDoesNotFitInInt = BigDecimal.valueOf(9223372036854775808.1);
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new BigDecimalConverter(bigDecimalThatDoesNotFitInInt).convertToLong());
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToFloat() throws DatabricksSQLException {
    assertEquals(new BigDecimalConverter(NON_ZERO_OBJECT).convertToFloat(), 10.2f);
    assertEquals(new BigDecimalConverter(ZERO_OBJECT).convertToFloat(), 0f);
  }

  @Test
  public void testConvertToDouble() throws DatabricksSQLException {
    assertEquals(new BigDecimalConverter(NON_ZERO_OBJECT).convertToDouble(), (double) 10.2);
    assertEquals(new BigDecimalConverter(ZERO_OBJECT).convertToDouble(), (double) 0);
  }

  @Test
  public void testConvertToBigDecimal() throws DatabricksSQLException {
    assertEquals(
        new BigDecimalConverter(NON_ZERO_OBJECT).convertToBigDecimal(), BigDecimal.valueOf(10.2));
    assertEquals(new BigDecimalConverter(ZERO_OBJECT).convertToBigDecimal(), BigDecimal.valueOf(0));
    assertEquals(
        new BigDecimalConverter(NON_ZERO_OBJECT.toString()).convertToBigDecimal(),
        BigDecimal.valueOf(10.2));
  }

  @Test
  public void testConvertToBoolean() throws DatabricksSQLException {
    assertEquals(new BigDecimalConverter(NON_ZERO_OBJECT).convertToBoolean(), true);
    assertEquals(new BigDecimalConverter(ZERO_OBJECT).convertToBoolean(), false);
  }

  @Test
  public void testConvertToByteArray() throws DatabricksSQLException {
    assertTrue(
        Arrays.equals(
            new BigDecimalConverter(NON_ZERO_OBJECT).convertToByteArray(),
            BigDecimal.valueOf(10.2).toBigInteger().toByteArray()));
    assertTrue(
        Arrays.equals(
            new BigDecimalConverter(ZERO_OBJECT).convertToByteArray(),
            BigDecimal.valueOf(0).toBigInteger().toByteArray()));
  }

  @Test
  public void testConvertToChar() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new BigDecimalConverter(NON_ZERO_OBJECT).convertToChar());
    assertTrue(exception.getMessage().contains("Unsupported conversion operation"));
  }

  @Test
  public void testConvertToString() throws DatabricksSQLException {
    assertEquals(new BigDecimalConverter(NON_ZERO_OBJECT).convertToString(), "10.2");
    assertEquals(new BigDecimalConverter(ZERO_OBJECT).convertToString(), "0");
  }

  @Test
  public void testConvertToTimestamp() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new BigDecimalConverter(NON_ZERO_OBJECT).convertToTimestamp());
    assertTrue(exception.getMessage().contains("Unsupported conversion operation"));
  }

  @Test
  public void testConvertToDate() throws DatabricksSQLException {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new BigDecimalConverter(NON_ZERO_OBJECT).convertToDate());
    assertTrue(exception.getMessage().contains("Unsupported conversion operation"));
  }

  @Test
  public void testConvertToUnicodeStream() throws DatabricksSQLException, IOException {
    InputStream unicodeStream = new BigDecimalConverter(NON_ZERO_OBJECT).convertToUnicodeStream();
    BufferedReader reader = new BufferedReader(new InputStreamReader(unicodeStream));
    String result = reader.readLine();
    assertEquals(NON_ZERO_OBJECT.toString(), result);
  }

  @Test
  public void testConvertToBinaryStream()
      throws DatabricksSQLException, IOException, ClassNotFoundException {
    InputStream binaryStream = new BigDecimalConverter(NON_ZERO_OBJECT).convertToBinaryStream();
    ObjectInputStream objectInputStream = new ObjectInputStream(binaryStream);
    assertEquals(objectInputStream.readObject().toString(), NON_ZERO_OBJECT.toString());
  }

  @Test
  public void testConvertToAsciiStream() throws DatabricksSQLException, IOException {
    InputStream asciiStream = new BigDecimalConverter(NON_ZERO_OBJECT).convertToAsciiStream();
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(asciiStream, StandardCharsets.US_ASCII));
    String result = reader.readLine();
    assertEquals(NON_ZERO_OBJECT.toString(), result);
  }

  @Test
  public void testConvertToCharacterStream() throws DatabricksSQLException, IOException {
    BufferedReader reader =
        new BufferedReader(new BigDecimalConverter(NON_ZERO_OBJECT).convertToCharacterStream());
    String result = reader.readLine();
    assertEquals(NON_ZERO_OBJECT.toString(), result);
  }
}
