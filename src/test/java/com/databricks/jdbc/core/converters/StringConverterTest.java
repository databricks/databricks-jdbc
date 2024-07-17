package com.databricks.jdbc.core.converters;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.core.DatabricksSQLException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

public class StringConverterTest {

  private String NUMERICAL_STRING = "10";
  private String NUMBERICAL_ZERO_STRING = "0";

  private String CHARACTER_STRING = "ABC";

  private String TIME_STAMP_STRING = "2023-09-10 00:00:00";

  private String DATE_STRING = "2023-09-10";

  @Test
  public void testConvertToByte() throws DatabricksSQLException {
    String singleCharacterString = "A";
    assertEquals(new StringConverter(singleCharacterString).convertToByte(), (byte) 'A');

    DatabricksSQLException tooManyCharactersException =
        assertThrows(
            DatabricksSQLException.class,
            () -> new StringConverter(CHARACTER_STRING).convertToByte());
    assertTrue(tooManyCharactersException.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToShort() throws DatabricksSQLException {
    assertEquals(new StringConverter(NUMERICAL_STRING).convertToShort(), (short) 10);
    assertEquals(new StringConverter(NUMBERICAL_ZERO_STRING).convertToShort(), (short) 0);

    String stringThatDoesNotFitInShort = "32768";
    DatabricksSQLException outOfRangeException =
        assertThrows(
            DatabricksSQLException.class,
            () -> new StringConverter(stringThatDoesNotFitInShort).convertToShort());
    assertTrue(outOfRangeException.getMessage().contains("Invalid conversion"));
    DatabricksSQLException invalidCharactersException =
        assertThrows(
            DatabricksSQLException.class,
            () -> new StringConverter(CHARACTER_STRING).convertToShort());
    assertTrue(invalidCharactersException.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToInt() throws DatabricksSQLException {
    assertEquals(new StringConverter(NUMERICAL_STRING).convertToInt(), (int) 10);
    assertEquals(new StringConverter(NUMBERICAL_ZERO_STRING).convertToInt(), (int) 0);

    String stringThatDoesNotFitInInt = "2147483648";
    DatabricksSQLException outOfRangeException =
        assertThrows(
            DatabricksSQLException.class,
            () -> new StringConverter(stringThatDoesNotFitInInt).convertToInt());
    assertTrue(outOfRangeException.getMessage().contains("Invalid conversion"));
    DatabricksSQLException invalidCharactersException =
        assertThrows(
            DatabricksSQLException.class,
            () -> new StringConverter(CHARACTER_STRING).convertToInt());
    assertTrue(invalidCharactersException.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToLong() throws DatabricksSQLException {
    assertEquals(new StringConverter(NUMERICAL_STRING).convertToLong(), (long) 10);
    assertEquals(new StringConverter(NUMBERICAL_ZERO_STRING).convertToLong(), (long) 0);

    String stringThatDoesNotFitInLong = "9223372036854775808";
    DatabricksSQLException outOfRangeException =
        assertThrows(
            DatabricksSQLException.class,
            () -> new StringConverter(stringThatDoesNotFitInLong).convertToLong());
    assertTrue(outOfRangeException.getMessage().contains("Invalid conversion"));
    DatabricksSQLException invalidCharactersException =
        assertThrows(
            DatabricksSQLException.class,
            () -> new StringConverter(CHARACTER_STRING).convertToLong());
    assertTrue(invalidCharactersException.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToFloat() throws DatabricksSQLException {
    assertEquals(new StringConverter(NUMERICAL_STRING).convertToFloat(), 10f);
    assertEquals(new StringConverter(NUMBERICAL_ZERO_STRING).convertToFloat(), 0f);
    DatabricksSQLException invalidCharactersException =
        assertThrows(
            DatabricksSQLException.class,
            () -> new StringConverter(CHARACTER_STRING).convertToFloat());
    assertTrue(invalidCharactersException.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToDouble() throws DatabricksSQLException {
    assertEquals(new StringConverter(NUMERICAL_STRING).convertToDouble(), (double) 10);
    assertEquals(new StringConverter(NUMBERICAL_ZERO_STRING).convertToDouble(), (double) 0);
    DatabricksSQLException invalidCharactersException =
        assertThrows(
            DatabricksSQLException.class,
            () -> new StringConverter(CHARACTER_STRING).convertToDouble());
    assertTrue(invalidCharactersException.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToBigDecimal() throws DatabricksSQLException {
    assertEquals(new StringConverter(NUMERICAL_STRING).convertToBigDecimal(), new BigDecimal("10"));
    assertEquals(
        new StringConverter(NUMBERICAL_ZERO_STRING).convertToBigDecimal(), new BigDecimal("0"));
    DatabricksSQLException invalidCharactersException =
        assertThrows(
            DatabricksSQLException.class,
            () -> new StringConverter(CHARACTER_STRING).convertToLong());
    assertTrue(invalidCharactersException.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToBoolean() throws DatabricksSQLException {
    assertEquals(new StringConverter("1").convertToBoolean(), true);
    assertEquals(new StringConverter(NUMBERICAL_ZERO_STRING).convertToBoolean(), false);
    assertEquals(new StringConverter("true").convertToBoolean(), true);
    assertEquals(new StringConverter("false").convertToBoolean(), false);

    DatabricksSQLException invalidCharactersException =
        assertThrows(
            DatabricksSQLException.class,
            () -> new StringConverter(CHARACTER_STRING).convertToBoolean());
    assertTrue(invalidCharactersException.getMessage().contains("Invalid conversion"));

    DatabricksSQLException invalidNumberException =
        assertThrows(
            DatabricksSQLException.class,
            () -> new StringConverter(NUMERICAL_STRING).convertToBoolean());
    assertTrue(invalidNumberException.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToByteArray() throws DatabricksSQLException {
    assertTrue(
        Arrays.equals(
            new StringConverter(NUMERICAL_STRING).convertToByteArray(),
            NUMERICAL_STRING.getBytes()));
    assertTrue(
        Arrays.equals(
            new StringConverter(NUMBERICAL_ZERO_STRING).convertToByteArray(),
            NUMBERICAL_ZERO_STRING.getBytes()));
    assertTrue(
        Arrays.equals(
            new StringConverter(CHARACTER_STRING).convertToByteArray(),
            CHARACTER_STRING.getBytes()));
  }

  @Test
  public void testConvertToChar() throws DatabricksSQLException {
    assertEquals(new StringConverter(NUMBERICAL_ZERO_STRING).convertToChar(), '0');
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new StringConverter(NUMERICAL_STRING).convertToChar());
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToString() throws DatabricksSQLException {
    assertEquals(new StringConverter(NUMERICAL_STRING).convertToString(), "10");
    assertEquals(new StringConverter(NUMBERICAL_ZERO_STRING).convertToString(), "0");
    assertEquals(new StringConverter(CHARACTER_STRING).convertToString(), "ABC");
  }

  @Test
  public void testConvertToTimestamp() throws DatabricksSQLException {
    assertEquals(
        new StringConverter(TIME_STAMP_STRING).convertToTimestamp(),
        Timestamp.valueOf(TIME_STAMP_STRING));
  }

  @Test
  public void testConvertToDate() throws DatabricksSQLException {
    assertEquals(new StringConverter(DATE_STRING).convertToDate(), Date.valueOf(DATE_STRING));
  }

  @Test
  public void testConvertToBigInteger() throws DatabricksSQLException {
    assertEquals(
        new StringConverter(NUMERICAL_STRING).convertToBigInteger(),
        new BigDecimal("10").toBigInteger());
    assertEquals(
        new StringConverter(NUMBERICAL_ZERO_STRING).convertToBigInteger(),
        new BigDecimal("0").toBigInteger());
    DatabricksSQLException invalidCharactersException =
        assertThrows(
            DatabricksSQLException.class,
            () -> new StringConverter(CHARACTER_STRING).convertToBigInteger());
    assertTrue(invalidCharactersException.getMessage().contains("Invalid conversion"));
  }
}
