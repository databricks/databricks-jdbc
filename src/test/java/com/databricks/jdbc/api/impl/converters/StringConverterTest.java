package com.databricks.jdbc.api.impl.converters;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import org.junit.jupiter.api.Test;

public class StringConverterTest {

  private final String NUMERICAL_STRING = "10";
  private final String NUMBERICAL_ZERO_STRING = "0";
  private final String CHARACTER_STRING = "ABC";
  private final String TIME_STAMP_STRING = "2023-09-10 00:00:00";
  private final String DATE_STRING = "2023-09-10";

  @Test
  public void testConvertToByte() throws DatabricksSQLException {
    String singleCharacterString = "A";
    assertEquals(new StringConverter(null).toByte(singleCharacterString), (byte) 'A');

    DatabricksSQLException tooManyCharactersException =
        assertThrows(
            DatabricksSQLException.class, () -> new StringConverter(null).toByte(CHARACTER_STRING));
    assertTrue(tooManyCharactersException.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToShort() throws DatabricksSQLException {
    assertEquals(new StringConverter(null).toShort(NUMERICAL_STRING), (short) 10);
    assertEquals(new StringConverter(null).toShort(NUMBERICAL_ZERO_STRING), (short) 0);

    String stringThatDoesNotFitInShort = "32768";
    DatabricksSQLException outOfRangeException =
        assertThrows(
            DatabricksSQLException.class,
            () -> new StringConverter(null).toShort(stringThatDoesNotFitInShort));
    assertTrue(outOfRangeException.getMessage().contains("Invalid conversion"));
    DatabricksSQLException invalidCharactersException =
        assertThrows(
            DatabricksSQLException.class,
            () -> new StringConverter(null).toShort(CHARACTER_STRING));
    assertTrue(invalidCharactersException.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToInt() throws DatabricksSQLException {
    assertEquals(new StringConverter(null).toInt(NUMERICAL_STRING), 10);
    assertEquals(new StringConverter(null).toInt(NUMBERICAL_ZERO_STRING), 0);

    String stringThatDoesNotFitInInt = "2147483648";
    DatabricksSQLException outOfRangeException =
        assertThrows(
            DatabricksSQLException.class,
            () -> new StringConverter(null).toInt(stringThatDoesNotFitInInt));
    assertTrue(outOfRangeException.getMessage().contains("Invalid conversion"));
    DatabricksSQLException invalidCharactersException =
        assertThrows(
            DatabricksSQLException.class, () -> new StringConverter(null).toInt(CHARACTER_STRING));
    assertTrue(invalidCharactersException.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToLong() throws DatabricksSQLException {
    assertEquals(new StringConverter(null).toLong(NUMERICAL_STRING), 10);
    assertEquals(new StringConverter(null).toLong(NUMBERICAL_ZERO_STRING), 0);

    String stringThatDoesNotFitInLong = "9223372036854775808";
    DatabricksSQLException outOfRangeException =
        assertThrows(
            DatabricksSQLException.class,
            () -> new StringConverter(null).toLong(stringThatDoesNotFitInLong));
    assertTrue(outOfRangeException.getMessage().contains("Invalid conversion"));
    DatabricksSQLException invalidCharactersException =
        assertThrows(
            DatabricksSQLException.class, () -> new StringConverter(null).toLong(CHARACTER_STRING));
    assertTrue(invalidCharactersException.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToFloat() throws DatabricksSQLException {
    assertEquals(new StringConverter(null).toFloat(NUMERICAL_STRING), 10f);
    assertEquals(new StringConverter(null).toFloat(NUMBERICAL_ZERO_STRING), 0f);
    DatabricksSQLException invalidCharactersException =
        assertThrows(
            DatabricksSQLException.class,
            () -> new StringConverter(null).toFloat(CHARACTER_STRING));
    assertTrue(invalidCharactersException.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToDouble() throws DatabricksSQLException {
    assertEquals(new StringConverter(null).toDouble(NUMERICAL_STRING), 10);
    assertEquals(new StringConverter(null).toDouble(NUMBERICAL_ZERO_STRING), 0);
    DatabricksSQLException invalidCharactersException =
        assertThrows(
            DatabricksSQLException.class,
            () -> new StringConverter(null).toDouble(CHARACTER_STRING));
    assertTrue(invalidCharactersException.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToBigDecimal() throws DatabricksSQLException {
    assertEquals(new StringConverter(null).toBigDecimal(NUMERICAL_STRING), new BigDecimal("10"));
    assertEquals(
        new StringConverter(null).toBigDecimal(NUMBERICAL_ZERO_STRING), new BigDecimal("0"));
    DatabricksSQLException invalidCharactersException =
        assertThrows(
            DatabricksSQLException.class, () -> new StringConverter(null).toLong(CHARACTER_STRING));
    assertTrue(invalidCharactersException.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToBoolean() throws DatabricksSQLException {
    assertTrue(new StringConverter(null).toBoolean("1"));
    assertFalse(new StringConverter(null).toBoolean(NUMBERICAL_ZERO_STRING));
    assertTrue(new StringConverter(null).toBoolean("true"));
    assertFalse(new StringConverter(null).toBoolean("false"));

    DatabricksSQLException invalidCharactersException =
        assertThrows(
            DatabricksSQLException.class,
            () -> new StringConverter(null).toBoolean(CHARACTER_STRING));
    assertTrue(invalidCharactersException.getMessage().contains("Invalid conversion"));

    DatabricksSQLException invalidNumberException =
        assertThrows(
            DatabricksSQLException.class,
            () -> new StringConverter(null).toBoolean(NUMERICAL_STRING));
    assertTrue(invalidNumberException.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToByteArray() throws DatabricksSQLException {
    assertArrayEquals(
        new StringConverter(null).toByteArray(NUMERICAL_STRING), NUMERICAL_STRING.getBytes());
    assertArrayEquals(
        new StringConverter(null).toByteArray(NUMBERICAL_ZERO_STRING),
        NUMBERICAL_ZERO_STRING.getBytes());
    assertArrayEquals(
        new StringConverter(null).toByteArray(CHARACTER_STRING), CHARACTER_STRING.getBytes());
  }

  @Test
  public void testConvertToChar() throws DatabricksSQLException {
    assertEquals(new StringConverter(null).toChar(NUMBERICAL_ZERO_STRING), '0');
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new StringConverter(null).toChar(NUMERICAL_STRING));
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToString() throws DatabricksSQLException {
    assertEquals(new StringConverter(null).toString(NUMERICAL_STRING), "10");
    assertEquals(new StringConverter(null).toString(NUMBERICAL_ZERO_STRING), "0");
    assertEquals(new StringConverter(null).toString(CHARACTER_STRING), "ABC");
  }

  @Test
  public void testConvertToTimestamp() throws DatabricksSQLException {
    assertEquals(
        new StringConverter(null).toTimestamp(TIME_STAMP_STRING),
        Timestamp.valueOf(TIME_STAMP_STRING));
  }

  @Test
  public void testConvertToDate() throws DatabricksSQLException {
    assertEquals(new StringConverter(null).toDate(DATE_STRING), Date.valueOf(DATE_STRING));
  }

  @Test
  public void testConvertToBigInteger() throws DatabricksSQLException {
    assertEquals(
        new StringConverter(null).toBigInteger(NUMERICAL_STRING),
        new BigDecimal("10").toBigInteger());
    assertEquals(
        new StringConverter(null).toBigInteger(NUMBERICAL_ZERO_STRING),
        new BigDecimal("0").toBigInteger());
    DatabricksSQLException invalidCharactersException =
        assertThrows(
            DatabricksSQLException.class,
            () -> new StringConverter(null).toBigInteger(CHARACTER_STRING));
    assertTrue(invalidCharactersException.getMessage().contains("Invalid conversion"));
  }
}
