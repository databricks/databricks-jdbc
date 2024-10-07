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
    assertEquals(new StringConverter().toByte(singleCharacterString), (byte) 'A');

    DatabricksSQLException tooManyCharactersException =
        assertThrows(
            DatabricksSQLException.class, () -> new StringConverter().toByte(CHARACTER_STRING));
    assertTrue(tooManyCharactersException.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToShort() throws DatabricksSQLException {
    assertEquals(new StringConverter().toShort(NUMERICAL_STRING), (short) 10);
    assertEquals(new StringConverter().toShort(NUMBERICAL_ZERO_STRING), (short) 0);

    String stringThatDoesNotFitInShort = "32768";
    DatabricksSQLException outOfRangeException =
        assertThrows(
            DatabricksSQLException.class,
            () -> new StringConverter().toShort(stringThatDoesNotFitInShort));
    assertTrue(outOfRangeException.getMessage().contains("Invalid conversion"));
    DatabricksSQLException invalidCharactersException =
        assertThrows(
            DatabricksSQLException.class, () -> new StringConverter().toShort(CHARACTER_STRING));
    assertTrue(invalidCharactersException.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToInt() throws DatabricksSQLException {
    assertEquals(new StringConverter().toInt(NUMERICAL_STRING), 10);
    assertEquals(new StringConverter().toInt(NUMBERICAL_ZERO_STRING), 0);

    String stringThatDoesNotFitInInt = "2147483648";
    DatabricksSQLException outOfRangeException =
        assertThrows(
            DatabricksSQLException.class,
            () -> new StringConverter().toInt(stringThatDoesNotFitInInt));
    assertTrue(outOfRangeException.getMessage().contains("Invalid conversion"));
    DatabricksSQLException invalidCharactersException =
        assertThrows(
            DatabricksSQLException.class, () -> new StringConverter().toInt(CHARACTER_STRING));
    assertTrue(invalidCharactersException.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToLong() throws DatabricksSQLException {
    assertEquals(new StringConverter().toLong(NUMERICAL_STRING), 10);
    assertEquals(new StringConverter().toLong(NUMBERICAL_ZERO_STRING), 0);

    String stringThatDoesNotFitInLong = "9223372036854775808";
    DatabricksSQLException outOfRangeException =
        assertThrows(
            DatabricksSQLException.class,
            () -> new StringConverter().toLong(stringThatDoesNotFitInLong));
    assertTrue(outOfRangeException.getMessage().contains("Invalid conversion"));
    DatabricksSQLException invalidCharactersException =
        assertThrows(
            DatabricksSQLException.class, () -> new StringConverter().toLong(CHARACTER_STRING));
    assertTrue(invalidCharactersException.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToFloat() throws DatabricksSQLException {
    assertEquals(new StringConverter().toFloat(NUMERICAL_STRING), 10f);
    assertEquals(new StringConverter().toFloat(NUMBERICAL_ZERO_STRING), 0f);
    DatabricksSQLException invalidCharactersException =
        assertThrows(
            DatabricksSQLException.class, () -> new StringConverter().toFloat(CHARACTER_STRING));
    assertTrue(invalidCharactersException.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToDouble() throws DatabricksSQLException {
    assertEquals(new StringConverter().toDouble(NUMERICAL_STRING), 10);
    assertEquals(new StringConverter().toDouble(NUMBERICAL_ZERO_STRING), 0);
    DatabricksSQLException invalidCharactersException =
        assertThrows(
            DatabricksSQLException.class, () -> new StringConverter().toDouble(CHARACTER_STRING));
    assertTrue(invalidCharactersException.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToBigDecimal() throws DatabricksSQLException {
    assertEquals(new StringConverter().toBigDecimal(NUMERICAL_STRING), new BigDecimal("10"));
    assertEquals(new StringConverter().toBigDecimal(NUMBERICAL_ZERO_STRING), new BigDecimal("0"));
    DatabricksSQLException invalidCharactersException =
        assertThrows(
            DatabricksSQLException.class, () -> new StringConverter().toLong(CHARACTER_STRING));
    assertTrue(invalidCharactersException.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToBoolean() throws DatabricksSQLException {
    assertTrue(new StringConverter().toBoolean("1"));
    assertFalse(new StringConverter().toBoolean(NUMBERICAL_ZERO_STRING));
    assertTrue(new StringConverter().toBoolean("true"));
    assertFalse(new StringConverter().toBoolean("false"));

    DatabricksSQLException invalidCharactersException =
        assertThrows(
            DatabricksSQLException.class, () -> new StringConverter().toBoolean(CHARACTER_STRING));
    assertTrue(invalidCharactersException.getMessage().contains("Invalid conversion"));

    DatabricksSQLException invalidNumberException =
        assertThrows(
            DatabricksSQLException.class, () -> new StringConverter().toBoolean(NUMERICAL_STRING));
    assertTrue(invalidNumberException.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToByteArray() throws DatabricksSQLException {
    assertArrayEquals(
        new StringConverter().toByteArray(NUMERICAL_STRING), NUMERICAL_STRING.getBytes());
    assertArrayEquals(
        new StringConverter().toByteArray(NUMBERICAL_ZERO_STRING),
        NUMBERICAL_ZERO_STRING.getBytes());
    assertArrayEquals(
        new StringConverter().toByteArray(CHARACTER_STRING), CHARACTER_STRING.getBytes());
  }

  @Test
  public void testConvertToChar() throws DatabricksSQLException {
    assertEquals(new StringConverter().toChar(NUMBERICAL_ZERO_STRING), '0');
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new StringConverter().toChar(NUMERICAL_STRING));
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToString() throws DatabricksSQLException {
    assertEquals(new StringConverter().toString(NUMERICAL_STRING), "10");
    assertEquals(new StringConverter().toString(NUMBERICAL_ZERO_STRING), "0");
    assertEquals(new StringConverter().toString(CHARACTER_STRING), "ABC");
  }

  @Test
  public void testConvertToTimestamp() throws DatabricksSQLException {
    assertEquals(
        new StringConverter().toTimestamp(TIME_STAMP_STRING), Timestamp.valueOf(TIME_STAMP_STRING));
  }

  @Test
  public void testConvertToDate() throws DatabricksSQLException {
    assertEquals(new StringConverter().toDate(DATE_STRING), Date.valueOf(DATE_STRING));
  }

  @Test
  public void testConvertToBigInteger() throws DatabricksSQLException {
    assertEquals(
        new StringConverter().toBigInteger(NUMERICAL_STRING), new BigDecimal("10").toBigInteger());
    assertEquals(
        new StringConverter().toBigInteger(NUMBERICAL_ZERO_STRING),
        new BigDecimal("0").toBigInteger());
    DatabricksSQLException invalidCharactersException =
        assertThrows(
            DatabricksSQLException.class,
            () -> new StringConverter().toBigInteger(CHARACTER_STRING));
    assertTrue(invalidCharactersException.getMessage().contains("Invalid conversion"));
  }
}
