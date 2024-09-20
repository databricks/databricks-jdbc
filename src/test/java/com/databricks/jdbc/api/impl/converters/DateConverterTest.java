package com.databricks.jdbc.api.impl.converters;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import org.junit.jupiter.api.Test;

public class DateConverterTest {

  private final Date DATE = Date.valueOf("2023-09-10");

  @Test
  public void testConvertToShort() throws DatabricksSQLException {
    assertEquals(new DateConverter(DATE).convertToShort(), 19610);

    Date dateDoesNotFitInShort = Date.valueOf("5050-12-31");
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> new DateConverter(dateDoesNotFitInShort).convertToShort());
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToInt() throws DatabricksSQLException {
    assertEquals(new DateConverter(DATE).convertToInt(), 19610);
  }

  @Test
  public void testConvertToLong() throws DatabricksSQLException {
    assertEquals(new DateConverter(DATE).convertToLong(), 19610L);
  }

  @Test
  public void testConvertToString() throws DatabricksSQLException {
    assertEquals(new DateConverter(DATE).convertToString(), "2023-09-10");
  }

  @Test
  public void testConvertFromString() throws DatabricksSQLException {
    assertEquals(new DateConverter("2023-09-10").convertToDate(), DATE);
  }

  @Test
  public void testConvertToTimestamp() throws DatabricksSQLException {
    assertEquals(
        new DateConverter(DATE).convertToTimestamp(), Timestamp.valueOf("2023-09-10 00:00:00"));
  }

  @Test
  public void testConvertToDate() throws DatabricksSQLException {
    assertEquals(
        new DateConverter(DATE).convertToDate(),
        new Date(Timestamp.valueOf("2023-09-10 00:00:00").getTime()));
  }

  @Test
  public void testConvertToLocalDate() throws DatabricksSQLException {
    assertEquals(new DateConverter(DATE).convertToLocalDate(), DATE.toLocalDate());
  }

  @Test
  public void testConvertToBigInteger() throws DatabricksSQLException {
    assertEquals(
        new DateConverter(DATE).convertToBigInteger(),
        BigInteger.valueOf(DATE.toLocalDate().toEpochDay()));
  }
}
