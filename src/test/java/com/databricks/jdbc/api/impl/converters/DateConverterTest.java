package com.databricks.jdbc.api.impl.converters;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import org.junit.jupiter.api.Test;

public class DateConverterTest {
  private final Date DATE = Date.valueOf("2023-09-10");

  @Test
  public void testConvertToShort() throws DatabricksSQLException {
    assertEquals(new DateConverter().toShort(DATE), 19610);

    Date dateDoesNotFitInShort = Date.valueOf("5050-12-31");
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class, () -> new DateConverter().toShort(dateDoesNotFitInShort));
    assertTrue(exception.getMessage().contains("Invalid conversion"));
  }

  @Test
  public void testConvertToInt() throws DatabricksSQLException {
    assertEquals(new DateConverter().toInt(DATE), 19610);
  }

  @Test
  public void testConvertToLong() throws DatabricksSQLException {
    assertEquals(new DateConverter().toLong(DATE), 19610L);
  }

  @Test
  public void testConvertToString() throws DatabricksSQLException {
    assertEquals(new DateConverter().toString(DATE), "2023-09-10");
  }

  @Test
  public void testConvertFromString() throws DatabricksSQLException {
    assertEquals(new DateConverter().toDate("2023-09-10"), DATE);
  }

  @Test
  public void testConvertToTimestamp() throws DatabricksSQLException {
    assertEquals(new DateConverter().toTimestamp(DATE), Timestamp.valueOf("2023-09-10 00:00:00"));
  }

  @Test
  public void testConvertToDate() throws DatabricksSQLException {
    assertEquals(new DateConverter().toDate(DATE), Date.valueOf("2023-09-10"));
  }

  @Test
  public void testConvertToLocalDate() throws DatabricksSQLException {
    assertEquals(new DateConverter().toLocalDate(DATE), DATE.toLocalDate());
  }

  @Test
  public void testConvertToBigInteger() throws DatabricksSQLException {
    assertEquals(
        new DateConverter().toBigInteger(DATE),
        BigInteger.valueOf(DATE.toLocalDate().toEpochDay()));
  }
}
