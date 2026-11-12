package com.databricks.jdbc.api.impl.converters;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

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

  @ParameterizedTest
  @CsvSource({
    "-2015-01-01, -1455491", // 2016 BC
    "1582-10-05, -141437", // inside the Julian/Gregorian cutover gap
    "1582-10-14, -141428", // last day of the gap
    "1000-01-01, -354285",
    "2023-09-10, 19610"
  })
  public void testConvertToLong_ExactForJavaTimeAndText(String date, long epochDay)
      throws DatabricksSQLException {
    DateConverter converter = new DateConverter();
    // The LocalDate read from Arrow and the server's text skip java.sql.Date.
    assertEquals(epochDay, converter.toLong(LocalDate.parse(date)));
    assertEquals(epochDay, converter.toLong(date));
    assertEquals((int) epochDay, converter.toInt(LocalDate.parse(date)));
    assertEquals(BigInteger.valueOf(epochDay), converter.toBigInteger(LocalDate.parse(date)));
  }

  @Test
  public void testConvertToLong_LegacyDateUnchanged() throws DatabricksSQLException {
    // A java.sql.Date has already lost the value; it keeps its existing result.
    assertEquals(-141427L, new DateConverter().toLong(Date.valueOf(LocalDate.of(1582, 10, 5))));
    // Non-padded text is read on the proleptic calendar too.
    assertEquals(19610L, new DateConverter().toLong("2023-9-10"));
    assertEquals(-141437L, new DateConverter().toLong("1582-10-5"));
  }
}
