package com.databricks.jdbc.api.impl.converters;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.util.TimeZone;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TimestampConverterTest {
  private final Timestamp TIMESTAMP =
      Timestamp.from(
          LocalDateTime.of(2023, Month.SEPTEMBER, 10, 20, 45).atZone(ZoneId.of("UTC")).toInstant());

  @Test
  public void testTimestampInIST() throws DatabricksSQLException {
    TimeZone defaultTimeZone = TimeZone.getDefault();
    try {
      // Create a timestamp in Indian Standard Time (IST)
      TimeZone.setDefault(TimeZone.getTimeZone("Asia/Kolkata"));
      Timestamp istTimestamp =
          Timestamp.from(
              LocalDateTime.of(2023, Month.SEPTEMBER, 11, 8, 44)
                  .atZone(ZoneId.of("Asia/Kolkata"))
                  .toInstant());

      // Test that the converter stores it as UTC
      TimestampConverter converter = new TimestampConverter();
      assertEquals(
          "2023-09-11T03:14:00Z", converter.toString(istTimestamp)); // Should be converted to UTC

    } finally {
      // Restore the original timezone after the test
      TimeZone.setDefault(defaultTimeZone);
    }
  }

  @Test
  public void testConvertToLong() throws DatabricksSQLException {
    assertEquals(1694378700000L, new TimestampConverter().toLong(TIMESTAMP));
  }

  @Test
  public void testConvertToString() throws DatabricksSQLException {
    assertEquals("2023-09-10T20:45:00Z", new TimestampConverter().toString(TIMESTAMP));
  }

  @Test
  public void testConvertToTime() throws DatabricksSQLException {
    assertEquals(
        new Time(TIMESTAMP.getTime()), new TimestampConverter().toTime("2023-09-10T20:45:00Z"));
  }

  @Test
  public void testConvertFromString() throws DatabricksSQLException {
    assertEquals(TIMESTAMP, new TimestampConverter().toTimestamp("2023-09-10T20:45:00Z"));
    Assertions.assertDoesNotThrow(() -> new TimestampConverter().toString("2023-09-10 20:45:00"));
  }

  @Test
  public void testConvertToDate() throws DatabricksSQLException {
    TimestampConverter converter = new TimestampConverter();
    java.sql.Date actualDate = converter.toDate(TIMESTAMP);

    // Convert the java.sql.Date to a LocalDate
    LocalDate actualLocalDate = actualDate.toLocalDate();
    // Compute the expected LocalDate based on the system default time zone.
    LocalDate expectedLocalDate =
        TIMESTAMP.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

    assertEquals(expectedLocalDate, actualLocalDate);
  }

  @Test
  public void testConvertToBigInteger() throws DatabricksSQLException {
    assertEquals(
        BigInteger.valueOf(1694378700000L), new TimestampConverter().toBigInteger(TIMESTAMP));
  }
}
