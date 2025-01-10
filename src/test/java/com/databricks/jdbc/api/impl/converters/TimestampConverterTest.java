package com.databricks.jdbc.api.impl.converters;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigInteger;
import java.sql.Timestamp;
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
      TimestampConverter converter = new TimestampConverter(null);
      assertEquals(
          converter.toString(istTimestamp), "2023-09-11T03:14:00Z"); // Should be converted to UTC

    } finally {
      // Restore the original timezone after the test
      TimeZone.setDefault(defaultTimeZone);
    }
  }

  @Test
  public void testConvertToLong() throws DatabricksSQLException {
    assertEquals(new TimestampConverter(null).toLong(TIMESTAMP), 1694378700000L);
  }

  @Test
  public void testConvertToString() throws DatabricksSQLException {
    assertEquals(new TimestampConverter(null).toString(TIMESTAMP), "2023-09-10T20:45:00Z");
  }

  @Test
  public void testConvertFromString() throws DatabricksSQLException {
    assertEquals(new TimestampConverter(null).toTimestamp("2023-09-10T20:45:00Z"), TIMESTAMP);
    Assertions.assertDoesNotThrow(
        () -> new TimestampConverter(null).toString("2023-09-10 20:45:00"));
  }

  @Test
  public void testConvertToDate() throws DatabricksSQLException {
    assertEquals(new TimestampConverter(null).toDate(TIMESTAMP).toString(), "2023-09-10");
  }

  @Test
  public void testConvertToBigInteger() throws DatabricksSQLException {
    assertEquals(
        new TimestampConverter(null).toBigInteger(TIMESTAMP), BigInteger.valueOf(1694378700000L));
  }
}
