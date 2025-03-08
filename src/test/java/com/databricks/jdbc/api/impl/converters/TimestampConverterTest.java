package com.databricks.jdbc.api.impl.converters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;
import java.util.TimeZone;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TimestampConverterTest {
  private final Timestamp TIMESTAMP =
      Timestamp.from(
          LocalDateTime.of(2023, Month.SEPTEMBER, 10, 20, 45).atZone(ZoneId.of("UTC")).toInstant());

  private TimestampConverter converter;

  @BeforeEach
  public void setUp() {
    converter = new TimestampConverter();
  }

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

      assertEquals(
          "2023-09-11T03:14:00Z", converter.toString(istTimestamp)); // Should be converted to UTC

    } finally {
      // Restore the original timezone after the test
      TimeZone.setDefault(defaultTimeZone);
    }
  }

  @Test
  public void testToTimestamp() throws DatabricksSQLException {
    // Case 1: Input is already a Timestamp.
    Timestamp ts = Timestamp.valueOf("2023-03-15 12:34:56");
    assertSame(ts, converter.toTimestamp(ts));

    // Case 2: Input is a String with a timezone offset.
    // Example: "2023-03-15T12:34:56+05:30"
    String tsWithOffset = "2023-03-15T12:34:56+05:30";
    Timestamp expectedWithOffset =
        Timestamp.valueOf(OffsetDateTime.parse(tsWithOffset).toLocalDateTime()); // UTC
    assertEquals(expectedWithOffset, converter.toTimestamp(tsWithOffset));

    // Case 3: Input is a String without a timezone offset.
    // Example: "2023-03-15T12:34:56" becomes "2023-03-15 12:34:56" for Timestamp.valueOf.
    String tsWithoutOffset = "2023-03-15T12:34:56";
    Timestamp expectedWithoutOffset = Timestamp.valueOf("2023-03-15 12:34:56");
    assertEquals(expectedWithoutOffset, converter.toTimestamp(tsWithoutOffset));

    // Case 4: Fallback parsing via Instant.parse.
    // Example: "2023-03-15T12:34:56Z" (Z denotes UTC).
    String tsFallback = "2023-03-15T12:34:56Z";
    Timestamp expectedFallback =
        new Timestamp(Instant.parse("2023-03-15T12:34:56Z").toEpochMilli());
    assertEquals(expectedFallback, converter.toTimestamp(tsFallback));

    // Case 5: Invalid String should throw DatabricksSQLException.
    String invalidTs = "not-a-timestamp";
    assertThrows(DatabricksSQLException.class, () -> converter.toTimestamp(invalidTs));

    // Case 6: Unsupported type (e.g., Integer) should throw DatabricksSQLException.
    Integer unsupportedInput = 12345;
    assertThrows(DatabricksSQLException.class, () -> converter.toTimestamp(unsupportedInput));
  }

  @Test
  public void testConvertToLong() throws DatabricksSQLException {
    assertEquals(1694378700000L, converter.toLong(TIMESTAMP));
  }

  @Test
  public void testConvertToString() throws DatabricksSQLException {
    assertEquals("2023-09-10T20:45:00Z", converter.toString(TIMESTAMP));
  }

  @Test
  public void testConvertToTime() throws DatabricksSQLException {
    assertEquals(
        Time.valueOf(TIMESTAMP.toLocalDateTime().toLocalTime()),
        converter.toTime("2023-09-10T20:45:00Z"));
  }

  @Test
  public void testConvertFromString() throws DatabricksSQLException {
    assertEquals(TIMESTAMP, converter.toTimestamp("2023-09-10T20:45:00Z"));
    Assertions.assertDoesNotThrow(() -> converter.toString("2023-09-10 20:45:00"));
  }

  @Test
  public void testToDate() throws DatabricksSQLException {
    // Case 1: Input is already a Date.
    Date date = new Date(System.currentTimeMillis());
    assertSame(date, converter.toDate(date));

    //     Case 2: Input is a Timestamp.
    Timestamp ts = Timestamp.valueOf("2023-03-15 12:34:56");
    Date expectedFromTimestamp = new Date(ts.getTime());
    assertEquals(expectedFromTimestamp, converter.toDate(ts));

    // Case 3: Input is a String with a timezone offset.
    // Example: "2023-03-15T12:34:56+05:30"
    String dateWithOffset = "2023-03-15T12:34:56+05:30";
    Date expectedWithOffset = Date.valueOf(OffsetDateTime.parse(dateWithOffset).toLocalDate());
    Date actual = converter.toDate(dateWithOffset);
    assertEquals(expectedWithOffset, actual);

    // Case 4: Input is a String without offset containing 'T'.
    // Example: "2023-03-15T12:34:56" will have 'T' replaced by a space.
    String dateWithoutOffsetT = "2023-03-15T12:34:56";
    Date expectedWithoutOffset = Date.valueOf("2023-03-15");
    assertEquals(expectedWithoutOffset, converter.toDate(dateWithoutOffsetT));

    // Case 5: Input is a String without offset containing a space.
    // Example: "2023-03-15 12:34:56"
    String dateWithoutOffsetSpace = "2023-03-15 12:34:56";
    assertEquals(expectedWithoutOffset, converter.toDate(dateWithoutOffsetSpace));

    // Case 6: Invalid date string should throw DatabricksSQLException.
    String invalidDate = "invalid-date";
    assertThrows(DatabricksSQLException.class, () -> converter.toDate(invalidDate));
  }

  @Test
  public void testConvertToBigInteger() throws DatabricksSQLException {
    assertEquals(BigInteger.valueOf(1694378700000L), converter.toBigInteger(TIMESTAMP));
  }
}
