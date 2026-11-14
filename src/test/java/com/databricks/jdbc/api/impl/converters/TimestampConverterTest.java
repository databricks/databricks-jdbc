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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
          "2023-09-11 08:44:00.0",
          converter.toString(istTimestamp)); // Should return SQL format without conversion

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
    TimeZone defaultTimeZone = TimeZone.getDefault();
    try {
      // Set timezone to UTC to ensure consistent test results
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
      assertEquals("2023-09-10 20:45:00.0", converter.toString(TIMESTAMP));
    } finally {
      // Restore the original timezone after the test
      TimeZone.setDefault(defaultTimeZone);
    }
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

  @ParameterizedTest
  @ValueSource(strings = {"UTC", "America/Los_Angeles", "America/New_York", "Asia/Kolkata"})
  public void testConvertToLong_JavaTimeAndTextMatchLegacyAfterGregorianCutover(String defaultZone)
      throws DatabricksSQLException {
    TimeZone defaultTimeZone = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone(defaultZone));
      for (String fields :
          new String[] {
            "2023-06-15T12:30:00.123456",
            "2023-11-05T01:30:00", // ambiguous in America/Los_Angeles
            "2024-03-10T02:30:00", // skipped in America/Los_Angeles
            "1950-06-01T00:00:00",
            // java.util.TimeZone and java.time disagree on these offsets (local mean time)
            "1890-01-01T00:00:00",
            "1850-01-01T12:00:00",
            "1582-10-17T00:00:00"
          }) {
        LocalDateTime localDateTime = LocalDateTime.parse(fields);
        long legacy = Timestamp.valueOf(localDateTime).getTime();
        // TIMESTAMP_NTZ and TIMESTAMP values read from Arrow
        assertEquals(legacy, converter.toLong(localDateTime), fields);
        assertEquals(
            legacy, converter.toLong(localDateTime.atOffset(ZoneOffset.ofHours(9))), fields);
        // Text without an offset, and with a non-UTC offset, keeps the rendered fields
        assertEquals(legacy, converter.toLong(fields.replace('T', ' ')), fields);
        assertEquals(legacy, converter.toLong(fields + "+05:30"), fields);
        assertEquals(
            converter.toTimestamp(fields + "+05:30").getTime(),
            converter.toLong(fields + "+05:30"),
            fields);
      }
      // Text ending in 'Z' is read as an instant
      String utcText = "2023-06-15T12:30:00.123Z";
      assertEquals(converter.toTimestamp(utcText).getTime(), converter.toLong(utcText));
      assertEquals(Instant.parse(utcText).toEpochMilli(), converter.toLong(utcText));
      // Text that toTimestamp rejects is rejected by toLong too
      assertThrows(
          DatabricksSQLException.class, () -> converter.toLong("2023-03-15 12:34:56+05:30"));
    } finally {
      TimeZone.setDefault(defaultTimeZone);
    }
  }

  @Test
  public void testConvertToLong_ExactBefore1582() throws DatabricksSQLException {
    TimeZone defaultTimeZone = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
      LocalDateTime year1000 = LocalDateTime.of(1000, 1, 1, 0, 0);
      long exact = -30610224000000L; // 1000-01-01T00:00:00Z
      // java.sql.Timestamp puts it 5 days later
      assertEquals(exact + 5 * 86_400_000L, Timestamp.valueOf(year1000).getTime());

      assertEquals(exact, converter.toLong(year1000));
      assertEquals(exact, converter.toLong(year1000.atOffset(ZoneOffset.UTC)));
      assertEquals(exact, converter.toLong("1000-01-01 00:00:00"));
      assertEquals(exact, converter.toLong("1000-01-01T00:00:00.000Z"));
      assertEquals(
          -125754422400000L, // TO_TIMESTAMP('2016 bc', 'yyyy G')
          converter.toLong(OffsetDateTime.parse("-2015-01-01T00:00:00Z")));
      assertEquals(BigInteger.valueOf(exact), converter.toBigInteger(year1000));
      // The last day of the 1582-10-05..14 gap, which java.sql.Timestamp moves 10 days later
      long lastGapDay =
          LocalDateTime.of(1582, 10, 14, 12, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
      assertEquals(
          lastGapDay + 10 * 86_400_000L, Timestamp.valueOf("1582-10-14 12:00:00").getTime());
      assertEquals(lastGapDay, converter.toLong("1582-10-14 12:00:00"));
      assertEquals(lastGapDay, converter.toLong(LocalDateTime.of(1582, 10, 14, 12, 0)));
    } finally {
      TimeZone.setDefault(defaultTimeZone);
    }
  }

  @Test
  public void testConvertToLong_ExactBefore1582InNonUtcZone() throws DatabricksSQLException {
    TimeZone defaultTimeZone = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
      LocalDateTime year1000 = LocalDateTime.of(1000, 1, 1, 0, 0);
      long expected = year1000.atZone(ZoneId.of("America/Los_Angeles")).toInstant().toEpochMilli();

      assertEquals(expected, converter.toLong(year1000));
      assertEquals(expected, converter.toLong("1000-01-01 00:00:00"));
      // Text ending in 'Z' stays an instant
      assertEquals(-30610224000000L, converter.toLong("1000-01-01T00:00:00.000Z"));
    } finally {
      TimeZone.setDefault(defaultTimeZone);
    }
  }
}
