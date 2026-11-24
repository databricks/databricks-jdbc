package com.databricks.jdbc.common.util;

import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

public final class TemporalParameterTestData {
  private TemporalParameterTestData() {}

  public static Stream<Arguments> values() {
    return Stream.concat(
        signedValues(),
        legacyValues()
            .map(
                value ->
                    Arguments.of(
                        value,
                        DatabricksTypeUtil.getColumnInfoType(
                            DatabricksTypeUtil.inferDatabricksType(value)),
                        value instanceof ZonedDateTime
                            ? ((ZonedDateTime) value).toOffsetDateTime().toString()
                            : value.toString())));
  }

  public static Stream<Arguments> explicitOffsetValues() {
    return Stream.of("2024-01-01T01:00:00+05:00", "2024-06-30T23:59:59.999999-03:00")
        .map(OffsetDateTime::parse)
        .flatMap(
            value ->
                Stream.of(
                        ColumnInfoTypeName.DATE,
                        ColumnInfoTypeName.STRING,
                        ColumnInfoTypeName.TIMESTAMP)
                    .map(type -> Arguments.of(value, type, value.toString())));
  }

  private static Stream<Arguments> signedValues() {
    return Stream.of(
        timestamp(
            OffsetDateTime.parse("2026-01-02T03:04:05.123456+05:45"),
            "2026-01-02T03:04:05.123456+05:45"),
        timestamp(
            OffsetDateTime.parse("2026-01-02T03:04:05.123456-07:00"),
            "2026-01-02T03:04:05.123456-07:00"),
        timestamp(
            OffsetDateTime.parse("0000-01-02T03:04:05.123456+00:00:01"),
            "0000-01-02T03:04:05.123456+00:00:01"),
        timestamp(
            OffsetDateTime.parse("-0100-01-02T03:04:05.123456+18:00"),
            "-0100-01-02T03:04:05.123456+18:00"),
        timestamp(
            OffsetDateTime.parse("1000-01-02T03:04:05.123456-18:00"),
            "1000-01-02T03:04:05.123456-18:00"),
        timestamp(
            OffsetDateTime.parse("+10000-01-02T03:04:05.123456Z"), "+10000-01-02T03:04:05.123456Z"),
        timestamp(legacyTimestamp(-100), "-0100-01-02 03:04:05.123456789"),
        timestamp(legacyTimestamp(0), "0000-01-02 03:04:05.123456789"),
        timestamp(legacyTimestamp(10000), "+10000-01-02 03:04:05.123456789"),
        date(new Date(legacyTimestamp(-100).getTime()), "-0100-01-02"),
        date(new Date(legacyTimestamp(0).getTime()), "0000-01-02"),
        date(new Date(legacyTimestamp(10000).getTime()), "+10000-01-02"),
        date(LocalDate.of(-100, 1, 2), "-0100-01-02"),
        date(LocalDate.of(10000, 1, 2), "+10000-01-02"),
        timestamp(
            LocalDateTime.parse("-0100-01-02T03:04:05.123456"), "-0100-01-02T03:04:05.123456"),
        timestamp(LocalDateTime.parse("+10000-01-02T03:04"), "+10000-01-02T03:04"),
        timestamp(LocalDateTime.parse("0000-01-02T03:04"), "0000-01-02T03:04"),
        timestamp(
            ZonedDateTime.parse("+10000-01-02T03:04+01:00[Europe/Paris]"),
            "+10000-01-02T03:04+01:00"),
        timestamp(
            ZonedDateTime.parse("2026-01-02T03:04:05.123456-05:00[America/New_York]"),
            "2026-01-02T03:04:05.123456-05:00"),
        timestamp(Instant.parse("-0100-01-02T03:04:05.123456Z"), "-0100-01-02T03:04:05.123456Z"),
        timestamp(null, null));
  }

  public static Stream<Object> legacyValues() {
    return Stream.of(1, 1000, 1582, 2026, 9999)
        .flatMap(
            year -> {
              Timestamp timestamp = legacyTimestamp(year);
              LocalDateTime local = LocalDateTime.of(year, 1, 2, 3, 4, 5, 123456789);
              return Stream.of(
                  timestamp,
                  new Date(timestamp.getTime()),
                  local,
                  local.toLocalDate(),
                  local.withSecond(0).withNano(0),
                  local.withNano(0),
                  local.toInstant(ZoneOffset.UTC),
                  local.atZone(ZoneId.of("America/New_York")));
            });
  }

  private static Arguments timestamp(Object value, String expected) {
    return Arguments.of(value, ColumnInfoTypeName.TIMESTAMP, expected);
  }

  private static Arguments date(Object value, String expected) {
    return Arguments.of(value, ColumnInfoTypeName.DATE, expected);
  }

  private static Timestamp legacyTimestamp(int year) {
    Calendar calendar = new GregorianCalendar();
    calendar.clear();
    calendar.set(Calendar.ERA, year <= 0 ? GregorianCalendar.BC : GregorianCalendar.AD);
    calendar.set(Calendar.YEAR, year <= 0 ? 1 - year : year);
    calendar.set(Calendar.MONTH, Calendar.JANUARY);
    calendar.set(Calendar.DAY_OF_MONTH, 2);
    calendar.set(Calendar.HOUR_OF_DAY, 3);
    calendar.set(Calendar.MINUTE, 4);
    calendar.set(Calendar.SECOND, 5);
    Timestamp timestamp = new Timestamp(calendar.getTimeInMillis());
    timestamp.setNanos(123456789);
    return timestamp;
  }
}
