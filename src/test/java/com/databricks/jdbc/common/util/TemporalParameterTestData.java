package com.databricks.jdbc.common.util;

import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

public final class TemporalParameterTestData {
  private TemporalParameterTestData() {}

  public static Stream<Arguments> values() {
    return Stream.concat(signedValues(), legacyValues());
  }

  public static Stream<Arguments> bindingMatrix() {
    return matrixValues()
        .flatMap(
            row ->
                Stream.<Integer>of(
                        null,
                        Types.DATE,
                        Types.VARCHAR,
                        Types.TIMESTAMP,
                        Types.TIMESTAMP_WITH_TIMEZONE)
                    .map(
                        target -> {
                          ColumnInfoTypeName type =
                              target == null
                                  ? row.inferredType
                                  : target == Types.DATE
                                      ? ColumnInfoTypeName.DATE
                                      : target == Types.VARCHAR
                                          ? ColumnInfoTypeName.STRING
                                          : ColumnInfoTypeName.TIMESTAMP;
                          String text =
                              type == ColumnInfoTypeName.TIMESTAMP ? row.timestampText : row.text;
                          return Arguments.of(row.value, target, type, text);
                        }));
  }

  public static Stream<Arguments> targetValues() {
    return bindingMatrix()
        .map(
            arguments -> {
              Object[] row = arguments.get();
              return Arguments.of(row[0], row[2], row[3]);
            });
  }

  public static Stream<Arguments> interpolatedTargetValues() {
    return targetValues().filter(arguments -> arguments.get()[1] != ColumnInfoTypeName.STRING);
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
            "+10000-01-02T03:04:00+01:00"),
        timestamp(
            ZonedDateTime.parse("2026-01-02T03:04:05.123456-05:00[America/New_York]"),
            "2026-01-02T03:04:05.123456-05:00"),
        timestamp(Instant.parse("-0100-01-02T03:04:05.123456Z"), "-0100-01-02T03:04:05.123456Z"),
        timestamp(null, null));
  }

  private static Stream<Arguments> legacyValues() {
    return Stream.of(
            legacyYear(
                1,
                "0001-01-02",
                "0001-01-02 03:04:05.123456789",
                "0001-01-02T03:04:05.123456789",
                "0001-01-02T03:04:05.123456789Z",
                "0001-01-02T03:04:05.123456789-04:56:02"),
            legacyYear(
                1000,
                "1000-01-02",
                "1000-01-02 03:04:05.123456789",
                "1000-01-02T03:04:05.123456789",
                "1000-01-02T03:04:05.123456789Z",
                "1000-01-02T03:04:05.123456789-04:56:02"),
            legacyYear(
                1582,
                "1582-01-02",
                "1582-01-02 03:04:05.123456789",
                "1582-01-02T03:04:05.123456789",
                "1582-01-02T03:04:05.123456789Z",
                "1582-01-02T03:04:05.123456789-04:56:02"),
            legacyYear(
                2026,
                "2026-01-02",
                "2026-01-02 03:04:05.123456789",
                "2026-01-02T03:04:05.123456789",
                "2026-01-02T03:04:05.123456789Z",
                "2026-01-02T03:04:05.123456789-05:00"),
            legacyYear(
                9999,
                "9999-01-02",
                "9999-01-02 03:04:05.123456789",
                "9999-01-02T03:04:05.123456789",
                "9999-01-02T03:04:05.123456789Z",
                "9999-01-02T03:04:05.123456789-05:00"))
        .flatMap(stream -> stream);
  }

  private static Stream<Arguments> legacyYear(
      int year,
      String dateText,
      String timestampText,
      String localText,
      String instantText,
      String zonedText) {
    Timestamp timestamp = legacyTimestamp(year);
    LocalDateTime local = LocalDateTime.of(year, 1, 2, 3, 4, 5, 123456789);
    return Stream.of(
        timestamp(timestamp, timestampText),
        date(new Date(timestamp.getTime()), dateText),
        date(local.toLocalDate(), dateText),
        timestamp(local, localText),
        timestamp(local.toInstant(ZoneOffset.UTC), instantText),
        timestamp(local.atZone(ZoneId.of("America/New_York")), zonedText));
  }

  private static Stream<TemporalValue> matrixValues() {
    return Stream.of(
        new TemporalValue(
            Timestamp.valueOf("2024-01-01 01:00:00"),
            ColumnInfoTypeName.TIMESTAMP,
            "2024-01-01 01:00:00.0"),
        new TemporalValue(Date.valueOf("2024-01-01"), ColumnInfoTypeName.DATE, "2024-01-01"),
        new TemporalValue(LocalDate.of(2024, 1, 1), ColumnInfoTypeName.DATE, "2024-01-01"),
        new TemporalValue(
            LocalDateTime.of(2024, 1, 1, 1, 0), ColumnInfoTypeName.TIMESTAMP, "2024-01-01T01:00"),
        new TemporalValue(
            Instant.parse("2024-01-01T01:00:00Z"),
            ColumnInfoTypeName.TIMESTAMP,
            "2024-01-01T01:00:00Z"),
        new TemporalValue(
            OffsetDateTime.parse("2024-01-01T01:00:00+05:00"),
            ColumnInfoTypeName.TIMESTAMP,
            "2024-01-01T01:00+05:00",
            "2024-01-01T01:00:00+05:00"),
        new TemporalValue(
            ZonedDateTime.parse("2024-01-01T01:00:00-05:00[America/New_York]"),
            ColumnInfoTypeName.TIMESTAMP,
            "2024-01-01T01:00-05:00[America/New_York]",
            "2024-01-01T01:00:00-05:00"),
        new TemporalValue(
            OffsetDateTime.parse("2024-01-01T00:00:00Z"),
            ColumnInfoTypeName.TIMESTAMP,
            "2024-01-01T00:00Z",
            "2024-01-01T00:00:00Z"),
        new TemporalValue(
            OffsetDateTime.parse("2024-01-01T05:45:37.123456-07:00").truncatedTo(ChronoUnit.HOURS),
            ColumnInfoTypeName.TIMESTAMP,
            "2024-01-01T05:00-07:00",
            "2024-01-01T05:00:00-07:00"),
        new TemporalValue(
            OffsetDateTime.parse("2024-01-01T05:45:37.123456+05:45")
                .truncatedTo(ChronoUnit.MINUTES),
            ColumnInfoTypeName.TIMESTAMP,
            "2024-01-01T05:45+05:45",
            "2024-01-01T05:45:00+05:45"),
        new TemporalValue(
            OffsetDateTime.parse("0000-01-01T00:00:00+00:00:01"),
            ColumnInfoTypeName.TIMESTAMP,
            "0000-01-01T00:00+00:00:01",
            "0000-01-01T00:00:00+00:00:01"),
        new TemporalValue(
            OffsetDateTime.parse("-0100-01-01T00:00:00+18:00"),
            ColumnInfoTypeName.TIMESTAMP,
            "-0100-01-01T00:00+18:00",
            "-0100-01-01T00:00:00+18:00"),
        new TemporalValue(
            ZonedDateTime.parse("+10000-01-02T03:04+01:00[Europe/Paris]"),
            ColumnInfoTypeName.TIMESTAMP,
            "+10000-01-02T03:04+01:00[Europe/Paris]",
            "+10000-01-02T03:04:00+01:00"),
        new TemporalValue(
            ZonedDateTime.parse("2024-01-01T00:00:00Z[UTC]"),
            ColumnInfoTypeName.TIMESTAMP,
            "2024-01-01T00:00Z[UTC]",
            "2024-01-01T00:00:00Z"),
        new TemporalValue(
            ZonedDateTime.parse("2024-01-01T05:45:37.123456-05:00[America/New_York]")
                .truncatedTo(ChronoUnit.HOURS),
            ColumnInfoTypeName.TIMESTAMP,
            "2024-01-01T05:00-05:00[America/New_York]",
            "2024-01-01T05:00:00-05:00"),
        new TemporalValue(
            OffsetDateTime.parse("2024-01-01T01:00:00.500+05:00"),
            ColumnInfoTypeName.TIMESTAMP,
            "2024-01-01T01:00:00.500+05:00"),
        new TemporalValue(
            ZonedDateTime.parse("2024-01-01T01:00:00.500-05:00[America/New_York]"),
            ColumnInfoTypeName.TIMESTAMP,
            "2024-01-01T01:00:00.500-05:00[America/New_York]",
            "2024-01-01T01:00:00.500-05:00"),
        new TemporalValue(
            ZonedDateTime.parse("2026-01-02T03:04:05.123456-05:00[America/New_York]"),
            ColumnInfoTypeName.TIMESTAMP,
            "2026-01-02T03:04:05.123456-05:00[America/New_York]",
            "2026-01-02T03:04:05.123456-05:00"),
        new TemporalValue(
            OffsetDateTime.parse("2024-06-30T23:59:59.999999-03:00"),
            ColumnInfoTypeName.TIMESTAMP,
            "2024-06-30T23:59:59.999999-03:00"),
        new TemporalValue(
            legacyTimestamp(-100), ColumnInfoTypeName.TIMESTAMP, "-0100-01-02 03:04:05.123456789"),
        new TemporalValue(
            new Date(legacyTimestamp(0).getTime()), ColumnInfoTypeName.DATE, "0000-01-02"),
        new TemporalValue(
            legacyTimestamp(10000),
            ColumnInfoTypeName.TIMESTAMP,
            "+10000-01-02 03:04:05.123456789"),
        new TemporalValue(
            new Date(legacyTimestamp(10000).getTime()), ColumnInfoTypeName.DATE, "+10000-01-02"));
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

  private static final class TemporalValue {
    private final Object value;
    private final ColumnInfoTypeName inferredType;
    private final String text;
    private final String timestampText;

    private TemporalValue(Object value, ColumnInfoTypeName inferredType, String text) {
      this(value, inferredType, text, text);
    }

    private TemporalValue(
        Object value, ColumnInfoTypeName inferredType, String text, String timestampText) {
      this.value = value;
      this.inferredType = inferredType;
      this.text = text;
      this.timestampText = timestampText;
    }
  }
}
