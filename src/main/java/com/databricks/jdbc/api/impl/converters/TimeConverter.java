package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.DateTimeException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

public class TimeConverter implements ObjectConverter {
  public static final Class<?> DEFAULT_OBJECT_CLASS = Time.class;
  private static final DateTimeFormatter CLOCK =
      new DateTimeFormatterBuilder()
          .appendPattern("HH:mm:ss")
          .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
          .toFormatter(java.util.Locale.ROOT);

  public static LocalTime toLocalTime(Object value) {
    if (value instanceof LocalTime) {
      return (LocalTime) value;
    }
    if (value instanceof java.time.LocalDateTime) {
      return ((java.time.LocalDateTime) value).toLocalTime();
    }
    if (value instanceof Time) {
      Time time = (Time) value;
      return time.toLocalTime().withNano((int) Math.floorMod(time.getTime(), 1000L) * 1_000_000);
    }
    if (value instanceof Number) {
      return LocalTime.ofNanoOfDay(((Number) value).longValue());
    }
    String text = value.toString();
    return text.indexOf(':') >= 0
        ? LocalTime.parse(text)
        : LocalTime.ofNanoOfDay(Long.parseLong(text));
  }

  public static String format(Object value) {
    return CLOCK.format(toLocalTime(value));
  }

  public static Time toSqlTime(Object value) {
    LocalTime time = toLocalTime(value);
    return new Time(Time.valueOf(time).getTime() + time.getNano() / 1_000_000);
  }

  public static DatabricksSQLException invalidValue(Throwable cause) {
    return new DatabricksSQLException(
        "Invalid conversion to Time",
        DatabricksDriverErrorCode.UNSUPPORTED_OPERATION.name(),
        DatabricksDriverErrorCode.UNSUPPORTED_OPERATION.getCode(),
        cause);
  }

  @Override
  public Timestamp toTimestamp(Object value) throws DatabricksSQLException {
    try {
      return Timestamp.valueOf(toLocalTime(value).atDate(java.time.LocalDate.of(1970, 1, 1)));
    } catch (DateTimeException | IllegalArgumentException exception) {
      throw invalidValue(exception);
    }
  }

  @Override
  public Time toTime(Object value) throws DatabricksSQLException {
    try {
      return toSqlTime(value);
    } catch (DateTimeException | IllegalArgumentException exception) {
      throw invalidValue(exception);
    }
  }

  @Override
  public String toString(Object value) throws DatabricksSQLException {
    try {
      return format(value);
    } catch (DateTimeException | IllegalArgumentException exception) {
      throw invalidValue(exception);
    }
  }
}
