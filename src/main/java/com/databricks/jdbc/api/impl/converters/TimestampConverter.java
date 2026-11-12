package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.time.temporal.Temporal;

public class TimestampConverter implements ObjectConverter {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(IntConverter.class);

  // Two days after the 1582-10-15 Gregorian cutover; java.sql.Timestamp is exact from here on.
  private static final LocalDateTime EXACT_FROM = LocalDateTime.of(1582, 10, 17, 0, 0);

  @Override
  public Time toTime(Object object) throws DatabricksSQLException {
    Timestamp timestamp = toTimestamp(object);
    // Extract just the time components
    LocalTime localTime = timestamp.toLocalDateTime().toLocalTime();
    // Create Time from LocalTime (this strips date components)
    long epochWithMillis = Time.valueOf(localTime).getTime() + timestamp.getTime() % 1000;
    return new Time(epochWithMillis);
  }

  /**
   * Converts an object to a {@link Timestamp}.
   *
   * <p>The method supports various input formats depending on the mode:
   *
   * <ul>
   *   <li><b>timestamp_ntz:</b>
   *       <ul>
   *         <li><b>Thrift Arrow:</b> <code>timestamp yyyy-MM-dd HH:mm:ss.S</code>
   *         <li><b>Thrift Inline:</b> <code>yyyy-MM-dd HH:mm:ss.S</code> (String)
   *         <li><b>Sea Inline:</b> <code>yyyy-MM-dd'T'HH:mm:ss.SSS</code> (String)
   *         <li><b>Sea Arrow:</b> <code>timestamp yyyy-MM-dd HH:mm:ss.S</code>
   *       </ul>
   *   <li><b>Timestamp:</b>
   *       <ul>
   *         <li><b>Thrift Arrow:</b> <code>timestamp yyyy-MM-dd HH:mm:ss.S</code>
   *         <li><b>Thrift Inline:</b> <code>yyyy-MM-dd HH:mm:ss</code> (String)
   *         <li><b>Sea Inline:</b> <code>yyyy-MM-dd'T'HH:mm:ss.SSSXXX</code> (String)
   *         <li><b>Sea Arrow:</b> <code>yyyy-MM-dd HH:mm:ss.S</code>
   *       </ul>
   * </ul>
   *
   * @param object the object to convert
   * @return the converted {@link Timestamp}
   * @throws DatabricksSQLException if the conversion fails
   */
  @Override
  public Timestamp toTimestamp(Object object) throws DatabricksSQLException {
    if (object instanceof Timestamp) {
      return (Timestamp) object;
    } else if (object instanceof String) {
      return parseStringToTimestamp((String) object);
    }
    throw new DatabricksSQLException(
        "Unsupported conversion to Timestamp for type: " + object.getClass().getName(),
        DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }

  /**
   * Returns epoch milliseconds: the same value as {@code toTimestamp(object).getTime()}, except
   * before the 1582-10-15 Gregorian cutover, where {@link Timestamp}'s Julian calendar shifts the
   * value by days and BC years lose their era.
   *
   * <p>Before the cutover, the {@code java.time} values read from Arrow ({@link OffsetDateTime} for
   * TIMESTAMP, {@link LocalDateTime} for TIMESTAMP_NTZ) and ISO text are read on the proleptic
   * Gregorian calendar, with the same meaning as {@code getTimestamp()}: the session-local fields
   * in the JVM default time zone (text ending in 'Z' is an instant).
   */
  @Override
  public long toLong(Object object) throws DatabricksSQLException {
    if (object instanceof OffsetDateTime) {
      return toEpochMilli(((OffsetDateTime) object).toLocalDateTime());
    }
    if (object instanceof LocalDateTime) {
      return toEpochMilli((LocalDateTime) object);
    }
    Timestamp timestamp = toTimestamp(object);
    long epochMilli = timestamp.toInstant().toEpochMilli();
    if (!(object instanceof String) || !JavaTimeConverter.mayBeCalendarShifted(timestamp)) {
      return epochMilli;
    }
    String text = ((String) object).trim();
    Temporal parsed = JavaTimeConverter.parseTimestampText(text);
    if (parsed instanceof OffsetDateTime) {
      // parseStringToTimestamp reads text ending in 'Z' as an instant and ignores other offsets.
      OffsetDateTime offsetDateTime = (OffsetDateTime) parsed;
      return text.endsWith("Z") || text.endsWith("z")
          ? offsetDateTime.toInstant().toEpochMilli()
          : toEpochMilli(offsetDateTime.toLocalDateTime());
    }
    return parsed instanceof LocalDateTime ? toEpochMilli((LocalDateTime) parsed) : epochMilli;
  }

  /**
   * Reads the fields in the JVM default time zone: {@code Timestamp.valueOf(fields).getTime()} from
   * two days after the Gregorian cutover, and {@code java.time} on the proleptic Gregorian calendar
   * before it.
   */
  private static long toEpochMilli(LocalDateTime fields) {
    if (!fields.isBefore(EXACT_FROM)) {
      return Timestamp.valueOf(fields).toInstant().toEpochMilli();
    }
    return fields
        .atZone(ZoneId.systemDefault())
        .withLaterOffsetAtOverlap()
        .toInstant()
        .toEpochMilli();
  }

  @Override
  public BigInteger toBigInteger(Object object) throws DatabricksSQLException {
    return BigInteger.valueOf(toLong(object));
  }

  @Override
  public String toString(Object object) throws DatabricksSQLException {
    return toTimestamp(object).toString();
  }

  @Override
  public Date toDate(Object object) throws DatabricksSQLException {
    if (object instanceof Date) {
      return (Date) object;
    } else if (object instanceof Timestamp) {
      return new Date(((Timestamp) object).getTime());
    } else if (object instanceof String) {
      // Use the common helper method to parse the string into a Timestamp.
      Timestamp ts = parseStringToTimestamp((String) object);
      LocalDate date = ts.toLocalDateTime().toLocalDate();
      return Date.valueOf(date);
    }
    return new Date(toLong(object));
  }

  /**
   * Parses a timestamp represented as a string.
   *
   * @param inputTimestamp the string to parse
   * @return the parsed Timestamp
   * @throws DatabricksSQLException if the string cannot be parsed into a Timestamp
   */
  private Timestamp parseStringToTimestamp(String inputTimestamp) throws DatabricksSQLException {
    try {
      // Check if the string contains a timezone offset.
      // Example with offset: "2023-03-15T12:34:56+05:30"
      if (inputTimestamp.matches(".*T.*([+\\-]\\d\\d:\\d\\d)$")) {
        // Parse using OffsetDateTime for strings with timezone offset.
        OffsetDateTime odt = OffsetDateTime.parse(inputTimestamp);
        // JDBC always assumes that the time returned is local time and hence, the offset is
        // actually ignored.
        return Timestamp.valueOf(odt.toLocalDateTime());
      } else {
        // For strings without offset, replace 'T' with a space.
        // Example: "2023-03-15T12:34:56" becomes "2023-03-15 12:34:56"
        String tsStr = inputTimestamp.replace("T", " ");
        return Timestamp.valueOf(tsStr);
      }
    } catch (IllegalArgumentException | DateTimeParseException e) {
      LOGGER.error("Failed to parse timestamp: {}", inputTimestamp, e);
      // As a fallback, try parsing as an Instant using the original inputTimestamp.
      try {
        Instant instant = Instant.parse(inputTimestamp);
        return Timestamp.from(instant);
      } catch (Exception ex) {
        throw new DatabricksSQLException(
            "Invalid conversion to Timestamp for input: " + inputTimestamp,
            ex,
            DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
      }
    }
  }
}
