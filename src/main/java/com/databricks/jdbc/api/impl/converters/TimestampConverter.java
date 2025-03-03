package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;

public class TimestampConverter implements ObjectConverter {

  @Override
  public Time toTime(Object object) throws DatabricksSQLException {
    Timestamp timestamp = toTimestamp(object);
    return new Time(timestamp.getTime());
  }

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

  @Override
  public long toLong(Object object) throws DatabricksSQLException {
    return toTimestamp(object).toInstant().toEpochMilli();
  }

  @Override
  public BigInteger toBigInteger(Object object) throws DatabricksSQLException {
    return BigInteger.valueOf(toLong(object));
  }

  @Override
  public String toString(Object object) throws DatabricksSQLException {
    return toTimestamp(object).toInstant().toString();
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
      return new Date(ts.getTime());
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
        return Timestamp.from(odt.toInstant());
      } else {
        // For strings without offset, replace 'T' with a space.
        // Example: "2023-03-15T12:34:56" becomes "2023-03-15 12:34:56"
        String tsStr = inputTimestamp.replace("T", " ");
        return Timestamp.valueOf(tsStr);
      }
    } catch (IllegalArgumentException | DateTimeParseException e) {
      // As a fallback, try parsing as an Instant using the original inputTimestamp.
      try {
        Instant instant = Instant.parse(inputTimestamp);
        return Timestamp.from(instant);
      } catch (Exception ex) {
        throw new DatabricksSQLException(
            "Invalid conversion to Timestamp", ex, DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
      }
    }
  }
}
