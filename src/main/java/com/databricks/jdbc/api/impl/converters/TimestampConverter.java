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
      String inputTimestamp = (String) object;
      // Check if the string contains a timezone offset.
      // This regex checks for a '+' or '-' after the time component.
      // Example with offset: "2023-03-15T12:34:56+05:30"
      boolean hasOffset = inputTimestamp.matches(".*T.*([+\\-]\\d\\d:\\d\\d)$");
      try {
        if (hasOffset) {
          // Parse using OffsetDateTime for strings with timezone offset
          OffsetDateTime odt = OffsetDateTime.parse(inputTimestamp);
          return Timestamp.from(odt.toInstant());
        } else {
          // For strings without offset, replace 'T' with a space and use Timestamp.valueOf
          // Timestamp_ntz columns don't have offset. No timezone info.
          // Example without offset: "2023-03-15T12:34:56"
          String timestamp = inputTimestamp.replace("T", " ");
          return Timestamp.valueOf(timestamp);
        }
      } catch (IllegalArgumentException | DateTimeParseException e) {
        // As a fallback, try parsing as an Instant using the original inputTimestamp
        try {
          Instant instant = Instant.parse(inputTimestamp);
          return Timestamp.from(instant);
        } catch (Exception ex) {
          throw new DatabricksSQLException(
              "Invalid conversion to Timestamp",
              ex,
              DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
        }
      }
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
      String dateString = (String) object;
      try {
        // Check if the string ends with a timezone offset (e.g., -08:00 or +05:30)
        if (dateString.matches(".*[+-]\\d{2}:\\d{2}$")) {
          // Parse the string as an OffsetDateTime which respects the embedded offset.
          // Example with offset: "2023-03-15T12:34:56+05:30"
          OffsetDateTime odt = OffsetDateTime.parse(dateString);
          return new Date(odt.toInstant().toEpochMilli());
        } else {
          // Otherwise, assume no offset information and parse as LocalDateTime. TIMESTAMP_NTZ
          // Example without offset: "2023-03-15T12:34:56"
          if (dateString.contains("T")) {
            dateString = dateString.replace("T", " ");
          }
          // Parse as a local date-time using Timestamp.valueOf, then convert to java.sql.Date.
          Timestamp ts = Timestamp.valueOf(dateString);
          return new Date(ts.getTime());
        }
      } catch (Exception e) {
        throw new DatabricksSQLException(
            "Invalid conversion to Date", e, DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
      }
    }
    return new Date(toLong(object));
  }
}
