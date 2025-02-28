package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.TimeZone;

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
      try {
        return Timestamp.valueOf((String) object);
      } catch (IllegalArgumentException e) {
        try {
          Instant instant = Instant.parse((String) object);
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
    TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
    return new Date(toLong(object));
  }
}
