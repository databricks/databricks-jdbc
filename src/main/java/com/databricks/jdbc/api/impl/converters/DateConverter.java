package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;

public class DateConverter implements ObjectConverter {
  @Override
  public Date toDate(Object object) throws DatabricksSQLException {
    if (object instanceof String) {
      return Date.valueOf((String) object);
    } else if (object instanceof Date) {
      return (Date) object;
    } else {
      throw new DatabricksSQLException(
          "Unsupported type for DateObjectConverter: " + object.getClass(),
          DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
    }
  }

  /** Returns the epoch day. Exact for {@link LocalDate} and ISO text, see {@link #toLocalDate}. */
  @Override
  public long toLong(Object object) throws DatabricksSQLException {
    return toLocalDate(object).toEpochDay();
  }

  @Override
  public short toShort(Object object) throws DatabricksSQLException {
    long epochDays = toLong(object);
    if ((short) epochDays == epochDays) {
      return (short) epochDays;
    }
    throw new DatabricksValidationException("Invalid conversion: Date value out of short range");
  }

  @Override
  public int toInt(Object object) throws DatabricksSQLException {
    // the convertToLong will always be within integer limits
    return (int) toLong(object);
  }

  @Override
  public BigInteger toBigInteger(Object object) throws DatabricksSQLException {
    return BigInteger.valueOf(toLong(object));
  }

  /**
   * Converts to a {@link LocalDate}. A {@link LocalDate} (the exact value read from Arrow) and ISO
   * text are read without going through {@link Date}, which shifts pre-1582 dates and drops the BC
   * era.
   */
  @Override
  public LocalDate toLocalDate(Object object) throws DatabricksSQLException {
    if (object instanceof LocalDate) {
      return (LocalDate) object;
    }
    if (object instanceof String) {
      LocalDate date = JavaTimeConverter.parseDateText((String) object);
      if (date != null) {
        return date;
      }
    }
    return toDate(object).toLocalDate();
  }

  @Override
  public String toString(Object object) throws DatabricksSQLException {
    return toDate(object).toString();
  }

  @Override
  public Timestamp toTimestamp(Object object) throws DatabricksSQLException {
    return Timestamp.valueOf(toDate(object).toLocalDate().atStartOfDay());
  }
}
