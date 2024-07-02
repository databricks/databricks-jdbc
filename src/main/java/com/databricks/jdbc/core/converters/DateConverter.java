package com.databricks.jdbc.core.converters;

import com.databricks.jdbc.core.DatabricksSQLException;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

public class DateConverter extends AbstractObjectConverter {

  private Date object;

  public DateConverter(Object object) throws DatabricksSQLException {
    super(object);
    if (object instanceof String) {
      this.object = Date.valueOf((String) object);
    } else {
      this.object = (Date) object;
    }
  }

  @Override
  public BigInteger convertToBigInteger() throws DatabricksSQLException {
    long epochDays = convertToLong();
    return BigInteger.valueOf(epochDays);
  }

  @Override
  public LocalDate convertToLocalDate() throws DatabricksSQLException {
    return this.object.toLocalDate();
  }

  @Override
  public Date convertToDate() throws DatabricksSQLException {
    return this.object;
  }

  @Override
  public short convertToShort() throws DatabricksSQLException {
    long epochDays = convertToLong();
    if ((short) epochDays == epochDays) {
      return (short) epochDays;
    }
    throw new DatabricksSQLException("Invalid conversion");
  }

  @Override
  public int convertToInt() throws DatabricksSQLException {
    long epochDays = convertToLong();
    return (int) epochDays; // the convertToLong will always be within integer limits
  }

  @Override
  public long convertToLong() throws DatabricksSQLException {
    LocalDate localStartDate = LocalDate.ofEpochDay(0);
    return ChronoUnit.DAYS.between(localStartDate, this.object.toLocalDate());
  }

  @Override
  public String convertToString() throws DatabricksSQLException {
    return this.object.toString();
  }

  @Override
  public Timestamp convertToTimestamp() throws DatabricksSQLException {
    return Timestamp.valueOf(this.object.toLocalDate().atStartOfDay());
  }
}
