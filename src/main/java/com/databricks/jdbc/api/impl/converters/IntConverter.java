package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;

public class IntConverter extends AbstractObjectConverter {

  private final int object;

  public IntConverter(Object object) throws DatabricksSQLException {
    super(object);
    if (object instanceof String) {
      this.object = Integer.parseInt((String) object);
    } else {
      this.object = (int) object;
    }
  }

  @Override
  public byte convertToByte() throws DatabricksSQLException {
    if (object >= Byte.MIN_VALUE && object <= Byte.MAX_VALUE) {
      return (byte) object;
    }
    throw new DatabricksSQLException("Invalid conversion");
  }

  @Override
  public short convertToShort() throws DatabricksSQLException {
    if (object >= Short.MIN_VALUE && object <= Short.MAX_VALUE) {
      return (short) object;
    }
    throw new DatabricksSQLException("Invalid conversion");
  }

  @Override
  public int convertToInt() throws DatabricksSQLException {
    return object;
  }

  @Override
  public long convertToLong() throws DatabricksSQLException {
    return object;
  }

  @Override
  public float convertToFloat() throws DatabricksSQLException {
    return (float) object;
  }

  @Override
  public double convertToDouble() throws DatabricksSQLException {
    return object;
  }

  @Override
  public BigDecimal convertToBigDecimal() throws DatabricksSQLException {
    return BigDecimal.valueOf(object);
  }

  @Override
  public BigInteger convertToBigInteger() throws DatabricksSQLException {
    return BigInteger.valueOf(object);
  }

  @Override
  public boolean convertToBoolean() throws DatabricksSQLException {
    return (object != 0);
  }

  @Override
  public byte[] convertToByteArray() throws DatabricksSQLException {
    return ByteBuffer.allocate(4).putInt(object).array();
  }

  @Override
  public String convertToString() throws DatabricksSQLException {
    return String.valueOf(object);
  }

  @Override
  public Timestamp convertToTimestamp() throws DatabricksSQLException {
    return convertToTimestamp(super.DEFAULT_TIMESTAMP_SCALE);
  }

  @Override
  public Timestamp convertToTimestamp(int scale) throws DatabricksSQLException {
    if (scale > 9) {
      throw new DatabricksSQLException("Unsupported scale");
    }
    long nanoseconds = (long) object * super.POWERS_OF_TEN[9 - scale];
    Time time = new Time(nanoseconds / super.POWERS_OF_TEN[6]);
    return new Timestamp(time.getTime());
  }

  @Override
  public Date convertToDate() throws DatabricksSQLException {
    LocalDate localDate = LocalDate.ofEpochDay(object);
    return Date.valueOf(localDate);
  }
}
