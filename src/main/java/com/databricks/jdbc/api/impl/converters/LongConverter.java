package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;

public class LongConverter extends AbstractObjectConverter {

  private long object;

  public LongConverter(Object object) throws DatabricksSQLException {
    super(object);
    if (object instanceof String) {
      this.object = Long.parseLong((String) object);
    } else {
      this.object = (long) object;
    }
  }

  @Override
  public BigInteger convertToBigInteger() throws DatabricksSQLException {
    return BigInteger.valueOf(this.object);
  }

  @Override
  public long convertToLong() throws DatabricksSQLException {
    return this.object;
  }

  @Override
  public boolean convertToBoolean() throws DatabricksSQLException {
    return (this.object == 0 ? false : true);
  }

  @Override
  public byte convertToByte() throws DatabricksSQLException {
    if (this.object >= Byte.MIN_VALUE && this.object <= Byte.MAX_VALUE) {
      return (byte) this.object;
    }
    throw new DatabricksSQLException("Invalid conversion");
  }

  @Override
  public short convertToShort() throws DatabricksSQLException {
    if (this.object >= Short.MIN_VALUE && this.object <= Short.MAX_VALUE) {
      return (short) this.object;
    }
    throw new DatabricksSQLException("Invalid conversion");
  }

  @Override
  public int convertToInt() throws DatabricksSQLException {
    if (this.object >= Integer.MIN_VALUE && this.object <= Integer.MAX_VALUE) {
      return (int) this.object;
    }
    throw new DatabricksSQLException("Invalid conversion");
  }

  @Override
  public float convertToFloat() throws DatabricksSQLException {
    return (float) this.object;
  }

  @Override
  public double convertToDouble() throws DatabricksSQLException {
    return (double) this.object;
  }

  @Override
  public BigDecimal convertToBigDecimal() throws DatabricksSQLException {
    return BigDecimal.valueOf(this.object);
  }

  @Override
  public byte[] convertToByteArray() throws DatabricksSQLException {
    return ByteBuffer.allocate(8).putLong(this.object).array();
  }

  @Override
  public String convertToString() throws DatabricksSQLException {
    return String.valueOf(this.object);
  }

  @Override
  public Date convertToDate() throws DatabricksSQLException {
    LocalDate localDate = LocalDate.ofEpochDay(this.object);
    return Date.valueOf(localDate);
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
    long nanoseconds = this.object * super.POWERS_OF_TEN[9 - scale];
    Time time = new Time(nanoseconds / super.POWERS_OF_TEN[6]);
    return new Timestamp(time.getTime());
  }
}
