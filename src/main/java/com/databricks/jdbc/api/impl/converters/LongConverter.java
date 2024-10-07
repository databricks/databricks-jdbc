package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;

public class LongConverter implements ObjectConverter {
  @Override
  public long toLong(Object object) throws DatabricksSQLException {
    if (object instanceof String) {
      return Long.parseLong((String) object);
    } else if (object instanceof Number) {
      return ((Number) object).longValue();
    } else {
      throw new DatabricksSQLException(
          "Unsupported type for LongObjectConverter: " + object.getClass());
    }
  }

  @Override
  public byte toByte(Object object) throws DatabricksSQLException {
    long value = toLong(object);
    if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
      return (byte) value;
    }
    throw new DatabricksSQLException("Invalid conversion: Long value out of byte range");
  }

  @Override
  public short toShort(Object object) throws DatabricksSQLException {
    long value = toLong(object);
    if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
      return (short) value;
    }
    throw new DatabricksSQLException("Invalid conversion: Long value out of short range");
  }

  @Override
  public int toInt(Object object) throws DatabricksSQLException {
    long value = toLong(object);
    if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
      return (int) value;
    }
    throw new DatabricksSQLException("Invalid conversion: Long value out of int range");
  }

  @Override
  public float toFloat(Object object) throws DatabricksSQLException {
    return toLong(object);
  }

  @Override
  public double toDouble(Object object) throws DatabricksSQLException {
    return toLong(object);
  }

  @Override
  public BigDecimal toBigDecimal(Object object) throws DatabricksSQLException {
    return BigDecimal.valueOf(toLong(object));
  }

  @Override
  public BigInteger toBigInteger(Object object) throws DatabricksSQLException {
    return BigInteger.valueOf(toLong(object));
  }

  @Override
  public boolean toBoolean(Object object) throws DatabricksSQLException {
    return toLong(object) != 0;
  }

  @Override
  public byte[] toByteArray(Object object) throws DatabricksSQLException {
    return ByteBuffer.allocate(8).putLong(toLong(object)).array();
  }

  @Override
  public String toString(Object object) throws DatabricksSQLException {
    return String.valueOf(toLong(object));
  }

  @Override
  public Timestamp toTimestamp(Object object) throws DatabricksSQLException {
    return toTimestamp(object, DEFAULT_TIMESTAMP_SCALE);
  }

  @Override
  public Timestamp toTimestamp(Object object, int scale) throws DatabricksSQLException {
    if (scale > 9) {
      throw new DatabricksSQLException("Unsupported scale");
    }
    long nanoseconds = toLong(object) * POWERS_OF_TEN[9 - scale];
    Time time = new Time(nanoseconds / POWERS_OF_TEN[6]);
    return new Timestamp(time.getTime());
  }

  @Override
  public Date toDate(Object object) throws DatabricksSQLException {
    LocalDate localDate = LocalDate.ofEpochDay(toLong(object));
    return Date.valueOf(localDate);
  }
}
