package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;

public class IntConverter implements ObjectConverter {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(IntConverter.class);
  private final IDatabricksConnectionContext connectionContext;

  public IntConverter(IDatabricksConnectionContext connectionContext){
    this.connectionContext = connectionContext;
  }

  @Override
  public IDatabricksConnectionContext getConnectionContext(){
    return connectionContext;
  }

  @Override
  public int toInt(Object object) throws DatabricksSQLException {
    if (object instanceof String) {
      return Integer.parseInt((String) object);
    } else if (object instanceof Number) {
      return ((Number) object).intValue();
    } else {
      throw new DatabricksSQLException(
          "Unsupported type for IntObjectConverter: " + object.getClass(), DatabricksDriverErrorCode.UNSUPPORTED_OPERATION,getConnectionContext());
    }
  }

  @Override
  public byte toByte(Object object) throws DatabricksSQLException {
    int value = toInt(object);
    if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
      return (byte) value;
    }
    throw new DatabricksValidationException("Invalid conversion: Int value out of byte range",getConnectionContext());
  }

  @Override
  public short toShort(Object object) throws DatabricksSQLException {
    int value = toInt(object);
    if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
      return (short) value;
    }
    throw new DatabricksValidationException("Invalid conversion: Int value out of short range",getConnectionContext());
  }

  @Override
  public long toLong(Object object) throws DatabricksSQLException {
    return toInt(object);
  }

  @Override
  public float toFloat(Object object) throws DatabricksSQLException {
    return toInt(object);
  }

  @Override
  public double toDouble(Object object) throws DatabricksSQLException {
    return toInt(object);
  }

  @Override
  public BigDecimal toBigDecimal(Object object) throws DatabricksSQLException {
    return BigDecimal.valueOf(toInt(object));
  }

  @Override
  public BigInteger toBigInteger(Object object) throws DatabricksSQLException {
    return BigInteger.valueOf(toInt(object));
  }

  @Override
  public boolean toBoolean(Object object) throws DatabricksSQLException {
    return toInt(object) != 0;
  }

  @Override
  public byte[] toByteArray(Object object) throws DatabricksSQLException {
    return ByteBuffer.allocate(4).putInt(toInt(object)).array();
  }

  @Override
  public String toString(Object object) throws DatabricksSQLException {
    return String.valueOf(toInt(object));
  }

  @Override
  public Timestamp toTimestamp(Object object) throws DatabricksSQLException {
    return toTimestamp(object, DEFAULT_TIMESTAMP_SCALE);
  }

  @Override
  public Timestamp toTimestamp(Object object, int scale) throws DatabricksSQLException {
    if (scale > 9) {
      throw new DatabricksSQLException("Unsupported scale",DatabricksDriverErrorCode.UNSUPPORTED_OPERATION,getConnectionContext());
    }
    long nanoseconds = (long) toInt(object) * POWERS_OF_TEN[9 - scale];
    Time time = new Time(nanoseconds / POWERS_OF_TEN[6]);
    LOGGER.info("IntConverter#toTimestamp: " + time + " " + time.toLocalTime().toString());
    return new Timestamp(time.getTime());
  }

  @Override
  public Date toDate(Object object) throws DatabricksSQLException {
    LocalDate localDate = LocalDate.ofEpochDay(toInt(object));
    return Date.valueOf(localDate);
  }
}
