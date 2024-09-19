package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public class FloatConverter extends AbstractObjectConverter {

  private final float object;

  public FloatConverter(Object object) throws DatabricksSQLException {
    super(object);
    if (object instanceof String) {
      this.object = Float.parseFloat((String) object);
    } else {
      this.object = (float) object;
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
    if (object >= Integer.MIN_VALUE && object < Integer.MAX_VALUE) {
      return (int) object;
    }
    throw new DatabricksSQLException("Invalid conversion");
  }

  @Override
  public long convertToLong() throws DatabricksSQLException {
    if (object >= Long.MIN_VALUE && object < Long.MAX_VALUE) {
      return (long) object;
    }
    throw new DatabricksSQLException("Invalid conversion");
  }

  @Override
  public float convertToFloat() throws DatabricksSQLException {
    return object;
  }

  @Override
  public double convertToDouble() throws DatabricksSQLException {
    return object;
  }

  @Override
  public BigDecimal convertToBigDecimal() throws DatabricksSQLException {
    return new BigDecimal(Float.toString(object));
  }

  @Override
  public BigInteger convertToBigInteger() throws DatabricksSQLException {
    return BigInteger.valueOf(this.convertToLong());
  }

  @Override
  public boolean convertToBoolean() throws DatabricksSQLException {
    return (object != 0f);
  }

  @Override
  public byte[] convertToByteArray() throws DatabricksSQLException {
    return ByteBuffer.allocate(4).putFloat(object).array();
  }

  @Override
  public String convertToString() throws DatabricksSQLException {
    return String.valueOf(object);
  }
}
