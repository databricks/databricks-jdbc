package com.databricks.jdbc.core.converters;

import com.databricks.jdbc.core.DatabricksSQLException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public class DoubleConverter extends AbstractObjectConverter {

  private double object;

  public DoubleConverter(Object object) throws DatabricksSQLException {
    super(object);
    if (object instanceof String) {
      this.object = Double.parseDouble((String) object);
    } else {
      this.object = (double) object;
    }
  }

  @Override
  public double convertToDouble() throws DatabricksSQLException {
    return this.object;
  }

  @Override
  public BigInteger convertToBigInteger() throws DatabricksSQLException {
    return BigInteger.valueOf(this.convertToLong());
  }

  @Override
  public boolean convertToBoolean() throws DatabricksSQLException {
    return (this.object == 0f ? false : true);
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
  public long convertToLong() throws DatabricksSQLException {
    if (this.object >= Long.MIN_VALUE && this.object < Long.MAX_VALUE) {
      return (long) this.object;
    }
    throw new DatabricksSQLException("Invalid conversion");
  }

  @Override
  public float convertToFloat() throws DatabricksSQLException {
    if (this.object >= -Float.MAX_VALUE && this.object <= Float.MAX_VALUE) {
      return (float) this.object;
    }
    throw new DatabricksSQLException("Invalid conversion");
  }

  @Override
  public BigDecimal convertToBigDecimal() throws DatabricksSQLException {
    return new BigDecimal(Double.toString(this.object));
  }

  @Override
  public byte[] convertToByteArray() throws DatabricksSQLException {
    return ByteBuffer.allocate(8).putDouble(this.object).array();
  }

  @Override
  public String convertToString() throws DatabricksSQLException {
    return String.valueOf(this.object);
  }
}
