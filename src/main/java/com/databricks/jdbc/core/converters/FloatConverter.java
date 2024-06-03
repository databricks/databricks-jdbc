package com.databricks.jdbc.core.converters;

import com.databricks.jdbc.core.DatabricksSQLException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public class FloatConverter extends AbstractObjectConverter {

  private float object;

  public FloatConverter(Object object) throws DatabricksSQLException {
    super(object);
    if (object instanceof String) {
      this.object = Float.parseFloat((String) object);
    } else {
      this.object = (float) object;
    }
  }

  @Override
  public BigInteger convertToBigInteger() throws DatabricksSQLException {
    return BigInteger.valueOf(this.convertToLong());
  }

  @Override
  public float convertToFloat() throws DatabricksSQLException {
    return this.object;
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
    if (this.object >= Integer.MIN_VALUE && this.object < Integer.MAX_VALUE) {
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
  public double convertToDouble() throws DatabricksSQLException {
    return (double) this.object;
  }

  @Override
  public BigDecimal convertToBigDecimal() throws DatabricksSQLException {
    return new BigDecimal(Float.toString(this.object));
  }

  @Override
  public byte[] convertToByteArray() throws DatabricksSQLException {
    return ByteBuffer.allocate(4).putFloat(this.object).array();
  }

  @Override
  public String convertToString() throws DatabricksSQLException {
    return String.valueOf(this.object);
  }
}
