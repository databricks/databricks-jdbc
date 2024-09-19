package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigDecimal;
import java.math.BigInteger;

public class BigDecimalConverter extends AbstractObjectConverter {

  private final BigDecimal object;

  public BigDecimalConverter(Object object) throws DatabricksSQLException {
    super(object);
    if (object instanceof String) {
      this.object = new BigDecimal((String) object);
    } else {
      this.object = (BigDecimal) object;
    }
  }

  @Override
  public byte convertToByte() throws DatabricksSQLException {
    try {
      return object.toBigInteger().byteValueExact();
    } catch (ArithmeticException e) {
      throw new DatabricksSQLException("Invalid conversion");
    }
  }

  @Override
  public short convertToShort() throws DatabricksSQLException {
    try {
      return object.toBigInteger().shortValueExact();
    } catch (ArithmeticException e) {
      throw new DatabricksSQLException("Invalid conversion");
    }
  }

  @Override
  public int convertToInt() throws DatabricksSQLException {
    try {
      return object.toBigInteger().intValueExact();
    } catch (ArithmeticException e) {
      throw new DatabricksSQLException("Invalid conversion");
    }
  }

  @Override
  public long convertToLong() throws DatabricksSQLException {
    try {
      return object.toBigInteger().longValueExact();
    } catch (ArithmeticException e) {
      throw new DatabricksSQLException("Invalid conversion");
    }
  }

  @Override
  public float convertToFloat() throws DatabricksSQLException {
    return object.floatValue();
  }

  @Override
  public double convertToDouble() throws DatabricksSQLException {
    return object.doubleValue();
  }

  @Override
  public BigDecimal convertToBigDecimal() throws DatabricksSQLException {
    return object;
  }

  @Override
  public BigInteger convertToBigInteger() throws DatabricksSQLException {
    return object.toBigInteger();
  }

  @Override
  public boolean convertToBoolean() throws DatabricksSQLException {
    return !object.equals(BigDecimal.valueOf(0));
  }

  @Override
  public byte[] convertToByteArray() throws DatabricksSQLException {
    return object.toBigInteger().toByteArray();
  }

  @Override
  public String convertToString() throws DatabricksSQLException {
    return String.valueOf(object);
  }
}
