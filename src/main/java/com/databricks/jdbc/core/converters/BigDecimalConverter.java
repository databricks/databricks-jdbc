package com.databricks.jdbc.core.converters;

import com.databricks.jdbc.core.DatabricksSQLException;
import java.math.BigDecimal;

public class BigDecimalConverter extends AbstractObjectConverter {

  private BigDecimal object;

  public BigDecimalConverter(Object object) throws DatabricksSQLException {
    super(object);
    if (object instanceof String) {
      this.object = new BigDecimal((String) object);
    } else {
      this.object = (BigDecimal) object;
    }
  }

  @Override
  public BigDecimal convertToBigDecimal() throws DatabricksSQLException {
    return this.object;
  }

  @Override
  public byte convertToByte() throws DatabricksSQLException {
    try {
      return this.object.toBigInteger().byteValueExact();
    } catch (ArithmeticException e) {
      throw new DatabricksSQLException("Invalid conversion");
    }
  }

  @Override
  public byte[] convertToByteArray() throws DatabricksSQLException {
    return this.object.toBigInteger().toByteArray();
  }

  @Override
  public short convertToShort() throws DatabricksSQLException {
    try {
      return this.object.toBigInteger().shortValueExact();
    } catch (ArithmeticException e) {
      throw new DatabricksSQLException("Invalid conversion");
    }
  }

  @Override
  public int convertToInt() throws DatabricksSQLException {
    try {
      return this.object.toBigInteger().intValueExact();
    } catch (ArithmeticException e) {
      throw new DatabricksSQLException("Invalid conversion");
    }
  }

  @Override
  public long convertToLong() throws DatabricksSQLException {
    try {
      return this.object.toBigInteger().longValueExact();
    } catch (ArithmeticException e) {
      throw new DatabricksSQLException("Invalid conversion");
    }
  }

  @Override
  public float convertToFloat() throws DatabricksSQLException {
    return this.object.floatValue();
  }

  @Override
  public double convertToDouble() throws DatabricksSQLException {
    return this.object.doubleValue();
  }

  @Override
  public boolean convertToBoolean() throws DatabricksSQLException {
    return !this.object.equals(BigDecimal.valueOf(0));
  }

  @Override
  public String convertToString() throws DatabricksSQLException {
    return String.valueOf(this.object);
  }
}
