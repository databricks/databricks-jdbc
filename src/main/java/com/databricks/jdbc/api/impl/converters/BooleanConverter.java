package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigDecimal;
import java.math.BigInteger;

public class BooleanConverter extends AbstractObjectConverter {

  private final Boolean object;

  public BooleanConverter(Object object) throws DatabricksSQLException {
    super(object);
    if (object instanceof String) {
      this.object = Boolean.parseBoolean((String) object);
    } else {
      this.object = (boolean) object;
    }
  }

  @Override
  public byte convertToByte() throws DatabricksSQLException {
    return (byte) (object ? 1 : 0);
  }

  @Override
  public short convertToShort() throws DatabricksSQLException {
    return (short) (object ? 1 : 0);
  }

  @Override
  public int convertToInt() throws DatabricksSQLException {
    return object ? 1 : 0;
  }

  @Override
  public long convertToLong() throws DatabricksSQLException {
    return object ? 1L : 0L;
  }

  @Override
  public float convertToFloat() throws DatabricksSQLException {
    return object ? 1f : 0f;
  }

  @Override
  public double convertToDouble() throws DatabricksSQLException {
    return object ? 1 : 0;
  }

  @Override
  public BigDecimal convertToBigDecimal() throws DatabricksSQLException {
    return BigDecimal.valueOf(object ? 1 : 0);
  }

  @Override
  public BigInteger convertToBigInteger() throws DatabricksSQLException {
    return object ? BigInteger.ONE : BigInteger.ZERO;
  }

  @Override
  public boolean convertToBoolean() throws DatabricksSQLException {
    return object;
  }

  @Override
  public byte[] convertToByteArray() throws DatabricksSQLException {
    return object ? new byte[] {1} : new byte[] {0};
  }

  @Override
  public char convertToChar() throws DatabricksSQLException {
    return object ? '1' : '0';
  }

  @Override
  public String convertToString() throws DatabricksSQLException {
    return String.valueOf(object);
  }
}
