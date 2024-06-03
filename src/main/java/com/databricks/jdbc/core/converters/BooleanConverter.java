package com.databricks.jdbc.core.converters;

import com.databricks.jdbc.core.DatabricksSQLException;
import java.math.BigDecimal;
import java.math.BigInteger;

public class BooleanConverter extends AbstractObjectConverter {

  private Boolean object;

  public BooleanConverter(Object object) throws DatabricksSQLException {
    super(object);
    if (object instanceof String) {
      this.object = Boolean.parseBoolean((String) object);
    } else {
      this.object = (boolean) object;
    }
  }

  @Override
  public boolean convertToBoolean() throws DatabricksSQLException {
    return this.object;
  }

  @Override
  public byte convertToByte() throws DatabricksSQLException {
    return (byte) (this.object ? 1 : 0);
  }

  @Override
  public short convertToShort() throws DatabricksSQLException {
    return (short) (this.object ? 1 : 0);
  }

  @Override
  public int convertToInt() throws DatabricksSQLException {
    return this.object ? 1 : 0;
  }

  @Override
  public BigInteger convertToBigInteger() throws DatabricksSQLException {
    return this.object ? BigInteger.ONE : BigInteger.ZERO;
  }

  @Override
  public long convertToLong() throws DatabricksSQLException {
    return this.object ? 1L : 0L;
  }

  @Override
  public float convertToFloat() throws DatabricksSQLException {
    return this.object ? 1f : 0f;
  }

  @Override
  public double convertToDouble() throws DatabricksSQLException {
    return (double) (this.object ? 1 : 0);
  }

  @Override
  public BigDecimal convertToBigDecimal() throws DatabricksSQLException {
    return BigDecimal.valueOf(this.object ? 1 : 0);
  }

  @Override
  public byte[] convertToByteArray() throws DatabricksSQLException {
    return this.object ? new byte[] {1} : new byte[] {0};
  }

  @Override
  public char convertToChar() throws DatabricksSQLException {
    return this.object ? '1' : '0';
  }

  @Override
  public String convertToString() throws DatabricksSQLException {
    return String.valueOf(this.object);
  }
}
