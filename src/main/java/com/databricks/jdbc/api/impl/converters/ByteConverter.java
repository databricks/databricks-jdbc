package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigDecimal;
import java.math.BigInteger;

public class ByteConverter extends AbstractObjectConverter {

  private final byte object;

  public ByteConverter(Object object) throws DatabricksSQLException {
    super(object);
    if (object instanceof String) {
      this.object = Byte.parseByte((String) object);
    } else if (object instanceof Number) {
      this.object = ((Number) object).byteValue();
    } else if (object instanceof Boolean) {
      this.object = (byte) (((Boolean) object) ? 1 : 0);
    } else {
      this.object = (byte) object;
    }
  }

  @Override
  public byte convertToByte() throws DatabricksSQLException {
    return object;
  }

  @Override
  public short convertToShort() throws DatabricksSQLException {
    return object;
  }

  @Override
  public int convertToInt() throws DatabricksSQLException {
    return object;
  }

  @Override
  public long convertToLong() throws DatabricksSQLException {
    return object;
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
    return BigDecimal.valueOf(object);
  }

  @Override
  public BigInteger convertToBigInteger() throws DatabricksSQLException {
    return BigInteger.valueOf(object);
  }

  @Override
  public boolean convertToBoolean() throws DatabricksSQLException {
    return (object != 0);
  }

  @Override
  public byte[] convertToByteArray() throws DatabricksSQLException {
    return new byte[] {object};
  }

  @Override
  public String convertToString() throws DatabricksSQLException {
    return new String(new byte[] {object});
  }
}
