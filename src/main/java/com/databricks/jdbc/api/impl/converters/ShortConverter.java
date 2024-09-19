package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public class ShortConverter extends AbstractObjectConverter {

  private final short object;

  public ShortConverter(Object object) throws DatabricksSQLException {
    super(object);
    if (object instanceof String) {
      this.object = Short.parseShort((String) object);
    } else if (object instanceof Number) {
      this.object = ((Number) object).shortValue();
    } else if (object instanceof Boolean) {
      this.object = (short) (((Boolean) object) ? 1 : 0);
    } else {
      this.object = (short) object;
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
    return ByteBuffer.allocate(2).putShort(object).array();
  }

  @Override
  public String convertToString() throws DatabricksSQLException {
    return String.valueOf(object);
  }
}
