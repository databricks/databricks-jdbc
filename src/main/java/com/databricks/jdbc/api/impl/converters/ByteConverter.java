package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigDecimal;
import java.math.BigInteger;

public class ByteConverter extends AbstractObjectConverter {

  private byte object;

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
    return this.object;
  }

  @Override
  public boolean convertToBoolean() throws DatabricksSQLException {
    return (this.object == 0 ? false : true);
  }

  @Override
  public short convertToShort() throws DatabricksSQLException {
    return (short) this.object;
  }

  @Override
  public int convertToInt() throws DatabricksSQLException {
    return (int) this.object;
  }

  @Override
  public BigInteger convertToBigInteger() throws DatabricksSQLException {
    return BigInteger.valueOf(this.object);
  }

  @Override
  public long convertToLong() throws DatabricksSQLException {
    return (long) this.object;
  }

  @Override
  public float convertToFloat() throws DatabricksSQLException {
    return (float) this.object;
  }

  @Override
  public double convertToDouble() throws DatabricksSQLException {
    return (double) this.object;
  }

  @Override
  public BigDecimal convertToBigDecimal() throws DatabricksSQLException {
    return BigDecimal.valueOf((long) this.object);
  }

  @Override
  public byte[] convertToByteArray() throws DatabricksSQLException {
    return new byte[] {this.object};
  }

  @Override
  public String convertToString() throws DatabricksSQLException {
    return new String(new byte[] {this.object});
  }
}
