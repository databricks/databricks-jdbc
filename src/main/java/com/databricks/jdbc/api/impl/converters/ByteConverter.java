package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigDecimal;
import java.math.BigInteger;

public class ByteConverter implements ObjectConverter {

  @Override
  public byte toByte(Object object) throws DatabricksSQLException {
    if (object instanceof String) {
      return Byte.parseByte((String) object);
    } else if (object instanceof Number) {
      return ((Number) object).byteValue();
    } else if (object instanceof Boolean) {
      return (byte) (((Boolean) object) ? 1 : 0);
    } else {
      return (byte) object;
    }
  }

  @Override
  public short toShort(Object object) throws DatabricksSQLException {
    return toByte(object);
  }

  @Override
  public int toInt(Object object) throws DatabricksSQLException {
    return toByte(object);
  }

  @Override
  public long toLong(Object object) throws DatabricksSQLException {
    return toByte(object);
  }

  @Override
  public float toFloat(Object object) throws DatabricksSQLException {
    return toByte(object);
  }

  @Override
  public double toDouble(Object object) throws DatabricksSQLException {
    return toByte(object);
  }

  @Override
  public BigDecimal toBigDecimal(Object object) throws DatabricksSQLException {
    return BigDecimal.valueOf(toByte(object));
  }

  @Override
  public BigInteger toBigInteger(Object object) throws DatabricksSQLException {
    return BigInteger.valueOf(toByte(object));
  }

  @Override
  public boolean toBoolean(Object object) throws DatabricksSQLException {
    return toByte(object) != 0;
  }

  @Override
  public byte[] toByteArray(Object object) throws DatabricksSQLException {
    return new byte[] {toByte(object)};
  }

  @Override
  public String toString(Object object) throws DatabricksSQLException {
    return new String(new byte[] {toByte(object)});
  }
}
