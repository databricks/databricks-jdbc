package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigDecimal;
import java.math.BigInteger;

public class BooleanConverter implements ObjectConverter {
  @Override
  public boolean toBoolean(Object object) throws DatabricksSQLException {
    if (object instanceof Boolean) {
      return (Boolean) object;
    } else if (object instanceof String) {
      return Boolean.parseBoolean((String) object);
    } else {
      throw new DatabricksSQLException("Unsupported object type for BooleanObjectConverter");
    }
  }

  @Override
  public byte toByte(Object object) throws DatabricksSQLException {
    return (byte) (toBoolean(object) ? 1 : 0);
  }

  @Override
  public short toShort(Object object) throws DatabricksSQLException {
    return (short) (toBoolean(object) ? 1 : 0);
  }

  @Override
  public int toInt(Object object) throws DatabricksSQLException {
    return toBoolean(object) ? 1 : 0;
  }

  @Override
  public long toLong(Object object) throws DatabricksSQLException {
    return toBoolean(object) ? 1L : 0L;
  }

  @Override
  public float toFloat(Object object) throws DatabricksSQLException {
    return toBoolean(object) ? 1f : 0f;
  }

  @Override
  public double toDouble(Object object) throws DatabricksSQLException {
    return toBoolean(object) ? 1.0 : 0.0;
  }

  @Override
  public BigDecimal toBigDecimal(Object object) throws DatabricksSQLException {
    return BigDecimal.valueOf(toBoolean(object) ? 1 : 0);
  }

  @Override
  public BigInteger toBigInteger(Object object) throws DatabricksSQLException {
    return toBoolean(object) ? BigInteger.ONE : BigInteger.ZERO;
  }

  @Override
  public byte[] toByteArray(Object object) throws DatabricksSQLException {
    return toBoolean(object) ? new byte[] {1} : new byte[] {0};
  }

  @Override
  public char toChar(Object object) throws DatabricksSQLException {
    return toBoolean(object) ? '1' : '0';
  }

  @Override
  public String toString(Object object) throws DatabricksSQLException {
    return String.valueOf(toBoolean(object));
  }
}
