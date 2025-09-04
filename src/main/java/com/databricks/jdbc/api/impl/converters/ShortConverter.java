package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public class ShortConverter implements ObjectConverter {
  @Override
  public short toShort(Object object) throws DatabricksSQLException {
    if (object instanceof String) {
      return Short.parseShort((String) object);
    } else if (object instanceof Number) {
      return ((Number) object).shortValue();
    } else if (object instanceof Boolean) {
      return (short) (((Boolean) object) ? 1 : 0);
    } else {
      return (short) object;
    }
  }

  @Override
  public byte toByte(Object object) throws DatabricksSQLException {
    short value = toShort(object);
    if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
      return (byte) value;
    }
    throw new DatabricksValidationException("Invalid conversion to byte: value out of range");
  }

  @Override
  public int toInt(Object object) throws DatabricksSQLException {
    return toShort(object);
  }

  @Override
  public long toLong(Object object) throws DatabricksSQLException {
    return toShort(object);
  }

  @Override
  public float toFloat(Object object) throws DatabricksSQLException {
    return toShort(object);
  }

  @Override
  public double toDouble(Object object) throws DatabricksSQLException {
    return toShort(object);
  }

  @Override
  public BigDecimal toBigDecimal(Object object) throws DatabricksSQLException {
    return BigDecimal.valueOf(toShort(object));
  }

  @Override
  public BigInteger toBigInteger(Object object) throws DatabricksSQLException {
    return BigInteger.valueOf(toShort(object));
  }

  @Override
  public boolean toBoolean(Object object) throws DatabricksSQLException {
    return toShort(object) != 0;
  }

  @Override
  public byte[] toByteArray(Object object) throws DatabricksSQLException {
    return ByteBuffer.allocate(2).putShort(toShort(object)).array();
  }

  @Override
  public String toString(Object object) throws DatabricksSQLException {
    return String.valueOf(toShort(object));
  }
}
