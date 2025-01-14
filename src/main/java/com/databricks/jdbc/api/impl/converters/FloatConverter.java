package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public class FloatConverter implements ObjectConverter {

  @Override
  public float toFloat(Object object) throws DatabricksSQLException {
    if (object instanceof String) {
      return Float.parseFloat((String) object);
    } else if (object instanceof Number) {
      return ((Number) object).floatValue();
    } else {
      throw new DatabricksSQLException(
          "Unsupported type for FloatObjectConverter: " + object.getClass(),
          DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
    }
  }

  @Override
  public byte toByte(Object object) throws DatabricksSQLException {
    float value = toFloat(object);
    if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
      return (byte) value;
    }
    throw new DatabricksValidationException("Invalid conversion: Float value out of byte range");
  }

  @Override
  public short toShort(Object object) throws DatabricksSQLException {
    float value = toFloat(object);
    if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
      return (short) value;
    }
    throw new DatabricksValidationException("Invalid conversion: Float value out of short range");
  }

  @Override
  public int toInt(Object object) throws DatabricksSQLException {
    float value = toFloat(object);
    if (value >= Integer.MIN_VALUE && value < Integer.MAX_VALUE) {
      return (int) value;
    }
    throw new DatabricksValidationException("Invalid conversion: Float value out of int range");
  }

  @Override
  public long toLong(Object object) throws DatabricksSQLException {
    float value = toFloat(object);
    if (value >= Long.MIN_VALUE && value < Long.MAX_VALUE) {
      return (long) value;
    }
    throw new DatabricksValidationException("Invalid conversion: Float value out of long range");
  }

  @Override
  public double toDouble(Object object) throws DatabricksSQLException {
    return toFloat(object);
  }

  @Override
  public BigDecimal toBigDecimal(Object object) throws DatabricksSQLException {
    return new BigDecimal(Float.toString(toFloat(object)));
  }

  @Override
  public BigInteger toBigInteger(Object object) throws DatabricksSQLException {
    return BigInteger.valueOf(toLong(object));
  }

  @Override
  public boolean toBoolean(Object object) throws DatabricksSQLException {
    return toFloat(object) != 0f;
  }

  @Override
  public byte[] toByteArray(Object object) throws DatabricksSQLException {
    return ByteBuffer.allocate(4).putFloat(toFloat(object)).array();
  }

  @Override
  public String toString(Object object) throws DatabricksSQLException {
    return String.valueOf(toFloat(object));
  }
}
