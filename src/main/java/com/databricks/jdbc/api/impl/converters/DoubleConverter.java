package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public class DoubleConverter implements ObjectConverter {
  @Override
  public double toDouble(Object object) throws DatabricksSQLException {
    if (object instanceof String) {
      return Double.parseDouble((String) object);
    } else if (object instanceof Number) {
      return ((Number) object).doubleValue();
    } else {
      throw new DatabricksSQLException(
          "Unsupported type for DoubleObjectConverter: " + object.getClass(),
          DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
    }
  }

  @Override
  public boolean toBoolean(Object object) throws DatabricksSQLException {
    return toDouble(object) != 0.0;
  }

  @Override
  public byte toByte(Object object) throws DatabricksSQLException {
    double value = toDouble(object);
    if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
      return (byte) value;
    }
    throw new DatabricksValidationException("Invalid conversion: Double value out of byte range");
  }

  @Override
  public short toShort(Object object) throws DatabricksSQLException {
    double value = toDouble(object);
    if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
      return (short) value;
    }
    throw new DatabricksValidationException("Invalid conversion: Double value out of short range");
  }

  @Override
  public int toInt(Object object) throws DatabricksSQLException {
    double value = toDouble(object);
    if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
      return (int) value;
    }
    throw new DatabricksValidationException("Invalid conversion: Double value out of int range");
  }

  @Override
  public long toLong(Object object) throws DatabricksSQLException {
    double value = toDouble(object);
    if (value >= Long.MIN_VALUE && value <= Long.MAX_VALUE) {
      return (long) value;
    }
    throw new DatabricksValidationException("Invalid conversion: Double value out of long range");
  }

  @Override
  public float toFloat(Object object) throws DatabricksSQLException {
    double value = toDouble(object);
    if (value >= -Float.MAX_VALUE && value <= Float.MAX_VALUE) {
      return (float) value;
    }
    throw new DatabricksValidationException("Invalid conversion: Double value out of float range");
  }

  @Override
  public BigDecimal toBigDecimal(Object object) throws DatabricksSQLException {
    return BigDecimal.valueOf(toDouble(object));
  }

  @Override
  public BigInteger toBigInteger(Object object) throws DatabricksSQLException {
    return BigInteger.valueOf(toLong(object));
  }

  @Override
  public byte[] toByteArray(Object object) throws DatabricksSQLException {
    return ByteBuffer.allocate(8).putDouble(toDouble(object)).array();
  }

  @Override
  public String toString(Object object) throws DatabricksSQLException {
    return String.valueOf(toDouble(object));
  }
}
