package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

public class BigDecimalConverter implements ObjectConverter {
  @Override
  public byte toByte(Object object) throws DatabricksSQLException {
    try {
      return toBigDecimal(object).toBigInteger().byteValueExact();
    } catch (ArithmeticException e) {
      throw new DatabricksValidationException("Invalid conversion to byte", e);
    }
  }

  @Override
  public short toShort(Object object) throws DatabricksSQLException {
    try {
      return toBigDecimal(object).toBigInteger().shortValueExact();
    } catch (ArithmeticException e) {
      throw new DatabricksValidationException("Invalid conversion to short", e);
    }
  }

  @Override
  public int toInt(Object object) throws DatabricksSQLException {
    try {
      return toBigDecimal(object).toBigInteger().intValueExact();
    } catch (ArithmeticException e) {
      throw new DatabricksValidationException("Invalid conversion to int", e);
    }
  }

  @Override
  public long toLong(Object object) throws DatabricksSQLException {
    try {
      return toBigDecimal(object).toBigInteger().longValueExact();
    } catch (ArithmeticException e) {
      throw new DatabricksValidationException("Invalid conversion to long", e);
    }
  }

  @Override
  public float toFloat(Object object) throws DatabricksSQLException {
    return toBigDecimal(object).floatValue();
  }

  @Override
  public double toDouble(Object object) throws DatabricksSQLException {
    return toBigDecimal(object).doubleValue();
  }

  @Override
  public BigDecimal toBigDecimal(Object object) throws DatabricksSQLException {
    if (object instanceof BigDecimal) {
      return (BigDecimal) object;
    } else if (object instanceof String) {
      try {
        return new BigDecimal((String) object);
      } catch (NumberFormatException e) {
        throw new DatabricksValidationException("Invalid BigDecimal string: " + object, e);
      }
    } else if (object instanceof Number) {
      return BigDecimal.valueOf(((Number) object).doubleValue());
    }
    throw new DatabricksValidationException("Cannot convert to BigDecimal: " + object.getClass());
  }

  @Override
  public BigDecimal toBigDecimal(Object object, int scale) throws DatabricksSQLException {
    BigDecimal bigDecimal = toBigDecimal(object);
    try {
      return bigDecimal.setScale(scale, RoundingMode.HALF_EVEN);
    } catch (ArithmeticException e) {
      throw new DatabricksValidationException("Error setting scale for BigDecimal", e);
    }
  }

  @Override
  public BigInteger toBigInteger(Object object) throws DatabricksSQLException {
    return toBigDecimal(object).toBigInteger();
  }

  @Override
  public boolean toBoolean(Object object) throws DatabricksSQLException {
    return !toBigDecimal(object).equals(BigDecimal.ZERO);
  }

  @Override
  public byte[] toByteArray(Object object) throws DatabricksSQLException {
    return toBigInteger(object).toByteArray();
  }

  @Override
  public String toString(Object object) throws DatabricksSQLException {
    return toBigDecimal(object).toString();
  }
}
