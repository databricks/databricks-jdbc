package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;

public class StringConverter extends AbstractObjectConverter {

  private final String object;

  public StringConverter(Object object) throws DatabricksSQLException {
    super(object);
    if (object instanceof Character) {
      this.object = object.toString();
    } else {
      this.object = (String) object;
    }
  }

  @Override
  public byte convertToByte() throws DatabricksSQLException {
    byte[] byteArray = object.getBytes();
    if (byteArray.length == 1) {
      return object.getBytes()[0];
    }
    throw new DatabricksSQLException("Invalid conversion");
  }

  @Override
  public short convertToShort() throws DatabricksSQLException {
    try {
      return Short.parseShort(object);
    } catch (NumberFormatException e) {
      throw new DatabricksSQLException("Invalid conversion");
    }
  }

  @Override
  public int convertToInt() throws DatabricksSQLException {
    try {
      return Integer.parseInt(object);
    } catch (NumberFormatException e) {
      throw new DatabricksSQLException("Invalid conversion");
    }
  }

  @Override
  public long convertToLong() throws DatabricksSQLException {
    try {
      return Long.parseLong(object);
    } catch (NumberFormatException e) {
      throw new DatabricksSQLException("Invalid conversion");
    }
  }

  @Override
  public float convertToFloat() throws DatabricksSQLException {
    try {
      return Float.parseFloat(object);
    } catch (NumberFormatException e) {
      throw new DatabricksSQLException("Invalid conversion");
    }
  }

  @Override
  public double convertToDouble() throws DatabricksSQLException {
    try {
      return Double.parseDouble(object);
    } catch (NumberFormatException e) {
      throw new DatabricksSQLException("Invalid conversion");
    }
  }

  @Override
  public BigDecimal convertToBigDecimal() throws DatabricksSQLException {
    return new BigDecimal(object);
  }

  @Override
  public BigInteger convertToBigInteger() throws DatabricksSQLException {
    return BigInteger.valueOf(this.convertToLong());
  }

  @Override
  public boolean convertToBoolean() throws DatabricksSQLException {
    if ("0".equals(object) || "false".equalsIgnoreCase(object)) {
      return false;
    } else if ("1".equals(object) || "true".equalsIgnoreCase(object)) {
      return true;
    }
    throw new DatabricksSQLException("Invalid conversion");
  }

  @Override
  public byte[] convertToByteArray() throws DatabricksSQLException {
    return object.getBytes();
  }

  @Override
  public char convertToChar() throws DatabricksSQLException {
    if (object.length() == 1) {
      return object.charAt(0);
    }
    throw new DatabricksSQLException("Invalid conversion");
  }

  @Override
  public String convertToString() throws DatabricksSQLException {
    return object;
  }

  @Override
  public Date convertToDate() throws DatabricksSQLException {
    return Date.valueOf(object);
  }

  @Override
  public Timestamp convertToTimestamp() throws DatabricksSQLException {
    return Timestamp.valueOf(object);
  }
}
