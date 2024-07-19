package com.databricks.jdbc.core.converters;

import com.databricks.jdbc.core.DatabricksSQLException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;

public class StringConverter extends AbstractObjectConverter {

  private String object;

  public StringConverter(Object object) throws DatabricksSQLException {
    super(object);
    if (object instanceof Character) {
      this.object = object.toString(); // Convert Character to String
    } else {
      this.object = (String) object;
    }
  }

  @Override
  public String convertToString() throws DatabricksSQLException {
    return this.object;
  }

  @Override
  public char convertToChar() throws DatabricksSQLException {
    if (this.object.length() == 1) {
      return this.object.charAt(0);
    }
    throw new DatabricksSQLException("Invalid conversion");
  }

  @Override
  public boolean convertToBoolean() throws DatabricksSQLException {
    if ("0".equals(this.object) || "false".equalsIgnoreCase(this.object)) {
      return false;
    } else if ("1".equals(this.object) || "true".equalsIgnoreCase(this.object)) {
      return true;
    }
    throw new DatabricksSQLException("Invalid conversion");
  }

  @Override
  public byte convertToByte() throws DatabricksSQLException {
    byte[] byteArray = this.object.getBytes();
    if (byteArray.length == 1) {
      return this.object.getBytes()[0];
    }
    throw new DatabricksSQLException("Invalid conversion");
  }

  @Override
  public short convertToShort() throws DatabricksSQLException {
    try {
      return Short.parseShort(this.object);
    } catch (NumberFormatException e) {
      throw new DatabricksSQLException("Invalid conversion");
    }
  }

  @Override
  public int convertToInt() throws DatabricksSQLException {
    try {
      return Integer.parseInt(this.object);
    } catch (NumberFormatException e) {
      throw new DatabricksSQLException("Invalid conversion");
    }
  }

  @Override
  public long convertToLong() throws DatabricksSQLException {
    try {
      return Long.parseLong(this.object);
    } catch (NumberFormatException e) {
      throw new DatabricksSQLException("Invalid conversion");
    }
  }

  @Override
  public BigInteger convertToBigInteger() throws DatabricksSQLException {
    return BigInteger.valueOf(this.convertToLong());
  }

  @Override
  public float convertToFloat() throws DatabricksSQLException {
    try {
      return Float.parseFloat(this.object);
    } catch (NumberFormatException e) {
      throw new DatabricksSQLException("Invalid conversion");
    }
  }

  @Override
  public double convertToDouble() throws DatabricksSQLException {
    try {
      return Double.parseDouble(this.object);
    } catch (NumberFormatException e) {
      throw new DatabricksSQLException("Invalid conversion");
    }
  }

  @Override
  public BigDecimal convertToBigDecimal() throws DatabricksSQLException {
    return new BigDecimal(this.object);
  }

  @Override
  public byte[] convertToByteArray() throws DatabricksSQLException {
    return this.object.getBytes();
  }

  @Override
  public Date convertToDate() throws DatabricksSQLException {
    return Date.valueOf(this.object);
  }

  @Override
  public Timestamp convertToTimestamp() throws DatabricksSQLException {
    return Timestamp.valueOf(this.object);
  }
}
