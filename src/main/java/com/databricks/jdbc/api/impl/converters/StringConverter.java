package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksValidationException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;

public class StringConverter implements ObjectConverter {
  private final IDatabricksConnectionContext connectionContext;

  public StringConverter(IDatabricksConnectionContext connectionContext){
    this.connectionContext = connectionContext;
  }
  @Override
  public IDatabricksConnectionContext getConnectionContext(){
    return connectionContext;
  }
  @Override
  public String toString(Object object) throws DatabricksSQLException {
    if (object instanceof Character) {
      return object.toString();
    } else if (object instanceof String) {
      return (String) object;
    }
    throw new DatabricksValidationException("Invalid conversion to String",getConnectionContext());
  }

  @Override
  public byte toByte(Object object) throws DatabricksSQLException {
    String str = toString(object);
    byte[] byteArray = str.getBytes();
    if (byteArray.length == 1) {
      return byteArray[0];
    }
    throw new DatabricksValidationException("Invalid conversion to byte",getConnectionContext());
  }

  @Override
  public short toShort(Object object) throws DatabricksSQLException {
    try {
      return Short.parseShort(toString(object));
    } catch (NumberFormatException e) {
      throw new DatabricksValidationException("Invalid conversion to short", e,getConnectionContext());
    }
  }

  @Override
  public int toInt(Object object) throws DatabricksSQLException {
    try {
      return Integer.parseInt(toString(object));
    } catch (NumberFormatException e) {
      throw new DatabricksValidationException("Invalid conversion to int", e, getConnectionContext());
    }
  }

  @Override
  public long toLong(Object object) throws DatabricksSQLException {
    try {
      return Long.parseLong(toString(object));
    } catch (NumberFormatException e) {
      throw new DatabricksValidationException("Invalid conversion to long", e, getConnectionContext());
    }
  }

  @Override
  public float toFloat(Object object) throws DatabricksSQLException {
    try {
      return Float.parseFloat(toString(object));
    } catch (NumberFormatException e) {
      throw new DatabricksValidationException("Invalid conversion to float", e, getConnectionContext());
    }
  }

  @Override
  public double toDouble(Object object) throws DatabricksSQLException {
    try {
      return Double.parseDouble(toString(object));
    } catch (NumberFormatException e) {
      throw new DatabricksValidationException("Invalid conversion to double", e, getConnectionContext());
    }
  }

  @Override
  public BigDecimal toBigDecimal(Object object) throws DatabricksSQLException {
    return new BigDecimal(toString(object));
  }

  @Override
  public BigInteger toBigInteger(Object object) throws DatabricksSQLException {
    return BigInteger.valueOf(toLong(object));
  }

  @Override
  public boolean toBoolean(Object object) throws DatabricksSQLException {
    String str = toString(object).toLowerCase();
    if ("0".equals(str) || "false".equals(str)) {
      return false;
    } else if ("1".equals(str) || "true".equals(str)) {
      return true;
    }
    throw new DatabricksValidationException("Invalid conversion to boolean",getConnectionContext());
  }

  @Override
  public byte[] toByteArray(Object object) throws DatabricksSQLException {
    return toString(object).getBytes();
  }

  @Override
  public char toChar(Object object) throws DatabricksSQLException {
    String str = toString(object);
    if (str.length() == 1) {
      return str.charAt(0);
    }
    throw new DatabricksValidationException("Invalid conversion to char",getConnectionContext());
  }

  @Override
  public Date toDate(Object object) throws DatabricksSQLException {
    return Date.valueOf(toString(object));
  }

  @Override
  public Timestamp toTimestamp(Object object) throws DatabricksSQLException {
    return Timestamp.valueOf(toString(object));
  }
}
