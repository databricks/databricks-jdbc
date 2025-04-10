package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class StringConverter implements ObjectConverter {
  @Override
  public String toString(Object object) throws DatabricksSQLException {
    if (object instanceof Character) {
      return object.toString();
    } else if (object instanceof String) {
      return (String) object;
    }
    throw new DatabricksValidationException("Invalid conversion to String");
  }

  @Override
  public byte toByte(Object object) throws DatabricksSQLException {
    String str = toString(object);
    byte[] byteArray = str.getBytes();
    if (byteArray.length == 1) {
      return byteArray[0];
    }
    throw new DatabricksValidationException("Invalid conversion to byte");
  }

  @Override
  public short toShort(Object object) throws DatabricksSQLException {
    try {
      return Short.parseShort(toString(object));
    } catch (NumberFormatException e) {
      throw new DatabricksValidationException("Invalid conversion to short", e);
    }
  }

  @Override
  public int toInt(Object object) throws DatabricksSQLException {
    try {
      return Integer.parseInt(toString(object));
    } catch (NumberFormatException e) {
      throw new DatabricksValidationException("Invalid conversion to int", e);
    }
  }

  @Override
  public long toLong(Object object) throws DatabricksSQLException {
    try {
      return Long.parseLong(toString(object));
    } catch (NumberFormatException e) {
      throw new DatabricksValidationException("Invalid conversion to long", e);
    }
  }

  @Override
  public float toFloat(Object object) throws DatabricksSQLException {
    try {
      return Float.parseFloat(toString(object));
    } catch (NumberFormatException e) {
      throw new DatabricksValidationException("Invalid conversion to float", e);
    }
  }

  @Override
  public double toDouble(Object object) throws DatabricksSQLException {
    try {
      return Double.parseDouble(toString(object));
    } catch (NumberFormatException e) {
      throw new DatabricksValidationException("Invalid conversion to double", e);
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
    return true;
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
    throw new DatabricksValidationException("Invalid conversion to char");
  }

  @Override
  public Date toDate(Object object) throws DatabricksSQLException {
    return Date.valueOf(removeExtraQuotes(toString(object)));
  }

  @Override
  public Timestamp toTimestamp(Object object) throws DatabricksSQLException {
    String timestampStr = removeExtraQuotes(toString(object));

    try {
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSXXX");
      java.util.Date parsedDate = dateFormat.parse(timestampStr);
      return new Timestamp(parsedDate.getTime());
    } catch (ParseException e) {
      try {
        SimpleDateFormat simpleFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssXXX");
        java.util.Date parsedDate = simpleFormat.parse(timestampStr);
        return new Timestamp(parsedDate.getTime());
      } catch (ParseException e2) {
        try {
          return Timestamp.valueOf(timestampStr);
        } catch (IllegalArgumentException ex) {
          throw new DatabricksParsingException(
              "Invalid timestamp format: " + timestampStr,
              DatabricksDriverErrorCode.JSON_PARSING_ERROR);
        }
      }
    }
  }

  private String removeExtraQuotes(String str) {
    if (str.startsWith("\"") && str.endsWith("\"") && str.length() > 1) {
      str = str.substring(1, str.length() - 1);
    }
    return str;
  }
}
