package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.api.impl.DatabricksArray;
import com.databricks.jdbc.api.impl.DatabricksMap;
import com.databricks.jdbc.api.impl.DatabricksStruct;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

public class StringConverter implements ObjectConverter {
  @Override
  public String toString(Object object) throws DatabricksSQLException {
    if (object instanceof Character) {
      return object.toString();
    } else if (object instanceof String) {
      return (String) object;
    } else if (object instanceof DatabricksArray
        || object instanceof DatabricksMap
        || object instanceof DatabricksStruct) {
      return object.toString();
    } else if (object instanceof java.sql.Array) {
      return convertSqlArrayToString((java.sql.Array) object);
    } else if (object instanceof Struct) {
      return convertStructToString((Struct) object);
    } else if (object instanceof Map) {
      return convertMapToString((Map<?, ?>) object);
    } else if (object instanceof Collection) {
      return convertCollectionToString((Collection<?>) object);
    } else if (object != null && object.getClass().isArray()) {
      return convertJavaArrayToString(object);
    }
    if (object != null) {
      return object.toString();
    }
    throw new DatabricksValidationException("Invalid conversion to String");
  }

  private String convertSqlArrayToString(java.sql.Array array) throws DatabricksSQLException {
    try {
      Object arrayData = array.getArray();
      if (arrayData == null) {
        return "null";
      }
      return convertJavaArrayToString(arrayData);
    } catch (SQLException e) {
      throw new DatabricksValidationException("Invalid Array to String conversion", e);
    }
  }

  private String convertStructToString(Struct struct) throws DatabricksSQLException {
    try {
      Object[] attributes = struct.getAttributes();
      if (attributes == null) {
        return "{}";
      }
      StringBuilder sb = new StringBuilder("{");
      for (int i = 0; i < attributes.length; i++) {
        if (i > 0) {
          sb.append(",");
        }
        sb.append(convertValueToString(attributes[i]));
      }
      sb.append("}");
      return sb.toString();
    } catch (SQLException e) {
      throw new DatabricksValidationException("Invalid Struct to String conversion", e);
    }
  }

  private String convertMapToString(Map<?, ?> map) throws DatabricksSQLException {
    StringBuilder sb = new StringBuilder("{");
    Iterator<? extends Map.Entry<?, ?>> iterator = map.entrySet().iterator();
    int index = 0;
    while (iterator.hasNext()) {
      if (index++ > 0) {
        sb.append(",");
      }
      Map.Entry<?, ?> entry = iterator.next();
      if (entry == null) {
        sb.append("null:null");
      } else {
        sb.append(convertValueToString(entry.getKey()))
            .append(":")
            .append(convertValueToString(entry.getValue()));
      }
    }
    sb.append("}");
    return sb.toString();
  }

  private String convertCollectionToString(Collection<?> collection) throws DatabricksSQLException {
    if (collection == null) {
      return "null";
    }
    StringBuilder sb = new StringBuilder("[");
    Iterator<?> iterator = collection.iterator();
    int index = 0;
    while (iterator.hasNext()) {
      if (index++ > 0) {
        sb.append(",");
      }
      sb.append(convertValueToString(iterator.next()));
    }
    sb.append("]");
    return sb.toString();
  }

  private String convertJavaArrayToString(Object array) throws DatabricksSQLException {
    StringBuilder sb = new StringBuilder("[");
    int length = Array.getLength(array);
    for (int i = 0; i < length; i++) {
      if (i > 0) {
        sb.append(",");
      }
      Object element = Array.get(array, i);
      sb.append(convertValueToString(element));
    }
    sb.append("]");
    return sb.toString();
  }

  private String convertValueToString(Object value) throws DatabricksSQLException {
    if (value == null) {
      return "null";
    }
    if (value instanceof String || value instanceof Character) {
      return "\"" + escapeString(value.toString()) + "\"";
    }
    if (value instanceof DatabricksArray
        || value instanceof DatabricksMap
        || value instanceof DatabricksStruct) {
      return value.toString();
    }
    if (value instanceof Struct) {
      return convertStructToString((Struct) value);
    }
    if (value instanceof java.sql.Array) {
      return convertSqlArrayToString((java.sql.Array) value);
    }
    if (value instanceof Map) {
      return convertMapToString((Map<?, ?>) value);
    }
    if (value instanceof Collection) {
      return convertCollectionToString((Collection<?>) value);
    }
    if (value.getClass().isArray()) {
      return convertJavaArrayToString(value);
    }
    return value.toString();
  }

  private String escapeString(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
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
