package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.util.*;

public class ConverterHelper {

  private static final Map<Integer, ObjectConverter> CONVERTER_CACHE = new HashMap<>();

  static {
    CONVERTER_CACHE.put(Types.TINYINT, new ByteConverter());
    CONVERTER_CACHE.put(Types.SMALLINT, new ShortConverter());
    CONVERTER_CACHE.put(Types.INTEGER, new IntConverter());
    CONVERTER_CACHE.put(Types.BIGINT, new LongConverter());
    CONVERTER_CACHE.put(Types.FLOAT, new FloatConverter());
    CONVERTER_CACHE.put(Types.DOUBLE, new DoubleConverter());
    CONVERTER_CACHE.put(Types.DECIMAL, new BigDecimalConverter());
    CONVERTER_CACHE.put(Types.BOOLEAN, new BooleanConverter());
    CONVERTER_CACHE.put(Types.DATE, new DateConverter());
    CONVERTER_CACHE.put(Types.TIMESTAMP, new TimestampConverter());
    CONVERTER_CACHE.put(Types.BINARY, new ByteArrayConverter());
    CONVERTER_CACHE.put(Types.BIT, new BitConverter());
    CONVERTER_CACHE.put(Types.VARCHAR, new StringConverter());
    CONVERTER_CACHE.put(Types.CHAR, new StringConverter());
  }

  /**
   * Converts a SQL object to the appropriate Java object based on the SQL type.
   *
   * @param columnSqlType The SQL type of the column, as defined in java.sql.Types
   * @param object The object to be converted
   * @return The converted Java object
   * @throws DatabricksSQLException If there's an error during the conversion process
   */
  public static Object convertSqlTypeToJavaType(int columnSqlType, Object object)
      throws DatabricksSQLException {
    switch (columnSqlType) {
      case Types.TINYINT:
        // specific java type, sql type, object
        return convertSqlTypeToSpecificJavaType(Byte.class, Types.TINYINT, object);
      case Types.SMALLINT:
        return convertSqlTypeToSpecificJavaType(Short.class, Types.SMALLINT, object);
      case Types.INTEGER:
        return convertSqlTypeToSpecificJavaType(Integer.class, Types.INTEGER, object);
      case Types.BIGINT:
        return convertSqlTypeToSpecificJavaType(Long.class, Types.BIGINT, object);
      case Types.FLOAT:
        return convertSqlTypeToSpecificJavaType(Float.class, Types.FLOAT, object);
      case Types.DOUBLE:
        return convertSqlTypeToSpecificJavaType(Double.class, Types.DOUBLE, object);
      case Types.DECIMAL:
        return convertSqlTypeToSpecificJavaType(BigDecimal.class, Types.DECIMAL, object);
      case Types.BOOLEAN:
        return convertSqlTypeToSpecificJavaType(Boolean.class, Types.BOOLEAN, object);
      case Types.DATE:
        return convertSqlTypeToSpecificJavaType(Date.class, Types.DATE, object);
      case Types.TIMESTAMP:
        return convertSqlTypeToSpecificJavaType(Timestamp.class, Types.TIMESTAMP, object);
      case Types.BINARY:
        return convertSqlTypeToSpecificJavaType(byte[].class, Types.BINARY, object);
      case Types.BIT:
        return convertSqlTypeToSpecificJavaType(Boolean.class, Types.BIT, object);
      case Types.VARCHAR:
      case Types.CHAR:
      default:
        return convertSqlTypeToSpecificJavaType(String.class, Types.VARCHAR, object);
    }
  }

  /**
   * Converts an object to a specific Java type based on the provided SQL type and desired Java
   * class.
   *
   * @param javaType The Class object representing the desired Java type
   * @param columnSqlType The SQL type of the column, as defined in java.sql.Types
   * @param obj The object to be converted
   * @return The converted object of the specified Java type
   * @throws DatabricksSQLException If there's an error during the conversion process
   */
  public static Object convertSqlTypeToSpecificJavaType(
      Class<?> javaType, int columnSqlType, Object obj) throws DatabricksSQLException {
    // Get the appropriate converter for the SQL type
    ObjectConverter converter = getConverterForSqlType(columnSqlType);
    if (javaType == String.class) {
      return converter.toString(obj);
    } else if (javaType == BigDecimal.class) {
      return converter.toBigDecimal(obj);
    } else if (javaType == Boolean.class || javaType == boolean.class) {
      return converter.toBoolean(obj);
    } else if (javaType == Integer.class || javaType == int.class) {
      return converter.toInt(obj);
    } else if (javaType == Long.class || javaType == long.class) {
      return converter.toLong(obj);
    } else if (javaType == Float.class || javaType == float.class) {
      return converter.toFloat(obj);
    } else if (javaType == Double.class || javaType == double.class) {
      return converter.toDouble(obj);
    } else if (javaType == LocalDate.class) {
      return converter.toLocalDate(obj);
    } else if (javaType == BigInteger.class) {
      return converter.toBigInteger(obj);
    } else if (javaType == Date.class || javaType == java.sql.Date.class) {
      return converter.toDate(obj);
    } else if (javaType == Timestamp.class || javaType == Calendar.class) {
      return converter.toTimestamp(obj);
    } else if (javaType == byte.class || javaType == Byte.class) {
      return converter.toByte(obj);
    } else if (javaType == short.class || javaType == Short.class) {
      return converter.toShort(obj);
    } else if (javaType == byte[].class) {
      return converter.toByteArray(obj);
    } else if (javaType == char.class || javaType == Character.class) {
      return converter.toChar(obj);
    }
    return converter.toString(obj); // By default, convert to string
  }

  /**
   * Retrieves the appropriate ObjectConverter for a given SQL type.
   *
   * @param columnSqlType The SQL type of the column, as defined in java.sql.Types
   * @return An ObjectConverter suitable for the specified SQL type
   */
  public static ObjectConverter getConverterForSqlType(int columnSqlType) {
    return CONVERTER_CACHE.getOrDefault(columnSqlType, CONVERTER_CACHE.get(Types.VARCHAR));
  }
}
