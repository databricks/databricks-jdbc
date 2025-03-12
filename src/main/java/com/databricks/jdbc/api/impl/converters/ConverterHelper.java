package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.LocalDate;
import java.util.*;
import java.util.Date;

public class ConverterHelper {

  private static final Map<Integer, ObjectConverter> CONVERTER_CACHE = new HashMap<>();
  private static final Map<Integer, List<Integer>> SUPPORTED_CONVERSIONS = new HashMap<>();

  static {
    // Numeric Types
    SUPPORTED_CONVERSIONS.put(
        Types.TINYINT,
        List.of(
            Types.TINYINT,
            Types.SMALLINT,
            Types.INTEGER,
            Types.BIGINT,
            Types.DECIMAL,
            Types.DOUBLE,
            Types.REAL,
            Types.FLOAT,
            Types.CHAR,
            Types.NUMERIC,
            Types.VARCHAR,
            Types.NVARCHAR,
            Types.BINARY,
            Types.VARBINARY,
            Types.BIT,
            Types.LONGVARCHAR));
    SUPPORTED_CONVERSIONS.put(
        Types.SMALLINT,
        List.of(
            Types.SMALLINT,
            Types.INTEGER,
            Types.BIGINT,
            Types.DECIMAL,
            Types.DOUBLE,
            Types.REAL,
            Types.VARCHAR,
            Types.LONGVARCHAR,
            Types.BINARY,
            Types.NUMERIC,
            Types.VARBINARY,
            Types.NVARCHAR,
            Types.TINYINT,
            Types.BIT,
            Types.CHAR,
            Types.FLOAT,
            Types.BINARY,
            Types.NUMERIC,
            Types.LONGVARCHAR));
    SUPPORTED_CONVERSIONS.put(
        Types.INTEGER,
        List.of(
            Types.INTEGER,
            Types.BIGINT,
            Types.DECIMAL,
            Types.DOUBLE,
            Types.REAL,
            Types.NVARCHAR,
            Types.SMALLINT,
            Types.CHAR,
            Types.LONGVARCHAR,
            Types.VARBINARY,
            Types.TINYINT,
            Types.VARCHAR,
            Types.BINARY,
            Types.BIT,
            Types.FLOAT,
            Types.NUMERIC));
    SUPPORTED_CONVERSIONS.put(
        Types.BIGINT,
        List.of(
            Types.BIGINT,
            Types.DECIMAL,
            Types.DOUBLE,
            Types.REAL,
            Types.NVARCHAR,
            Types.BINARY,
            Types.FLOAT,
            Types.VARCHAR,
            Types.TINYINT,
            Types.NUMERIC,
            Types.LONGVARCHAR,
            Types.INTEGER,
            Types.CHAR,
            Types.VARBINARY,
            Types.SMALLINT,
            Types.BIT));
    SUPPORTED_CONVERSIONS.put(
        Types.FLOAT,
        List.of(
            Types.DOUBLE,
            Types.DECIMAL,
            Types.REAL,
            Types.FLOAT,
            Types.TINYINT,
            Types.BINARY,
            Types.VARCHAR,
            Types.BIT,
            Types.NUMERIC,
            Types.NVARCHAR,
            Types.CHAR,
            Types.LONGVARCHAR,
            Types.INTEGER,
            Types.SMALLINT,
            Types.VARBINARY,
            Types.BIGINT));
    SUPPORTED_CONVERSIONS.put(
        Types.REAL,
        List.of(
            Types.REAL,
            Types.DOUBLE,
            Types.DECIMAL,
            Types.VARCHAR,
            Types.NVARCHAR,
            Types.VARBINARY,
            Types.NUMERIC,
            Types.SMALLINT,
            Types.BINARY,
            Types.INTEGER,
            Types.CHAR,
            Types.BIGINT,
            Types.TINYINT,
            Types.BIT,
            Types.LONGVARCHAR,
            Types.FLOAT));
    SUPPORTED_CONVERSIONS.put(
        Types.DOUBLE,
        List.of(
            Types.DOUBLE,
            Types.DECIMAL,
            Types.REAL,
            Types.FLOAT,
            Types.TINYINT,
            Types.BINARY,
            Types.VARCHAR,
            Types.BIT,
            Types.NUMERIC,
            Types.NVARCHAR,
            Types.CHAR,
            Types.LONGVARCHAR,
            Types.INTEGER,
            Types.SMALLINT,
            Types.VARBINARY,
            Types.BIGINT));
    SUPPORTED_CONVERSIONS.put(
        Types.DECIMAL,
        List.of(
            Types.DECIMAL,
            Types.NUMERIC,
            Types.DOUBLE,
            Types.REAL,
            Types.TINYINT,
            Types.VARCHAR,
            Types.NVARCHAR,
            Types.SMALLINT,
            Types.INTEGER,
            Types.BIGINT,
            Types.CHAR,
            Types.FLOAT));
    SUPPORTED_CONVERSIONS.put(
        Types.NUMERIC,
        List.of(
            Types.NUMERIC,
            Types.DECIMAL,
            Types.DOUBLE,
            Types.REAL,
            Types.CHAR,
            Types.NVARCHAR,
            Types.BIGINT,
            Types.FLOAT,
            Types.VARCHAR,
            Types.SMALLINT,
            Types.TINYINT,
            Types.INTEGER));

    // Boolean/Bit Types
    SUPPORTED_CONVERSIONS.put(
        Types.BOOLEAN,
        List.of(
            Types.BOOLEAN,
            Types.BIT,
            Types.INTEGER,
            Types.VARCHAR,
            Types.REAL,
            Types.DECIMAL,
            Types.BINARY,
            Types.LONGVARCHAR,
            Types.VARBINARY,
            Types.INTEGER,
            Types.FLOAT,
            Types.SMALLINT,
            Types.NUMERIC,
            Types.BIGINT,
            Types.NVARCHAR,
            Types.DOUBLE,
            Types.CHAR,
            Types.TINYINT,
            Types.LONGVARBINARY));
    SUPPORTED_CONVERSIONS.put(
        Types.BIT,
        List.of(
            Types.BIT,
            Types.INTEGER,
            Types.VARCHAR,
            Types.BINARY,
            Types.DECIMAL,
            Types.DOUBLE,
            Types.CHAR,
            Types.NUMERIC,
            Types.TINYINT,
            Types.REAL,
            Types.LONGVARBINARY,
            Types.BIGINT,
            Types.FLOAT,
            Types.SMALLINT,
            Types.NVARCHAR,
            Types.VARBINARY,
            Types.LONGVARCHAR));

    // Date/Time TypesT
    SUPPORTED_CONVERSIONS.put(
        Types.DATE,
        List.of(
            Types.DATE,
            Types.TIMESTAMP,
            Types.VARCHAR,
            Types.VARBINARY,
            Types.NVARCHAR,
            Types.BINARY,
            Types.LONGVARCHAR,
            Types.CHAR));
    SUPPORTED_CONVERSIONS.put(
        Types.TIME,
        List.of(
            Types.TIME,
            Types.TIMESTAMP,
            Types.VARCHAR,
            Types.VARBINARY,
            Types.LONGVARCHAR,
            Types.CHAR,
            Types.NVARCHAR,
            Types.BINARY));
    SUPPORTED_CONVERSIONS.put(
        Types.TIMESTAMP,
        List.of(
            Types.TIMESTAMP,
            Types.DATE,
            Types.TIME,
            Types.VARCHAR,
            Types.BINARY,
            Types.NVARCHAR,
            Types.CHAR,
            Types.VARBINARY,
            Types.LONGVARCHAR));

    // Binary Types
    SUPPORTED_CONVERSIONS.put(
        Types.BINARY,
        List.of(
            Types.BINARY,
            Types.VARBINARY,
            Types.LONGVARBINARY,
            Types.VARCHAR,
            Types.LONGVARCHAR,
            Types.NVARCHAR,
            Types.CHAR));
    SUPPORTED_CONVERSIONS.put(
        Types.VARBINARY,
        List.of(
            Types.VARBINARY,
            Types.LONGVARBINARY,
            Types.CHAR,
            Types.LONGVARCHAR,
            Types.NVARCHAR,
            Types.BINARY,
            Types.VARCHAR));
    SUPPORTED_CONVERSIONS.put(
        Types.LONGVARBINARY,
        List.of(
            Types.LONGVARBINARY,
            Types.BINARY,
            Types.VARBINARY,
            Types.VARCHAR,
            Types.NVARCHAR,
            Types.CHAR,
            Types.LONGVARCHAR));

    // Character Types
    SUPPORTED_CONVERSIONS.put(
        Types.CHAR,
        List.of(
            Types.CHAR,
            Types.VARCHAR,
            Types.LONGVARCHAR,
            Types.TIMESTAMP,
            Types.FLOAT,
            Types.NVARCHAR,
            Types.TINYINT,
            Types.BINARY,
            Types.VARBINARY,
            Types.DATE,
            Types.LONGVARBINARY,
            Types.INTEGER,
            Types.SMALLINT,
            Types.DECIMAL,
            Types.REAL,
            Types.DOUBLE,
            Types.BIGINT,
            Types.NUMERIC,
            Types.BIT,
            Types.TIME));
    SUPPORTED_CONVERSIONS.put(
        Types.VARCHAR,
        List.of(
            Types.VARCHAR,
            Types.CHAR,
            Types.LONGVARCHAR,
            Types.NVARCHAR,
            Types.TIMESTAMP,
            Types.DECIMAL,
            Types.BIT,
            Types.BINARY,
            Types.SMALLINT,
            Types.BIGINT,
            Types.FLOAT,
            Types.TIME,
            Types.INTEGER,
            Types.LONGVARBINARY,
            Types.DATE,
            Types.VARBINARY,
            Types.NUMERIC,
            Types.REAL,
            Types.TINYINT,
            Types.DOUBLE));
    SUPPORTED_CONVERSIONS.put(
        Types.LONGVARCHAR,
        List.of(
            Types.LONGVARCHAR,
            Types.VARCHAR,
            Types.NVARCHAR,
            Types.NUMERIC,
            Types.INTEGER,
            Types.BIGINT,
            Types.DECIMAL,
            Types.TIME,
            Types.SMALLINT,
            Types.DATE,
            Types.BIT,
            Types.TINYINT,
            Types.CHAR,
            Types.DOUBLE,
            Types.FLOAT,
            Types.REAL,
            Types.TIMESTAMP));
    SUPPORTED_CONVERSIONS.put(
        Types.NVARCHAR,
        List.of(
            Types.NVARCHAR,
            Types.VARCHAR,
            Types.TIMESTAMP,
            Types.SMALLINT,
            Types.BINARY,
            Types.BIT,
            Types.INTEGER,
            Types.VARBINARY,
            Types.LONGVARCHAR,
            Types.DATE,
            Types.DECIMAL,
            Types.CHAR,
            Types.FLOAT,
            Types.DOUBLE,
            Types.TINYINT,
            Types.REAL,
            Types.NUMERIC,
            Types.BIGINT,
            Types.TIME,
            Types.LONGVARBINARY));

    // Complex types
    SUPPORTED_CONVERSIONS.put(Types.OTHER, List.of(Types.OTHER));
    SUPPORTED_CONVERSIONS.put(Types.STRUCT, List.of(Types.STRUCT, Types.VARCHAR));
    SUPPORTED_CONVERSIONS.put(Types.ARRAY, List.of(Types.ARRAY, Types.VARCHAR));
  }

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
    CONVERTER_CACHE.put(Types.TIME, new TimestampConverter());
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
      case Types.TIME:
        return convertSqlTypeToSpecificJavaType(Time.class, Types.TIME, object);
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
    } else if (javaType == Time.class) {
      return converter.toTime(obj);
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
    } else if (javaType == Map.class) {
      return converter.toDatabricksMap(obj);
    } else if (javaType == Array.class) {
      return converter.toDatabricksArray(obj);
    } else if (javaType == Struct.class) {
      return converter.toDatabricksStruct(obj);
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

  public static boolean isConversionSupported(int fromType, int toType) {
    return SUPPORTED_CONVERSIONS.containsKey(fromType)
        && SUPPORTED_CONVERSIONS.get(fromType).contains(toType);
  }
}
