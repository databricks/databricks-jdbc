package com.databricks.jdbc.core.converters;

import com.databricks.jdbc.core.DatabricksSQLException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;

public class ConverterHelper {

  public static Object getConvertedObject(Class<?> javaType, AbstractObjectConverter converter)
      throws DatabricksSQLException {
    if (javaType == String.class) {
      return converter.convertToString();
    } else if (javaType == BigDecimal.class) {
      return converter.convertToBigDecimal();
    } else if (javaType == Boolean.class || javaType == boolean.class) {
      return converter.convertToBoolean();
    } else if (javaType == Integer.class || javaType == int.class) {
      return converter.convertToInt();
    } else if (javaType == Long.class || javaType == long.class) {
      return converter.convertToLong();
    } else if (javaType == Float.class || javaType == float.class) {
      return converter.convertToFloat();
    } else if (javaType == Double.class || javaType == double.class) {
      return converter.convertToDouble();
    } else if (javaType == Date.class) {
      return converter.convertToDate();
    } else if (javaType == Timestamp.class || javaType == Calendar.class) {
      return converter.convertToTimestamp();
    } else if (javaType == byte.class || javaType == Byte.class) {
      return converter.convertToShort();
    }
    // TODO : add more types if required.
    return Types.OTHER;
  }

  public static AbstractObjectConverter getObjectConverter(Object object, int columnType)
      throws DatabricksSQLException {
    switch (columnType) {
      case Types.TINYINT:
        return new ByteConverter(object);
      case Types.SMALLINT:
        return new ShortConverter(object);
      case Types.INTEGER:
        return new IntConverter(object);
      case Types.BIGINT:
        return new LongConverter(object);
      case Types.FLOAT:
        return new FloatConverter(object);
      case Types.DOUBLE:
        return new DoubleConverter(object);
      case Types.DECIMAL:
        return new BigDecimalConverter(object);
      case Types.BOOLEAN:
        return new BooleanConverter(object);
      case Types.VARCHAR:
      case Types.CHAR:
        return new StringConverter(object);
      case Types.DATE:
        return new DateConverter(object);
      case Types.TIMESTAMP:
        return new TimestampConverter(object);
      default:
        throw new DatabricksSQLException("Bad object type");
    }
  }
}
