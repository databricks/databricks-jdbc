package com.databricks.jdbc.core;

import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;

public class ArrowToJavaObjectConverter {
  // TODO (Madhav): Check Arrow to JSON conversion
  public static Object convert(Object object, ColumnInfoTypeName requiredType)
      throws DatabricksSQLException {
    switch (requiredType) {
      case BYTE:
        return convertToByte(object);
      case SHORT:
        return convertToShort(object);
      case INT:
        return convertToInteger(object);
      case LONG:
        return convertToLong(object);
      case FLOAT:
        return convertToFloat(object);
      case DOUBLE:
        return convertToDouble(object);
      case DECIMAL:
        return convertToBigDecimal(object);
      case BINARY:
        return convertToByteArray(object);
      case BOOLEAN:
        return convertToBoolean(object);
      case CHAR:
        return convertToChar(object);
      case STRING:
        // Struct and Array are present in Arrow data in the VARCHAR ValueVector format
      case STRUCT:
      case ARRAY:
        return convertToString(object);
      case DATE:
        return convertToDate(object);
      case TIMESTAMP:
        return convertToTimestamp(object);
      case NULL:
        return null;
      default:
        throw new DatabricksSQLException("Unsupported type");
    }
  }

  private static Object convertToTimestamp(Object object) throws DatabricksSQLException {

    Instant instant =
        Instant.ofEpochMilli(object instanceof Integer ? (int) object : (long) object);
    return Timestamp.from(instant);
  }

  private static Date convertToDate(Object object) throws DatabricksSQLException {
    LocalDate localDate = LocalDate.ofEpochDay((int) object);
    return Date.valueOf(localDate);
  }

  private static char convertToChar(Object object) throws DatabricksSQLException {
    return (object.toString()).charAt(0);
  }

  private static String convertToString(Object object) throws DatabricksSQLException {
    return object.toString();
  }

  private static boolean convertToBoolean(Object object) throws DatabricksSQLException {
    return (boolean) object;
  }

  private static byte[] convertToByteArray(Object object) throws DatabricksSQLException {
    return (byte[]) object;
  }

  private static byte convertToByte(Object object) throws DatabricksSQLException {
    return (byte) object;
  }

  private static short convertToShort(Object object) throws DatabricksSQLException {
    return (short) object;
  }

  private static BigDecimal convertToBigDecimal(Object object) throws DatabricksSQLException {
    return (BigDecimal) object;
  }

  private static double convertToDouble(Object object) throws DatabricksSQLException {
    return (double) object;
  }

  private static float convertToFloat(Object object) throws DatabricksSQLException {
    return (float) object;
  }

  private static int convertToInteger(Object object) throws DatabricksSQLException {
    return (int) object;
  }

  private static long convertToLong(Object object) throws DatabricksSQLException {
    return (long) object;
  }
}
