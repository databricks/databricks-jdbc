package com.databricks.jdbc.core;

import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.vector.util.Text;

public class ArrowToJavaObjectConverter {
  private static List<DateTimeFormatter> DATE_FORMATTERS =
      Arrays.asList(
          DateTimeFormatter.ofPattern("yyyy-MM-dd"),
          DateTimeFormatter.ofPattern("yyyy/MM/dd"),
          DateTimeFormatter.ofPattern("yyyy.MM.dd"),
          DateTimeFormatter.ofPattern("yyyyMMdd"),
          DateTimeFormatter.ofPattern("dd-MM-yyyy"),
          DateTimeFormatter.ofPattern("dd/MM/yyyy"),
          DateTimeFormatter.ofPattern("dd.MM.yyyy"),
          DateTimeFormatter.ofPattern("ddMMyyyy"),
          DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
          DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss"),
          DateTimeFormatter.ofPattern("yyyy.MM.dd HH:mm:ss"),
          DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss"),
          DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss"),
          DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss"),
          DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss"),
          DateTimeFormatter.ofPattern("ddMMyyyy HH:mm:ss"),
          DateTimeFormatter.ISO_LOCAL_DATE_TIME,
          DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"),
          DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
          DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
          DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SS"),
          DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S"),
          DateTimeFormatter.RFC_1123_DATE_TIME);

  // TODO (Madhav): Check Arrow to JSON conversion
  public static Object convert(Object object, ColumnInfoTypeName requiredType)
      throws DatabricksSQLException {
    if (object == null) {
      return null;
    }
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
      case MAP:
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
    if (object instanceof Text) {
      return convertArrowTextToTimestamp(object.toString());
    }
    // Divide by 1000 since we need to convert from microseconds to milliseconds.
    Instant instant =
        Instant.ofEpochMilli(
            object instanceof Integer ? ((int) object) / 1000 : ((long) object) / 1000);
    return Timestamp.from(instant);
  }

  private static Object convertArrowTextToTimestamp(String arrowText)
      throws DatabricksSQLException {
    LocalDateTime localDateTime = parseDate(arrowText);
    return Timestamp.valueOf(localDateTime);
  }

  private static LocalDateTime parseDate(String text) throws DatabricksSQLException {
    for (DateTimeFormatter formatter : DATE_FORMATTERS) {
      try {
        return LocalDateTime.parse(text, formatter);
      } catch (DateTimeParseException e) {
        // Continue to try the next format
      }
    }
    throw new DatabricksSQLException("Unsupported text for conversion: " + text);
  }

  private static Date convertToDate(Object object) throws DatabricksSQLException {
    if (object instanceof Text) {
      LocalDateTime localDateTime = parseDate(object.toString());
      return java.sql.Date.valueOf(localDateTime.toLocalDate());
    }
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
    if (object instanceof Text) {
      return Boolean.parseBoolean(object.toString());
    }
    return (boolean) object;
  }

  private static byte[] convertToByteArray(Object object) throws DatabricksSQLException {
    if (object instanceof Text) {
      return object.toString().getBytes();
    }
    return (byte[]) object;
  }

  private static byte convertToByte(Object object) throws DatabricksSQLException {
    if (object instanceof Text) {
      return Byte.parseByte(object.toString());
    }
    if (object instanceof Number) {
      return ((Number) object).byteValue();
    }
    return (byte) object;
  }

  private static short convertToShort(Object object) throws DatabricksSQLException {
    if (object instanceof Text) {
      return Short.parseShort(object.toString());
    }
    if (object instanceof Number) {
      return ((Number) object).shortValue();
    }
    return (short) object;
  }

  private static BigDecimal convertToBigDecimal(Object object) throws DatabricksSQLException {
    if (object instanceof Text) {
      return new BigDecimal(object.toString());
    }
    return (BigDecimal) object;
  }

  private static double convertToDouble(Object object) throws DatabricksSQLException {
    if (object instanceof Text) {
      return Double.parseDouble(object.toString());
    }
    if (object instanceof Number) {
      return ((Number) object).doubleValue();
    }
    return (double) object;
  }

  private static float convertToFloat(Object object) throws DatabricksSQLException {
    if (object instanceof Text) {
      return Float.parseFloat(object.toString());
    }
    if (object instanceof Number) {
      return ((Number) object).floatValue();
    }
    return (float) object;
  }

  private static int convertToInteger(Object object) throws DatabricksSQLException {
    if (object instanceof Text) {
      return Integer.parseInt(object.toString());
    }
    if (object instanceof Number) {
      return ((Number) object).intValue();
    }
    return (int) object;
  }

  private static long convertToLong(Object object) throws DatabricksSQLException {
    if (object instanceof Text) {
      return Long.parseLong(object.toString());
    }
    if (object instanceof Number) {
      return ((Number) object).longValue();
    }
    return (long) object;
  }
}
