package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
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
import java.util.function.Function;
import org.apache.arrow.vector.util.Text;

public class ArrowToJavaObjectConverter {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(ArrowToJavaObjectConverter.class);
  private static final List<DateTimeFormatter> DATE_FORMATTERS =
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

  public static Object convert(Object object, ColumnInfoTypeName requiredType)
      throws DatabricksSQLException {
    if (object == null) {
      return null;
    }
    switch (requiredType) {
      case BYTE:
        return convertToNumber(object, Byte::parseByte, Number::byteValue);
      case SHORT:
        return convertToNumber(object, Short::parseShort, Number::shortValue);
      case INT:
        return convertToNumber(object, Integer::parseInt, Number::intValue);
      case LONG:
        return convertToNumber(object, Long::parseLong, Number::longValue);
      case FLOAT:
        return convertToNumber(object, Float::parseFloat, Number::floatValue);
      case DOUBLE:
        return convertToNumber(object, Double::parseDouble, Number::doubleValue);
      case DECIMAL:
        return convertToNumber(
            object, BigDecimal::new, num -> BigDecimal.valueOf(num.doubleValue()));
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
        String errorMessage = String.format("Unsupported conversion type %s", requiredType);
        LOGGER.error(errorMessage);
        throw new DatabricksValidationException(errorMessage);
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
    String errorMessage = String.format("Unsupported text for date conversion: %s", text);
    LOGGER.error(errorMessage);
    throw new DatabricksValidationException(errorMessage);
  }

  private static Date convertToDate(Object object) throws DatabricksSQLException {
    if (object instanceof Text) {
      LocalDateTime localDateTime = parseDate(object.toString());
      return java.sql.Date.valueOf(localDateTime.toLocalDate());
    }
    LocalDate localDate = LocalDate.ofEpochDay((int) object);
    return Date.valueOf(localDate);
  }

  private static char convertToChar(Object object) {
    return (object.toString()).charAt(0);
  }

  private static String convertToString(Object object) {
    return object.toString();
  }

  private static boolean convertToBoolean(Object object) {
    if (object instanceof Text) {
      return Boolean.parseBoolean(object.toString());
    }
    return (boolean) object;
  }

  private static byte[] convertToByteArray(Object object) {
    if (object instanceof Text) {
      return object.toString().getBytes();
    }
    return (byte[]) object;
  }

  private static <T extends Number> T convertToNumber(
      Object object, Function<String, T> parseFunc, Function<Number, T> convertFunc)
      throws DatabricksSQLException {
    if (object instanceof Text) {
      return parseFunc.apply(object.toString());
    }
    if (object instanceof Number) {
      return convertFunc.apply((Number) object);
    }
    String errorMessage =
        String.format("Unsupported object type for number conversion: %s", object.getClass());
    LOGGER.error(errorMessage);
    throw new DatabricksValidationException(errorMessage);
  }
}
