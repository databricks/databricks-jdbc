package com.databricks.jdbc.api.impl.converters;

import static com.databricks.jdbc.common.util.DatabricksTypeUtil.ARRAY;
import static com.databricks.jdbc.common.util.DatabricksTypeUtil.MAP;
import static com.databricks.jdbc.common.util.DatabricksTypeUtil.STRUCT;
import static com.databricks.jdbc.common.util.DatabricksTypeUtil.TIMESTAMP;
import static com.databricks.jdbc.common.util.DatabricksTypeUtil.VARIANT;

import com.databricks.jdbc.api.impl.*;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import java.math.BigDecimal;
import java.math.RoundingMode;
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

  public static Object convert(Object object, ColumnInfoTypeName requiredType, String arrowMetadata)
      throws DatabricksSQLException {
    if (arrowMetadata != null) {
      if (arrowMetadata.startsWith(ARRAY)) {
        requiredType = ColumnInfoTypeName.ARRAY;
      }
      if (arrowMetadata.startsWith(STRUCT)) {
        requiredType = ColumnInfoTypeName.STRUCT;
      }
      if (arrowMetadata.startsWith(MAP)) {
        requiredType = ColumnInfoTypeName.MAP;
      }
      if (arrowMetadata.startsWith(VARIANT)) {
        requiredType = ColumnInfoTypeName.STRING;
      }
      if (arrowMetadata.startsWith(TIMESTAMP)) { // for timestamp_ntz column
        requiredType = ColumnInfoTypeName.TIMESTAMP;
      }
    }
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
        return convertToDecimal(object, arrowMetadata);
      case BINARY:
        return convertToByteArray(object);
      case BOOLEAN:
        return convertToBoolean(object);
      case CHAR:
        return convertToChar(object);
      case STRUCT:
        return convertToStruct(object, arrowMetadata);
      case ARRAY:
        return convertToArray(object, arrowMetadata);
      case MAP:
        return convertToMap(object, arrowMetadata);
      case STRING:
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

  private static DatabricksMap convertToMap(Object object, String arrowMetadata)
      throws DatabricksParsingException {
    ComplexDataTypeParser parser = new ComplexDataTypeParser();
    return parser.parseJsonStringToDbMap(object.toString(), arrowMetadata);
  }

  private static DatabricksArray convertToArray(Object object, String arrowMetadata)
      throws DatabricksParsingException {
    ComplexDataTypeParser parser = new ComplexDataTypeParser();
    return parser.parseJsonStringToDbArray(object.toString(), arrowMetadata);
  }

  private static Object convertToStruct(Object object, String arrowMetadata)
      throws DatabricksParsingException {
    ComplexDataTypeParser parser = new ComplexDataTypeParser();
    return parser.parseJsonStringToDbStruct(object.toString(), arrowMetadata);
  }

  private static Object convertToTimestamp(Object object) throws DatabricksSQLException {
    if (object instanceof Text) {
      return convertArrowTextToTimestamp(object.toString());
    }
    if (object instanceof java.time.LocalDateTime) {
      // timestamp_ntz result is returned as local date time
      return Timestamp.valueOf((LocalDateTime) object);
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

  static BigDecimal convertToDecimal(Object object, String arrowMetadata)
      throws DatabricksValidationException {
    if (object instanceof Text) {
      return new BigDecimal(object.toString());
    }
    int scale;
    try {
      scale =
          Integer.parseInt(
              arrowMetadata
                  .substring(arrowMetadata.indexOf(',') + 1, arrowMetadata.indexOf(')'))
                  .trim());
    } catch (Exception e) {
      scale = 0;
    }
    if (object instanceof Number) {
      return new BigDecimal(object.toString()).setScale(scale, RoundingMode.HALF_UP);
    }
    String errorMessage =
        String.format("Unsupported object type for decimal conversion: %s", object.getClass());
    LOGGER.error(errorMessage);
    throw new DatabricksValidationException(errorMessage);
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
