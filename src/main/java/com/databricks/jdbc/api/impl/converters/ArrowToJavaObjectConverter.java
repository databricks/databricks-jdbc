package com.databricks.jdbc.api.impl.converters;

import static com.databricks.jdbc.common.util.DatabricksTypeUtil.*;

import com.databricks.jdbc.api.impl.*;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.core.ColumnInfo;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.ValueVector;
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

  public static Object convert(
      ValueVector columnVector,
      int vectorIndex,
      ColumnInfoTypeName requiredType,
      String arrowMetadata,
      ColumnInfo columnInfo)
      throws DatabricksSQLException {
    // check isNull before getting the object from the vector
    if (columnVector.isNull(vectorIndex)) {
      return null;
    }
    Object object = columnVector.getObject(vectorIndex);
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
      if (arrowMetadata.startsWith(GEOMETRY)) {
        requiredType = ColumnInfoTypeName.GEOMETRY;
      }
      if (arrowMetadata.startsWith(GEOGRAPHY)) {
        requiredType = ColumnInfoTypeName.GEOGRAPHY;
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
        return convertToDecimal(object, columnInfo);
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
        Optional<String> timeZone = Optional.empty();
        if (columnVector instanceof TimeStampMicroTZVector) {
          timeZone = Optional.of(((TimeStampMicroTZVector) columnVector).getTimeZone());
        }
        return convertToTimestamp(object, timeZone);
      case INTERVAL:
        if (arrowMetadata == null) {
          String errorMessage =
              String.format("Failed to read INTERVAL %s with null metadata.", object);
          LOGGER.error(errorMessage);
          throw new DatabricksValidationException(errorMessage);
        }
        IntervalConverter ic = new IntervalConverter(arrowMetadata);
        return ic.toLiteral(object);
      case GEOMETRY:
        return ConverterHelper.getConverterForColumnType(Types.OTHER, GEOMETRY)
            .toDatabricksGeometry(object);
      case GEOGRAPHY:
        return ConverterHelper.getConverterForColumnType(Types.OTHER, GEOGRAPHY)
            .toDatabricksGeography(object);
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

  private static Object convertToTimestamp(Object object, Optional<String> timeZoneOpt)
      throws DatabricksSQLException {
    if (object instanceof Text) {
      return convertArrowTextToTimestamp(object.toString());
    }
    if (object instanceof java.time.LocalDateTime) {
      // timestamp_ntz result is returned as local date time
      return Timestamp.valueOf((LocalDateTime) object);
    }
    long timeMicros = object instanceof Integer ? ((int) object) : ((long) object);
    Instant instant = Instant.ofEpochSecond(timeMicros / 1000_000, (timeMicros % 1000_000) * 1000);
    ZoneId zoneId = getZoneIdFromTimeZoneOpt(timeZoneOpt);
    LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, zoneId);
    return Timestamp.valueOf(localDateTime);
  }

  /**
   * For TIMESTAMP columns, timeZone will be in the form 'Asia/Kolkata' or '+5:30'. For
   * TIMESTAMP_NTZ columns, timeZoneOpt will be empty
   *
   * @param timeZoneOpt to fetch the zoneId for
   * @return the zone ID for the timezone opt
   */
  static ZoneId getZoneIdFromTimeZoneOpt(Optional<String> timeZoneOpt) {
    if (timeZoneOpt.isPresent()) {
      String tz = timeZoneOpt.get();
      try {
        // Try standard parsing first
        return ZoneId.of(tz);
      } catch (DateTimeException e) {
        if (tz.matches("[+-]\\d+:\\d+")) {
          // Parse custom format like +4:15
          boolean isNegative = tz.startsWith("-");
          String[] parts = tz.substring(1).split(":");
          int hours = Integer.parseInt(parts[0]);
          int minutes = Integer.parseInt(parts[1]);

          // Always pass positive values and use the sign parameter
          return ZoneOffset.ofHoursMinutes(
              isNegative ? -hours : hours, isNegative ? -minutes : minutes);
        } else {
          throw e;
        }
      }
    }
    // This will happen when reading timestamp_ntz columns
    return ZoneId.systemDefault();
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

  static BigDecimal convertToDecimal(Object object, ColumnInfo columnInfo)
      throws DatabricksValidationException {
    if (object instanceof Text || object instanceof Number) {
      BigDecimal bigDecimal = new BigDecimal(object.toString());
      Optional<Integer> bigDecimalScale =
          columnInfo.getTypeScale() != null
              ? Optional.of(columnInfo.getTypeScale().intValue())
              : Optional.empty();
      return bigDecimalScale
          .map(scale -> bigDecimal.setScale(scale, RoundingMode.HALF_UP))
          .orElse(bigDecimal);
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
