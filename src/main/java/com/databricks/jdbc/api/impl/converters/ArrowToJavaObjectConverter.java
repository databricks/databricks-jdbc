package com.databricks.jdbc.api.impl.converters;

import static com.databricks.jdbc.common.util.DatabricksTypeUtil.*;

import com.databricks.jdbc.api.IDatabricksGeospatial;
import com.databricks.jdbc.api.impl.*;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.core.ColumnInfo;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
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
import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
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
    return convert(columnVector, vectorIndex, requiredType, arrowMetadata, columnInfo, true);
  }

  public static Object convert(
      ValueVector columnVector,
      int vectorIndex,
      ColumnInfoTypeName requiredType,
      String arrowMetadata,
      ColumnInfo columnInfo,
      boolean geoSpatialSupportEnabled)
      throws DatabricksSQLException {
    // check isNull before getting the object from the vector
    if (columnVector.isNull(vectorIndex)) {
      return null;
    }
    Object object = columnVector.getObject(vectorIndex);
    ColumnInfoTypeName logicalGeospatialType = getLogicalGeospatialType(requiredType, columnInfo);
    boolean geospatialAsString =
        logicalGeospatialType != null && requiredType == ColumnInfoTypeName.STRING;

    // The structured result manifest is the logical authority for geospatial columns. Reyden's
    // physical Arrow vector is a struct<srid,wkb>, which must not override GEOMETRY/GEOGRAPHY to
    // an ordinary STRUCT merely because the Arrow field metadata describes its physical shape.
    if (logicalGeospatialType != null && !geospatialAsString) {
      requiredType = logicalGeospatialType;
    } else if (arrowMetadata != null && logicalGeospatialType == null) {
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
      // A collated string column (e.g. "STRING COLLATE UTF8_LCASE") is reported with a type_name
      // that does not map to any ColumnInfoTypeName, leaving requiredType null. Recover the type
      // from the metadata so the value is read as a string instead of throwing an NPE below.
      if (requiredType == null) {
        requiredType = recoverStringType(arrowMetadata);
      }
    }
    if (object == null) {
      return null;
    }
    if (requiredType == null) {
      // Do not log the raw cell value, which may contain sensitive data; log only the metadata.
      String errorMessage =
          String.format("Unable to determine column type from metadata %s", arrowMetadata);
      LOGGER.error(errorMessage);
      throw new DatabricksValidationException(errorMessage);
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
        return parserFor(columnVector, arrowMetadata, geoSpatialSupportEnabled)
            .parseJsonStringToDbStruct(object.toString(), arrowMetadata);
      case ARRAY:
        return parserFor(columnVector, arrowMetadata, geoSpatialSupportEnabled)
            .parseJsonStringToDbArray(object.toString(), arrowMetadata);
      case MAP:
        return parserFor(columnVector, arrowMetadata, geoSpatialSupportEnabled)
            .parseJsonStringToDbMap(object.toString(), arrowMetadata);
      case STRING:
        if (logicalGeospatialType != null && object instanceof java.util.Map<?, ?>) {
          IDatabricksGeospatial geospatial = convertNativeGeospatial(object, logicalGeospatialType);
          return GeospatialConverter.formatStringFallback(geospatial);
        }
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

  private static ColumnInfoTypeName getLogicalGeospatialType(
      ColumnInfoTypeName requiredType, ColumnInfo columnInfo) {
    ColumnInfoTypeName manifestType = columnInfo == null ? null : columnInfo.getTypeName();
    if (manifestType == ColumnInfoTypeName.GEOMETRY
        || manifestType == ColumnInfoTypeName.GEOGRAPHY) {
      return manifestType;
    }
    if (requiredType == ColumnInfoTypeName.GEOMETRY
        || requiredType == ColumnInfoTypeName.GEOGRAPHY) {
      return requiredType;
    }
    return null;
  }

  private static IDatabricksGeospatial convertNativeGeospatial(
      Object object, ColumnInfoTypeName geospatialType) throws DatabricksSQLException {
    if (geospatialType == ColumnInfoTypeName.GEOMETRY) {
      return ConverterHelper.getConverterForColumnType(Types.OTHER, GEOMETRY)
          .toDatabricksGeometry(object);
    }
    return ConverterHelper.getConverterForColumnType(Types.OTHER, GEOGRAPHY)
        .toDatabricksGeography(object);
  }

  /**
   * Returns the parser for an ARRAY, STRUCT or MAP value. Arrow serializes nested TIMESTAMP values
   * to JSON as epoch microseconds, which drops the time zone of their field, so the zone is passed
   * to the parser for the {@code java.time} values.
   */
  private static ComplexDataTypeParser parserFor(
      ValueVector columnVector, String arrowMetadata, boolean geoSpatialSupportEnabled) {
    return new ComplexDataTypeParser(
        geoSpatialSupportEnabled, nestedTimestampZone(columnVector, arrowMetadata));
  }

  /**
   * Returns the time zone of the first TIMESTAMP field nested in a complex column, which is the
   * session time zone, or null if it has none or the zone cannot be read.
   */
  static ZoneId nestedTimestampZone(ValueVector columnVector, String arrowMetadata) {
    if (arrowMetadata != null && !arrowMetadata.toUpperCase(Locale.ROOT).contains(TIMESTAMP)) {
      return null; // no nested TIMESTAMP; skip walking the field tree
    }
    String timeZone = findTimestampTimeZone(columnVector.getField());
    if (timeZone == null) {
      return null;
    }
    try {
      return getZoneIdFromTimeZoneOpt(Optional.of(timeZone));
    } catch (DateTimeException | NumberFormatException e) {
      LOGGER.debug("Cannot read the nested TIMESTAMP time zone; using UTC for java.time values");
      return null;
    }
  }

  private static String findTimestampTimeZone(Field field) {
    if (field.getType() instanceof ArrowType.Timestamp) {
      String timeZone = ((ArrowType.Timestamp) field.getType()).getTimezone();
      if (timeZone != null) {
        return timeZone;
      }
    }
    for (Field child : field.getChildren()) {
      String timeZone = findTimestampTimeZone(child);
      if (timeZone != null) {
        return timeZone;
      }
    }
    return null;
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
    Instant instant = instantFromEpochMicros(timeMicros);
    ZoneId zoneId = getZoneIdFromTimeZoneOpt(timeZoneOpt);
    LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, zoneId);
    return Timestamp.valueOf(localDateTime);
  }

  private static Instant instantFromEpochMicros(long epochMicros) {
    return Instant.ofEpochSecond(epochMicros / 1000_000, (epochMicros % 1000_000) * 1000);
  }

  /**
   * Returns true if {@link #convertToJavaTime} can read values from the vector. These vector types
   * are only used for DATE, TIMESTAMP and TIMESTAMP_NTZ columns.
   *
   * @param columnVector an Arrow column vector
   * @return true for the vector types the server uses for DATE, TIMESTAMP and TIMESTAMP_NTZ
   */
  public static boolean isJavaTimeVector(ValueVector columnVector) {
    return columnVector instanceof DateDayVector
        || columnVector instanceof TimeStampMicroTZVector
        || columnVector instanceof TimeStampMicroVector;
  }

  /**
   * Reads a DATE or TIMESTAMP value straight from the Arrow vector, for {@link JavaTimeConverter}.
   *
   * <p>The server sends proleptic Gregorian epoch days and epoch microseconds. Unlike {@link
   * #convert}, this does not build {@link Date} or {@link Timestamp} values, which use the hybrid
   * Julian/Gregorian calendar and cannot represent BC or pre-1582 values exactly.
   *
   * @param columnVector a vector accepted by {@link #isJavaTimeVector}
   * @param vectorIndex the row index in the vector
   * @return a {@link LocalDate} for DATE, a {@link LocalDateTime} for TIMESTAMP_NTZ, an {@link
   *     OffsetDateTime} in the vector's (session) time zone for TIMESTAMP, or null for SQL NULL
   * @throws DatabricksSQLException with {@code UNSUPPORTED_OPERATION} if the vector type is not
   *     supported
   */
  public static Object convertToJavaTime(ValueVector columnVector, int vectorIndex)
      throws DatabricksSQLException {
    if (columnVector.isNull(vectorIndex)) {
      return null;
    }
    if (columnVector instanceof DateDayVector) {
      return LocalDate.ofEpochDay(((DateDayVector) columnVector).get(vectorIndex));
    }
    if (columnVector instanceof TimeStampMicroTZVector) {
      TimeStampMicroTZVector timestampVector = (TimeStampMicroTZVector) columnVector;
      ZoneId zoneId = getZoneIdFromTimeZoneOpt(Optional.ofNullable(timestampVector.getTimeZone()));
      return instantFromEpochMicros(timestampVector.get(vectorIndex))
          .atZone(zoneId)
          .toOffsetDateTime();
    }
    if (columnVector instanceof TimeStampMicroVector) {
      // Arrow builds this LocalDateTime from the epoch microseconds with java.time, so it is exact.
      return ((TimeStampMicroVector) columnVector).getObject(vectorIndex);
    }
    String errorMessage =
        String.format(
            "Unsupported Arrow vector %s for java.time conversion",
            columnVector.getClass().getSimpleName());
    LOGGER.error(errorMessage);
    throw new DatabricksSQLException(
        errorMessage,
        DatabricksDriverErrorCode.UNSUPPORTED_OPERATION.name(),
        DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
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
