package com.databricks.jdbc.api.impl.converters;

import static com.databricks.jdbc.common.util.DatabricksTypeUtil.DATE;
import static com.databricks.jdbc.common.util.DatabricksTypeUtil.TIMESTAMP;
import static com.databricks.jdbc.common.util.DatabricksTypeUtil.TIMESTAMP_NTZ;

import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.sql.JDBCType;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.Temporal;
import java.util.Locale;
import java.util.Map;

/**
 * Converts DATE and TIMESTAMP values to the JDBC 4.2 {@code java.time} types for {@code
 * ResultSet.getObject(column, Class)}, and for nested values read through a type map ({@code
 * Array.getArray(Map)}, {@code Struct.getAttributes(Map)}).
 *
 * <p>Databricks stores DATE and TIMESTAMP on the proleptic Gregorian calendar. {@code
 * java.sql.Date} and {@code java.sql.Timestamp} use the hybrid Julian/Gregorian calendar and have
 * no BC era, so values before 1582-10-15 cannot be represented exactly by them. This converter
 * builds {@code java.time} values from the source value instead, which is exact for Arrow results.
 *
 * <p>The source value is one of:
 *
 * <ul>
 *   <li>{@link LocalDate}: a DATE read from Arrow.
 *   <li>{@link LocalDateTime}: a TIMESTAMP_NTZ read from Arrow.
 *   <li>{@link OffsetDateTime}: a TIMESTAMP read from Arrow, in the session time zone (in UTC for
 *       values nested in ARRAY, STRUCT or MAP, which carry no session time zone).
 *   <li>{@link String}: the server's text for the value (JSON and Thrift columnar results). This is
 *       best effort, because the server may already have lost precision or the era.
 *   <li>{@link java.sql.Date} or {@link Timestamp}: a value that only exists as a legacy type.
 * </ul>
 *
 * <p>Supported conversions:
 *
 * <pre>
 * Column        | LocalDate          | LocalDateTime         | Instant | OffsetDateTime
 * DATE          | exact              | atStartOfDay()        | error   | error
 * TIMESTAMP_NTZ | date part          | exact                 | error   | error
 * TIMESTAMP     | session-local date | session-local fields  | exact   | exact, UTC offset
 * </pre>
 */
public final class JavaTimeConverter {

  /**
   * Parses the text forms the server uses for timestamps: an ISO date, a space or 'T', an ISO time
   * with an optional fraction, and an optional offset. Years outside 0000-9999 carry a sign.
   */
  private static final DateTimeFormatter TIMESTAMP_TEXT_FORMATTER =
      new DateTimeFormatterBuilder()
          .parseCaseInsensitive()
          .append(DateTimeFormatter.ISO_LOCAL_DATE)
          .appendPattern("[ ]['T']")
          .append(DateTimeFormatter.ISO_LOCAL_TIME)
          .optionalStart()
          .appendOffsetId()
          .optionalEnd()
          .toFormatter()
          .withResolverStyle(ResolverStyle.STRICT);

  private JavaTimeConverter() {}

  /**
   * Returns true if {@code type} is a {@code java.time} class handled by this converter.
   *
   * @param type the class requested from {@code getObject}
   * @return true for {@link LocalDate}, {@link LocalDateTime}, {@link Instant} and {@link
   *     OffsetDateTime}
   */
  public static boolean isJavaTimeType(Class<?> type) {
    return type == LocalDate.class
        || type == LocalDateTime.class
        || type == Instant.class
        || type == OffsetDateTime.class;
  }

  /**
   * Converts a DATE or TIMESTAMP value to the requested {@code java.time} type.
   *
   * @param type the requested class, one of the types accepted by {@link #isJavaTimeType}
   * @param columnSqlType the column type, as defined in {@link Types}
   * @param columnTypeName the column type name from the result set metadata, may be null
   * @param value the source value, see the class documentation
   * @return the converted value, or null if {@code value} is null
   * @throws DatabricksSQLException if the column type cannot be converted to {@code type} or the
   *     value cannot be read
   */
  public static Object convert(
      Class<?> type, int columnSqlType, String columnTypeName, Object value)
      throws DatabricksSQLException {
    if (value == null) {
      return null;
    }
    if (columnSqlType != Types.DATE && columnSqlType != Types.TIMESTAMP) {
      throw unsupported(
          String.format(
              "Cannot convert a %s column to %s; only DATE and TIMESTAMP columns are supported",
              describeColumn(columnSqlType, columnTypeName), type.getName()));
    }
    Temporal temporal = toTemporal(columnSqlType, columnTypeName, value);
    if (temporal == null && value instanceof String) {
      // Do not include the raw value in the message: it may contain sensitive data and the
      // message is exported to telemetry.
      throw unsupported(
          String.format(
              "Cannot parse the %s value as %s",
              describeColumn(columnSqlType, columnTypeName), type.getName()));
    }
    if (temporal == null) {
      throw unsupported(
          String.format(
              "Cannot convert a %s value of type %s to %s",
              describeColumn(columnSqlType, columnTypeName),
              value.getClass().getName(),
              type.getName()));
    }
    if (temporal instanceof LocalDate) {
      return fromDate(type, (LocalDate) temporal);
    }
    if (temporal instanceof LocalDateTime) {
      return fromTimestampNtz(type, (LocalDateTime) temporal);
    }
    return fromTimestamp(type, (OffsetDateTime) temporal);
  }

  /**
   * Converts a DATE, TIMESTAMP or TIMESTAMP_NTZ value nested in an ARRAY, STRUCT or MAP to the
   * requested {@code java.time} type.
   *
   * @param type the requested class, one of the types accepted by {@link #isJavaTimeType}
   * @param typeName the nested value's type name, e.g. {@code DATE}
   * @param value the source value, see the class documentation
   * @return the converted value, or null if {@code value} is null
   * @throws DatabricksSQLException if the type cannot be converted to {@code type} or the value
   *     cannot be read
   */
  public static Object convertNested(Class<?> type, String typeName, Object value)
      throws DatabricksSQLException {
    return convert(type, temporalSqlType(typeName), typeName.trim(), value);
  }

  /**
   * Returns true if {@code typeName} is DATE, TIMESTAMP or TIMESTAMP_NTZ.
   *
   * @param typeName a Databricks type name, may be null
   * @return true for the temporal types handled by this converter
   */
  public static boolean isTemporalTypeName(String typeName) {
    return typeName != null && temporalSqlType(typeName) != Types.OTHER;
  }

  /**
   * Returns the {@code java.time} class that a JDBC type map assigns to a DATE, TIMESTAMP or
   * TIMESTAMP_NTZ type name, for example {@code Map.of("DATE", LocalDate.class)}. Keys are matched
   * case-insensitively. Other type names and classes are ignored and return null, keeping the
   * driver's existing behavior for them.
   *
   * @param typeMap the type map passed to {@code getArray(Map)} or {@code getAttributes(Map)}
   * @param typeName the type name of the values being read
   * @return the mapped {@code java.time} class, or null
   */
  public static Class<?> getJavaTimeMapping(Map<String, Class<?>> typeMap, String typeName) {
    if (typeMap == null || !isTemporalTypeName(typeName)) {
      return null;
    }
    for (Map.Entry<String, Class<?>> entry : typeMap.entrySet()) {
      if (entry.getKey() != null
          && entry.getKey().trim().equalsIgnoreCase(typeName.trim())
          && isJavaTimeType(entry.getValue())) {
        return entry.getValue();
      }
    }
    return null;
  }

  private static int temporalSqlType(String typeName) {
    String baseType = typeName.trim().toUpperCase(Locale.ROOT);
    if (baseType.equals(DATE)) {
      return Types.DATE;
    }
    if (baseType.equals(TIMESTAMP) || baseType.equals(TIMESTAMP_NTZ)) {
      return Types.TIMESTAMP;
    }
    return Types.OTHER;
  }

  /**
   * Normalizes a DATE or TIMESTAMP source value to a {@link LocalDate} (DATE), {@link
   * LocalDateTime} (TIMESTAMP_NTZ) or {@link OffsetDateTime} (TIMESTAMP).
   *
   * @param columnSqlType the column type, as defined in {@link Types}
   * @param columnTypeName the column type name, used to tell TIMESTAMP_NTZ text apart; may be null
   * @param value the source value, see the class documentation
   * @return the normalized value, or null if the column is not DATE or TIMESTAMP, the value type
   *     does not match the column, or the text cannot be parsed
   */
  public static Temporal toTemporal(int columnSqlType, String columnTypeName, Object value) {
    if (columnSqlType == Types.DATE) {
      if (value instanceof LocalDate) {
        return (LocalDate) value;
      }
      if (value instanceof java.sql.Date) {
        return ((java.sql.Date) value).toLocalDate();
      }
      if (value instanceof String) {
        return parseDateText((String) value);
      }
    } else if (columnSqlType == Types.TIMESTAMP) {
      // Arrow results carry the column kind in the value: TIMESTAMP_NTZ vectors produce
      // LocalDateTime and TIMESTAMP vectors produce OffsetDateTime. For text and legacy values the
      // kind comes from the metadata.
      if (value instanceof OffsetDateTime || value instanceof LocalDateTime) {
        return (Temporal) value;
      }
      boolean isTimestampNtz = isTimestampNtz(columnTypeName);
      if (value instanceof Timestamp) {
        Timestamp timestamp = (Timestamp) value;
        return isTimestampNtz
            ? timestamp.toLocalDateTime()
            : timestamp.toInstant().atZone(ZoneId.systemDefault()).toOffsetDateTime();
      }
      if (value instanceof String) {
        return normalizeTimestampText((String) value, isTimestampNtz);
      }
    }
    return null;
  }

  private static String describeColumn(int columnSqlType, String columnTypeName) {
    if (columnTypeName != null) {
      return columnTypeName;
    }
    try {
      return JDBCType.valueOf(columnSqlType).getName();
    } catch (IllegalArgumentException e) {
      return "SQL type " + columnSqlType;
    }
  }

  private static Object fromDate(Class<?> type, LocalDate date) throws DatabricksSQLException {
    if (type == LocalDate.class) {
      return date;
    }
    if (type == LocalDateTime.class) {
      return date.atStartOfDay();
    }
    throw unsupported(
        String.format(
            "Cannot convert a DATE value to %s: DATE has no time of day or time zone",
            type.getName()));
  }

  private static Object fromTimestampNtz(Class<?> type, LocalDateTime dateTime)
      throws DatabricksSQLException {
    if (type == LocalDate.class) {
      return dateTime.toLocalDate();
    }
    if (type == LocalDateTime.class) {
      return dateTime;
    }
    throw unsupported(
        String.format(
            "Cannot convert a TIMESTAMP_NTZ value to %s: TIMESTAMP_NTZ has no time zone; "
                + "use java.time.LocalDateTime",
            type.getName()));
  }

  private static Object fromTimestamp(Class<?> type, OffsetDateTime dateTime) {
    if (type == LocalDate.class) {
      return dateTime.toLocalDate();
    }
    if (type == LocalDateTime.class) {
      return dateTime.toLocalDateTime();
    }
    if (type == Instant.class) {
      return dateTime.toInstant();
    }
    return dateTime.withOffsetSameInstant(ZoneOffset.UTC);
  }

  /**
   * Parses the server's text for a DATE without going through {@code java.sql.Date}.
   *
   * @param text an ISO date, e.g. {@code 1582-10-05} or {@code -2015-01-01}
   * @return the date, or null if the text is not an ISO date
   */
  public static LocalDate parseDateText(String text) {
    try {
      return LocalDate.parse(text.trim());
    } catch (DateTimeParseException e) {
      return null;
    }
  }

  /**
   * Parses the server's text for a TIMESTAMP or TIMESTAMP_NTZ without going through {@code
   * java.sql.Timestamp}.
   *
   * @param text an ISO date and time, separated by a space or 'T', with an optional offset
   * @return an {@link OffsetDateTime} if the text has an offset, a {@link LocalDateTime} if not, or
   *     null if the text cannot be parsed
   */
  public static Temporal parseTimestampText(String text) {
    try {
      return (Temporal)
          TIMESTAMP_TEXT_FORMATTER.parseBest(
              text.trim(), OffsetDateTime::from, LocalDateTime::from);
    } catch (DateTimeParseException e) {
      return null;
    }
  }

  /**
   * Parses the server's text for a timestamp. Text without an offset holds the session-local
   * fields; for TIMESTAMP columns it is placed in the JVM default time zone, the same way {@code
   * getTimestamp()} interprets it.
   */
  private static Temporal normalizeTimestampText(String text, boolean isTimestampNtz) {
    Temporal parsed = parseTimestampText(text);
    if (parsed instanceof OffsetDateTime) {
      OffsetDateTime offsetDateTime = (OffsetDateTime) parsed;
      return isTimestampNtz ? offsetDateTime.toLocalDateTime() : offsetDateTime;
    }
    if (parsed == null || isTimestampNtz) {
      return parsed;
    }
    LocalDateTime localDateTime = (LocalDateTime) parsed;
    ZoneOffset offset = ZoneId.systemDefault().getRules().getOffset(localDateTime);
    return OffsetDateTime.of(localDateTime, offset);
  }

  private static boolean isTimestampNtz(String columnTypeName) {
    return columnTypeName != null && columnTypeName.trim().equalsIgnoreCase(TIMESTAMP_NTZ);
  }

  private static DatabricksSQLException unsupported(String message) {
    return new DatabricksSQLException(
        message,
        DatabricksDriverErrorCode.UNSUPPORTED_OPERATION.name(),
        DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }
}
