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
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.Locale;
import java.util.Map;

/**
 * Converts DATE and TIMESTAMP values, and ISO date or timestamp text in string columns, to the JDBC
 * 4.2 {@code java.time} types for {@code ResultSet.getObject(column, Class)}, and for nested values
 * read through a type map ({@code Array.getArray(Map)}, {@code Struct.getAttributes(Map)}).
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
 *   <li>{@link OffsetDateTime}: a TIMESTAMP read from Arrow, in the session time zone. Values
 *       nested in ARRAY or STRUCT carry no session time zone; Arrow's epoch microseconds are read
 *       in UTC.
 *   <li>{@link String}: the server's text for the value (JSON and Thrift columnar results).
 *   <li>{@link java.sql.Date} or {@link Timestamp}: a value that only exists as a legacy type.
 * </ul>
 *
 * <p>Text is best effort: the server may already have lost precision or the era. TIMESTAMP text
 * with an offset keeps its rendered (session-local) fields for {@link LocalDate} and {@link
 * LocalDateTime}, as for Arrow results, at the top level and nested; {@code getTimestamp()} instead
 * reads text ending in 'Z' as an instant in the JVM time zone. TIMESTAMP text without an offset is
 * read in the JVM time zone, as {@code getTimestamp()} does, so {@link Instant} and {@link
 * OffsetDateTime} assume the session time zone matches it. TIMESTAMP_NTZ is recognized from the
 * column metadata, which Thrift columnar results without an Arrow schema report as TIMESTAMP.
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
   * Parse the text forms the server uses for timestamps: an ISO date, a 'T' (or a space), an ISO
   * time with an optional fraction, and an optional offset. Years outside 0000-9999 carry a sign.
   */
  private static final DateTimeFormatter TIMESTAMP_TEXT_FORMATTER = timestampTextFormatter('T');

  private static final DateTimeFormatter TIMESTAMP_TEXT_WITH_SPACE_FORMATTER =
      timestampTextFormatter(' ');

  /** A date with non-padded month and day, e.g. {@code 2024-1-5}, as Date.valueOf accepts. */
  private static final DateTimeFormatter NON_PADDED_DATE_FORMATTER =
      DateTimeFormatter.ofPattern("uuuu-M-d").withResolverStyle(ResolverStyle.STRICT);

  /** 1582-10-15T00:00:00Z, the default cutover of {@link java.util.GregorianCalendar}. */
  private static final long GREGORIAN_CUTOVER_EPOCH_MILLI = -12219292800000L;

  private static final long MILLIS_PER_DAY = 86_400_000L;

  private JavaTimeConverter() {}

  private static DateTimeFormatter timestampTextFormatter(char separator) {
    return new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .append(DateTimeFormatter.ISO_LOCAL_DATE)
        .appendLiteral(separator)
        .append(DateTimeFormatter.ISO_LOCAL_TIME)
        .optionalStart()
        .appendOffsetId()
        .optionalEnd()
        .toFormatter()
        .withResolverStyle(ResolverStyle.STRICT);
  }

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
   * Converts a DATE or TIMESTAMP value, or ISO date or timestamp text in a string column, to the
   * requested {@code java.time} type.
   *
   * <p>String columns (as JDBC 4.2 allows) follow the rules of the type the text reads as: a date
   * alone as DATE, a date and time as TIMESTAMP_NTZ, and a date and time with an offset as
   * TIMESTAMP.
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
    boolean isText = isCharacterType(columnSqlType);
    if (!isSupportedColumnType(columnSqlType)) {
      throw unsupported(
          String.format(
              "Cannot convert a %s column to %s; only DATE, TIMESTAMP and string columns are"
                  + " supported",
              describeColumn(columnSqlType, columnTypeName), type.getName()));
    }
    if (columnSqlType == Types.TIMESTAMP
        && value instanceof Timestamp
        && !isTimestampNtz(columnTypeName)) {
      return fromLegacyTimestamp(type, (Timestamp) value);
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
      return fromDate(type, (LocalDate) temporal, isText);
    }
    if (temporal instanceof LocalDateTime) {
      return fromTimestampNtz(type, (LocalDateTime) temporal, isText);
    }
    return fromTimestamp(type, (OffsetDateTime) temporal);
  }

  /**
   * Returns true if values of the column type can be converted to {@code java.time} types.
   *
   * @param columnSqlType the column type, as defined in {@link Types}
   * @return true for DATE, TIMESTAMP and string columns
   */
  public static boolean isSupportedColumnType(int columnSqlType) {
    return columnSqlType == Types.DATE
        || columnSqlType == Types.TIMESTAMP
        || isCharacterType(columnSqlType);
  }

  private static boolean isCharacterType(int columnSqlType) {
    return columnSqlType == Types.VARCHAR
        || columnSqlType == Types.CHAR
        || columnSqlType == Types.LONGVARCHAR
        || columnSqlType == Types.NVARCHAR
        || columnSqlType == Types.NCHAR
        || columnSqlType == Types.LONGNVARCHAR;
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
    if (typeMap == null || typeMap.isEmpty() || !isTemporalTypeName(typeName)) {
      return null;
    }
    return findJavaTimeMapping(typeMap, typeName);
  }

  /**
   * Returns the {@code java.time} class that a JDBC type map assigns to {@code typeName}, for any
   * column type. Keys are matched case-insensitively; non-{@code java.time} classes are ignored.
   *
   * @param typeMap a JDBC type map, may be null
   * @param typeName the column type name, may be null
   * @return the mapped {@code java.time} class, or null
   */
  public static Class<?> findJavaTimeMapping(Map<String, Class<?>> typeMap, String typeName) {
    if (typeMap == null || typeName == null) {
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
   * @return the normalized value, or null if the column is not DATE, TIMESTAMP or a string type,
   *     the value type does not match the column, or the text cannot be parsed
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
    } else if (isCharacterType(columnSqlType) && value instanceof String) {
      // A date and time, with or without an offset, or a date alone.
      Temporal timestamp = parseTimestampText((String) value);
      return timestamp != null ? timestamp : parseDateText((String) value);
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

  private static Object fromDate(Class<?> type, LocalDate date, boolean isText)
      throws DatabricksSQLException {
    if (type == LocalDate.class) {
      return date;
    }
    if (type == LocalDateTime.class) {
      return date.atStartOfDay();
    }
    throw unsupported(
        isText
            ? String.format(
                "Cannot convert date text to %s: it has no time of day or time zone",
                type.getName())
            : String.format(
                "Cannot convert a DATE value to %s: DATE has no time of day or time zone",
                type.getName()));
  }

  private static Object fromTimestampNtz(Class<?> type, LocalDateTime dateTime, boolean isText)
      throws DatabricksSQLException {
    if (type == LocalDate.class) {
      return dateTime.toLocalDate();
    }
    if (type == LocalDateTime.class) {
      return dateTime;
    }
    throw unsupported(
        isText
            ? String.format(
                "Cannot convert timestamp text without an offset to %s; "
                    + "use java.time.LocalDateTime",
                type.getName())
            : String.format(
                "Cannot convert a TIMESTAMP_NTZ value to %s: TIMESTAMP_NTZ has no time zone; "
                    + "use java.time.LocalDateTime",
                type.getName()));
  }

  /**
   * Converts a TIMESTAMP that only exists as a {@code java.sql.Timestamp}: its own fields for
   * {@link LocalDate} and {@link LocalDateTime} and its instant for {@link Instant} and {@link
   * OffsetDateTime}, the same as {@code getTimestamp()}. Reading the instant back into fields with
   * {@code java.time} would move pre-1582 values by days and use different historical offsets.
   */
  private static Object fromLegacyTimestamp(Class<?> type, Timestamp timestamp) {
    if (type == LocalDate.class) {
      return timestamp.toLocalDateTime().toLocalDate();
    }
    if (type == LocalDateTime.class) {
      return timestamp.toLocalDateTime();
    }
    if (type == Instant.class) {
      return timestamp.toInstant();
    }
    return timestamp.toInstant().atOffset(ZoneOffset.UTC);
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
   * @param text an ISO date, e.g. {@code 1582-10-05} or {@code -2015-01-01}, or one with non-padded
   *     month and day as {@code java.sql.Date.valueOf} accepts, e.g. {@code 2024-1-5}; both are
   *     read on the proleptic Gregorian calendar
   * @return the date, or null if the text cannot be parsed
   */
  public static LocalDate parseDateText(String text) {
    String trimmed = text.trim();
    try {
      return LocalDate.parse(trimmed);
    } catch (DateTimeParseException e) {
      try {
        return LocalDate.parse(trimmed, NON_PADDED_DATE_FORMATTER);
      } catch (DateTimeParseException notADate) {
        return null;
      }
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
    String trimmed = text.trim();
    // Exactly one separator between the date and the time: 'T' if there is one, else a space.
    DateTimeFormatter formatter =
        trimmed.indexOf('T') >= 0 || trimmed.indexOf('t') >= 0
            ? TIMESTAMP_TEXT_FORMATTER
            : TIMESTAMP_TEXT_WITH_SPACE_FORMATTER;
    TemporalAccessor parsed;
    try {
      parsed = formatter.parse(trimmed);
    } catch (DateTimeParseException e) {
      return null;
    }
    return parsed.query(TemporalQueries.offset()) != null
        ? OffsetDateTime.from(parsed)
        : LocalDateTime.from(parsed);
  }

  /**
   * Returns true if a {@code java.sql.Date} or {@code java.sql.Timestamp} built by the driver may
   * have been shifted by the hybrid Julian/Gregorian calendar. That covers values before the
   * 1582-10-15 Gregorian cutover and up to 12 days after it: dates in the 1582-10-05..14 gap are
   * pushed forward by 10 days (1582-10-14 becomes 1582-10-24), plus a margin for any time zone.
   * Later values are exact.
   *
   * @param legacyValue a {@code java.sql.Date} or {@code java.sql.Timestamp}
   * @return true if the value may differ from the server's value
   */
  public static boolean mayBeCalendarShifted(java.util.Date legacyValue) {
    return legacyValue.getTime() < GREGORIAN_CUTOVER_EPOCH_MILLI + 12 * MILLIS_PER_DAY;
  }

  /**
   * Parses the server's text for a timestamp. Text without an offset holds the session-local
   * fields; for TIMESTAMP columns it is placed in the JVM default time zone, the same way {@code
   * getTimestamp()} interprets it (the later offset when the fields are ambiguous).
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
    return ((LocalDateTime) parsed)
        .atZone(ZoneId.systemDefault())
        .withLaterOffsetAtOverlap()
        .toOffsetDateTime();
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
