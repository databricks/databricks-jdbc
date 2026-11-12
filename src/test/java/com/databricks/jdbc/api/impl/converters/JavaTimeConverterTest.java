package com.databricks.jdbc.api.impl.converters;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

public class JavaTimeConverterTest {

  private static final LocalDate BC_DATE = LocalDate.of(-2015, 1, 1); // 2016 BC
  private static final LocalDate GAP_DATE = LocalDate.of(1582, 10, 5);
  private static final LocalDateTime BC_DATE_TIME = LocalDateTime.of(-2015, 1, 1, 10, 30, 0, 1000);

  /** 2023-12-31T20:30:01.234567Z rendered in the session time zone Asia/Tokyo. */
  private static final OffsetDateTime SESSION_TIMESTAMP =
      OffsetDateTime.parse("2024-01-01T05:30:01.234567+09:00");

  @ParameterizedTest
  @ValueSource(
      classes = {LocalDate.class, LocalDateTime.class, Instant.class, OffsetDateTime.class})
  public void testIsJavaTimeType_True(Class<?> type) {
    assertTrue(JavaTimeConverter.isJavaTimeType(type));
  }

  @ParameterizedTest
  @ValueSource(
      classes = {
        Date.class,
        Timestamp.class,
        java.util.Date.class,
        String.class,
        java.time.ZonedDateTime.class
      })
  public void testIsJavaTimeType_False(Class<?> type) {
    assertFalse(JavaTimeConverter.isJavaTimeType(type));
  }

  private static Stream<Arguments> supportedConversions() {
    return Stream.of(
        // DATE read from Arrow
        Arguments.of(Types.DATE, "DATE", BC_DATE, LocalDate.class, BC_DATE),
        Arguments.of(Types.DATE, "DATE", GAP_DATE, LocalDate.class, GAP_DATE),
        Arguments.of(
            Types.DATE, "DATE", BC_DATE, LocalDateTime.class, LocalDateTime.of(-2015, 1, 1, 0, 0)),
        // DATE text (JSON and Thrift columnar results)
        Arguments.of(Types.DATE, "DATE", "1582-10-05", LocalDate.class, GAP_DATE),
        Arguments.of(Types.DATE, "DATE", "-2015-01-01", LocalDate.class, BC_DATE),
        // DATE that only exists as java.sql.Date
        Arguments.of(
            Types.DATE,
            "DATE",
            Date.valueOf("2023-08-29"),
            LocalDate.class,
            LocalDate.of(2023, 8, 29)),
        // TIMESTAMP_NTZ read from Arrow. The value type wins over the metadata type name, which is
        // reported as TIMESTAMP when EnableTimestampNtzTypeName=0.
        Arguments.of(
            Types.TIMESTAMP, "TIMESTAMP_NTZ", BC_DATE_TIME, LocalDateTime.class, BC_DATE_TIME),
        Arguments.of(Types.TIMESTAMP, "TIMESTAMP", BC_DATE_TIME, LocalDateTime.class, BC_DATE_TIME),
        Arguments.of(Types.TIMESTAMP, "TIMESTAMP_NTZ", BC_DATE_TIME, LocalDate.class, BC_DATE),
        // TIMESTAMP_NTZ text
        Arguments.of(
            Types.TIMESTAMP,
            "TIMESTAMP_NTZ",
            "2023-01-01T12:30:00.123",
            LocalDateTime.class,
            LocalDateTime.of(2023, 1, 1, 12, 30, 0, 123_000_000)),
        Arguments.of(
            Types.TIMESTAMP,
            "timestamp_ntz",
            "-2015-01-01 10:30:00.000001",
            LocalDateTime.class,
            BC_DATE_TIME),
        // TIMESTAMP read from Arrow, in the session time zone
        Arguments.of(
            Types.TIMESTAMP,
            "TIMESTAMP",
            SESSION_TIMESTAMP,
            LocalDate.class,
            LocalDate.of(2024, 1, 1)),
        Arguments.of(
            Types.TIMESTAMP,
            "TIMESTAMP",
            SESSION_TIMESTAMP,
            LocalDateTime.class,
            LocalDateTime.of(2024, 1, 1, 5, 30, 1, 234_567_000)),
        Arguments.of(
            Types.TIMESTAMP,
            "TIMESTAMP",
            SESSION_TIMESTAMP,
            Instant.class,
            Instant.parse("2023-12-31T20:30:01.234567Z")),
        Arguments.of(
            Types.TIMESTAMP,
            "TIMESTAMP",
            SESSION_TIMESTAMP,
            OffsetDateTime.class,
            OffsetDateTime.parse("2023-12-31T20:30:01.234567Z")),
        Arguments.of(
            Types.TIMESTAMP,
            "TIMESTAMP",
            OffsetDateTime.parse("-2015-01-01T00:00:00Z"),
            Instant.class,
            Instant.parse("-2015-01-01T00:00:00Z")),
        // TIMESTAMP text with an offset (SEA JSON results)
        Arguments.of(
            Types.TIMESTAMP,
            "TIMESTAMP",
            "2016-01-01T00:00:00.000Z",
            Instant.class,
            Instant.parse("2016-01-01T00:00:00Z")),
        Arguments.of(
            Types.TIMESTAMP,
            "TIMESTAMP",
            "2023-01-01T12:30:00+05:30",
            LocalDateTime.class,
            LocalDateTime.of(2023, 1, 1, 12, 30)),
        Arguments.of(
            Types.TIMESTAMP,
            "TIMESTAMP",
            "2023-01-01T12:30:00+05:30",
            OffsetDateTime.class,
            OffsetDateTime.parse("2023-01-01T07:00:00Z")),
        // TIMESTAMP text without an offset keeps the rendered fields
        Arguments.of(
            Types.TIMESTAMP,
            "TIMESTAMP",
            "1000-01-01 00:00:00.123456",
            LocalDateTime.class,
            LocalDateTime.of(1000, 1, 1, 0, 0, 0, 123_456_000)));
  }

  @ParameterizedTest
  @MethodSource("supportedConversions")
  public void testConvert_Supported(
      int columnSqlType, String columnTypeName, Object value, Class<?> type, Object expected)
      throws DatabricksSQLException {
    Object actual = JavaTimeConverter.convert(type, columnSqlType, columnTypeName, value);
    assertInstanceOf(type, actual);
    assertEquals(expected, actual);
  }

  @Test
  public void testConvert_TimestampTextWithoutOffsetMatchesGetTimestamp()
      throws DatabricksSQLException {
    // Thrift columnar results send the session-local fields without an offset. Instant and
    // OffsetDateTime place them in the JVM default time zone, as getTimestamp() does.
    String text = "2023-06-15 12:30:00.123456";
    Instant expected = Timestamp.valueOf(text).toInstant();

    assertEquals(
        expected, JavaTimeConverter.convert(Instant.class, Types.TIMESTAMP, "TIMESTAMP", text));
    assertEquals(
        expected.atOffset(ZoneOffset.UTC),
        JavaTimeConverter.convert(OffsetDateTime.class, Types.TIMESTAMP, "TIMESTAMP", text));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "2024-11-03 01:30:00", // ambiguous in America/New_York
        "2024-03-10 02:30:00", // skipped in America/New_York
        "2024-06-15 12:00:00"
      })
  public void testConvert_TimestampTextWithoutOffsetMatchesGetTimestampAroundDst(String text)
      throws DatabricksSQLException {
    java.util.TimeZone defaultTimeZone = java.util.TimeZone.getDefault();
    try {
      java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("America/New_York"));
      Timestamp legacy = Timestamp.valueOf(text);
      assertEquals(
          legacy.toInstant(),
          JavaTimeConverter.convert(Instant.class, Types.TIMESTAMP, "TIMESTAMP", text));
      assertEquals(
          legacy.toLocalDateTime(),
          JavaTimeConverter.convert(LocalDateTime.class, Types.TIMESTAMP, "TIMESTAMP", text));
    } finally {
      java.util.TimeZone.setDefault(defaultTimeZone);
    }
  }

  @Test
  public void testMayBeCalendarShifted() {
    assertTrue(JavaTimeConverter.mayBeCalendarShifted(Date.valueOf(LocalDate.of(1582, 10, 5))));
    assertTrue(JavaTimeConverter.mayBeCalendarShifted(Date.valueOf(BC_DATE)));
    assertTrue(JavaTimeConverter.mayBeCalendarShifted(Timestamp.valueOf("1582-10-15 12:00:00")));
    assertFalse(JavaTimeConverter.mayBeCalendarShifted(Date.valueOf("1582-10-18")));
    assertFalse(JavaTimeConverter.mayBeCalendarShifted(Timestamp.valueOf("1900-01-01 00:00:00")));
  }

  @Test
  public void testConvert_LegacyTimestamp() throws DatabricksSQLException {
    Timestamp timestamp = Timestamp.valueOf("2023-06-15 12:30:00.123456789");

    assertEquals(
        timestamp.toInstant(),
        JavaTimeConverter.convert(Instant.class, Types.TIMESTAMP, "TIMESTAMP", timestamp));
    assertEquals(
        timestamp.toLocalDateTime(),
        JavaTimeConverter.convert(LocalDateTime.class, Types.TIMESTAMP, "TIMESTAMP", timestamp));
    assertEquals(
        timestamp.toLocalDateTime(),
        JavaTimeConverter.convert(
            LocalDateTime.class, Types.TIMESTAMP, "TIMESTAMP_NTZ", timestamp));
    assertUnsupported(
        () ->
            JavaTimeConverter.convert(Instant.class, Types.TIMESTAMP, "TIMESTAMP_NTZ", timestamp));
  }

  @Test
  public void testConvert_NullValue() throws DatabricksSQLException {
    assertNull(JavaTimeConverter.convert(Instant.class, Types.TIMESTAMP, "TIMESTAMP", null));
    assertNull(JavaTimeConverter.convert(LocalDate.class, Types.VARCHAR, "STRING", null));
  }

  private static Stream<Arguments> unsupportedConversions() {
    return Stream.of(
        // DATE has no time of day or time zone
        Arguments.of(Types.DATE, "DATE", BC_DATE, Instant.class),
        Arguments.of(Types.DATE, "DATE", BC_DATE, OffsetDateTime.class),
        Arguments.of(Types.DATE, "DATE", "2023-08-29", Instant.class),
        // TIMESTAMP_NTZ has no time zone
        Arguments.of(Types.TIMESTAMP, "TIMESTAMP_NTZ", BC_DATE_TIME, Instant.class),
        Arguments.of(Types.TIMESTAMP, "TIMESTAMP", BC_DATE_TIME, OffsetDateTime.class),
        Arguments.of(Types.TIMESTAMP, "TIMESTAMP_NTZ", "2023-01-01 12:30:00", Instant.class),
        // Non-temporal columns
        Arguments.of(Types.VARCHAR, "STRING", "2023-01-01", LocalDate.class),
        Arguments.of(Types.VARCHAR, "STRING", "2023-01-01T00:00:00Z", Instant.class),
        Arguments.of(Types.BIGINT, "BIGINT", 1L, LocalDateTime.class),
        Arguments.of(Types.DECIMAL, null, BigDecimal.ONE, OffsetDateTime.class),
        // Value types that do not match the column type
        Arguments.of(Types.DATE, "DATE", BC_DATE_TIME, LocalDate.class),
        Arguments.of(Types.TIMESTAMP, "TIMESTAMP", BC_DATE, LocalDate.class),
        // Text the server would not send
        Arguments.of(Types.DATE, "DATE", "not a date", LocalDate.class),
        Arguments.of(Types.TIMESTAMP, "TIMESTAMP", "2023-02-30 00:00:00", Instant.class),
        Arguments.of(Types.TIMESTAMP, "TIMESTAMP", "2023-01-01", LocalDateTime.class));
  }

  @ParameterizedTest
  @MethodSource("unsupportedConversions")
  public void testConvert_UnsupportedThrowsUnsupportedOperation(
      int columnSqlType, String columnTypeName, Object value, Class<?> type) {
    assertUnsupported(() -> JavaTimeConverter.convert(type, columnSqlType, columnTypeName, value));
  }

  @Test
  public void testConvert_ErrorMessages() {
    DatabricksSQLException ntz =
        assertUnsupported(
            () ->
                JavaTimeConverter.convert(
                    Instant.class, Types.TIMESTAMP, "TIMESTAMP_NTZ", BC_DATE_TIME));
    assertTrue(ntz.getMessage().contains("TIMESTAMP_NTZ has no time zone"), ntz.getMessage());

    DatabricksSQLException nonTemporal =
        assertUnsupported(
            () -> JavaTimeConverter.convert(Instant.class, Types.DECIMAL, null, BigDecimal.ONE));
    assertTrue(nonTemporal.getMessage().contains("DECIMAL column"), nonTemporal.getMessage());

    // The raw value may be sensitive and must not be included in the message.
    DatabricksSQLException unparseable =
        assertUnsupported(
            () ->
                JavaTimeConverter.convert(
                    Instant.class, Types.TIMESTAMP, "TIMESTAMP", "secret-value"));
    assertFalse(unparseable.getMessage().contains("secret-value"), unparseable.getMessage());
  }

  @Test
  public void testGetJavaTimeMapping() {
    java.util.Map<String, Class<?>> typeMap =
        java.util.Map.of(
            "date", LocalDate.class,
            "TIMESTAMP", Instant.class,
            "TIMESTAMP_NTZ", String.class,
            "INT", LocalDate.class);

    assertEquals(LocalDate.class, JavaTimeConverter.getJavaTimeMapping(typeMap, "DATE"));
    assertEquals(Instant.class, JavaTimeConverter.getJavaTimeMapping(typeMap, " timestamp "));
    // Non-java.time classes and non-temporal type names are ignored.
    assertNull(JavaTimeConverter.getJavaTimeMapping(typeMap, "TIMESTAMP_NTZ"));
    assertNull(JavaTimeConverter.getJavaTimeMapping(typeMap, "INT"));
    assertNull(JavaTimeConverter.getJavaTimeMapping(null, "DATE"));
  }

  @Test
  public void testConvertNested() throws DatabricksSQLException {
    assertEquals(BC_DATE, JavaTimeConverter.convertNested(LocalDate.class, "DATE", BC_DATE));
    assertEquals(
        BC_DATE_TIME,
        JavaTimeConverter.convertNested(LocalDateTime.class, "TIMESTAMP_NTZ", BC_DATE_TIME));
    assertEquals(
        Instant.parse("2023-12-31T20:30:01.234567Z"),
        JavaTimeConverter.convertNested(Instant.class, "TIMESTAMP", SESSION_TIMESTAMP));
    assertUnsupported(() -> JavaTimeConverter.convertNested(Instant.class, "DATE", BC_DATE));
  }

  @Test
  public void testTextParsersReturnNullForUnparseableText() {
    assertEquals(GAP_DATE, JavaTimeConverter.parseDateText(" 1582-10-05 "));
    // Forms java.sql.Date.valueOf accepts are still read
    assertEquals(LocalDate.of(2024, 1, 5), JavaTimeConverter.parseDateText("2024-1-5"));
    assertNull(JavaTimeConverter.parseDateText("19598"));
    assertEquals(
        OffsetDateTime.parse("2016-01-01T00:00:00Z"),
        JavaTimeConverter.parseTimestampText("2016-01-01T00:00:00.000Z"));
    assertEquals(BC_DATE_TIME, JavaTimeConverter.parseTimestampText("-2015-01-01 10:30:00.000001"));
    assertNull(JavaTimeConverter.parseTimestampText("1696519230000000"));
  }

  private static DatabricksSQLException assertUnsupported(
      org.junit.jupiter.api.function.Executable executable) {
    DatabricksSQLException exception = assertThrows(DatabricksSQLException.class, executable);
    assertEquals(DatabricksDriverErrorCode.UNSUPPORTED_OPERATION.name(), exception.getSQLState());
    assertEquals(
        DatabricksDriverErrorCode.UNSUPPORTED_OPERATION.getCode(), exception.getErrorCode());
    return exception;
  }
}
