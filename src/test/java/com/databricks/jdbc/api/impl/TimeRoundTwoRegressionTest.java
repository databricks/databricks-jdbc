package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.TestConstants;
import com.databricks.jdbc.api.impl.converters.ArrowToJavaObjectConverter;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.util.DatabricksTypeUtil;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.*;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.service.sql.Format;
import com.databricks.sdk.service.sql.StatementState;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Types;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Properties;
import java.util.TimeZone;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class TimeRoundTwoRegressionTest {
  @Test
  void timePrecheckHasNoKeywordFalseNegatives() {
    for (String metadata :
        Arrays.asList(
            "TIME",
            "time(6)",
            "ARRAY<TIME(3)>",
            "STRUCT<t:TIMESTAMP_NTZ,clock:TIME(6)>",
            "STRUCT<`time`:TIME(6)>")) {
      assertTrue(MetadataParser.mayContainTime(metadata), metadata);
    }
    for (String metadata :
        Arrays.asList(
            "INT", "ARRAY<STRUCT<ts:TIMESTAMP_NTZ>>", "timestamp", "MAP<STRING,TIMESTAMP>")) {
      assertFalse(MetadataParser.mayContainTime(metadata), metadata);
    }
    for (String metadata : Arrays.asList("STRUCT<>", "MAP<>", "ARRAY<", "STRUCT<broken")) {
      assertFalse(new MetadataParser.TimeType(metadata, null).containsTime);
    }
  }

  @Test
  void sharedParserMemoDoesNotMixConcurrentSchemas() throws Exception {
    ComplexDataTypeParser parser = new ComplexDataTypeParser();
    java.util.concurrent.ExecutorService executor =
        java.util.concurrent.Executors.newFixedThreadPool(2);
    try {
      java.util.concurrent.Future<?> array =
          executor.submit(
              () -> {
                for (int iteration = 0; iteration < 1000; iteration++) {
                  assertEquals(
                      "[\"01:02:03.123456\"]",
                      parser.formatComplexTypeString(
                          "[\"01:02:03.123456\"]", "ARRAY", "ARRAY<TIME(6)>"));
                }
                return null;
              });
      java.util.concurrent.Future<?> map =
          executor.submit(
              () -> {
                for (int iteration = 0; iteration < 1000; iteration++) {
                  assertEquals(
                      "{\"a\":\"01:02:03.123456\"}",
                      parser.formatComplexTypeString(
                          "{\"a\":\"01:02:03.123456\"}", "MAP", "MAP<STRING,TIME(6)>"));
                }
                return null;
              });
      array.get(10, java.util.concurrent.TimeUnit.SECONDS);
      map.get(10, java.util.concurrent.TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }
  }

  @ParameterizedTest
  @CsvSource({"1,0", "1,1", "0,0", "0,1"})
  void timeParameterBindingRemainsUnsupported(int thrift, int many) throws Exception {
    Properties properties = new Properties();
    properties.setProperty("UseThriftClient", Integer.toString(thrift));
    properties.setProperty("supportManyParameters", Integer.toString(many));
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(TestConstants.VALID_URL_1, properties);
    DatabricksConnection connection = mock(DatabricksConnection.class);
    when(connection.getConnectionContext()).thenReturn(context);
    try (DatabricksPreparedStatement statement =
        new DatabricksPreparedStatement(connection, "SELECT ?")) {
      Time time = new Time(Time.valueOf("09:15:00").getTime() + 123);
      assertThrows(
          SQLFeatureNotSupportedException.class, () -> statement.setObject(1, time, Types.TIME));
      assertThrows(
          SQLFeatureNotSupportedException.class, () -> statement.setObject(1, time, Types.TIME, 3));
      assertThrows(SQLFeatureNotSupportedException.class, () -> statement.setNull(1, Types.TIME));
      assertEquals(
          DatabricksTypeUtil.NULL, DatabricksTypeUtil.getDatabricksTypeFromSQLType(Types.TIME));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void localTimeInvalidIndicesAndLabelsThrowSqlExceptions(boolean strict) throws Exception {
    try (DatabricksResultSet result = inlineResult("TIME(6)", "01:02:03.123456", strict, false)) {
      assertTrue(result.next());
      for (int index : new int[] {-1, 0, 2, 99}) {
        assertThrows(DatabricksSQLException.class, () -> result.getObject(index, LocalTime.class));
      }
      assertThrows(
          DatabricksSQLException.class, () -> result.getObject("missing", LocalTime.class));
    }
  }

  @Test
  void calendarKeepsMillisecondsInUtcPlusJvmZone() throws Exception {
    TimeZone original = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("Asia/Tokyo"));
      for (String clock : Arrays.asList("01:02:03.123", "00:30:00.5", "12:00:00.250")) {
        try (DatabricksResultSet result = inlineResult("TIME(6)", clock, false, false)) {
          assertTrue(result.next());
          Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
          long unchanged = calendar.getTimeInMillis();
          long expected = LocalTime.parse(clock).toNanoOfDay() / 1_000_000;
          assertEquals(expected, result.getTime(1, calendar).getTime());
          assertEquals(expected, result.getTime("clock", calendar).getTime());
          assertEquals(unchanged, calendar.getTimeInMillis());
        }
        try (DatabricksResultSet result =
            inlineResult("TIMESTAMP", "2024-01-02 " + clock, false, false)) {
          assertTrue(result.next());
          assertEquals(
              LocalTime.parse(clock).toNanoOfDay() / 1_000_000,
              result.getTime(1, Calendar.getInstance(TimeZone.getTimeZone("UTC"))).getTime());
        }
      }
    } finally {
      TimeZone.setDefault(original);
    }
  }

  @Test
  void emptyStructChildrenDoNotForceTimeParsing() throws Exception {
    ComplexDataTypeParser parser = new ComplexDataTypeParser();
    assertEquals("[]", parser.parseJsonStringToDbArray("[]", "ARRAY<STRUCT<>>").toString());
    assertEquals("[null]", parser.parseJsonStringToDbArray("[null]", "ARRAY<STRUCT<>>").toString());
    assertEquals(
        "{\"a\":null}",
        parser.parseJsonStringToDbMap("{\"a\":null}", "MAP<STRING,STRUCT<>>").toString());
    assertEquals(
        "{\"a\":null}",
        parser.formatComplexTypeString(
            "[{\"key\":\"a\",\"value\":null}]", "MAP", "MAP<STRING,STRUCT<>>"));
    assertEquals(
        "{\"a\":1,\"e\":null}",
        parser
            .parseJsonStringToDbStruct("{\"a\":1,\"e\":null}", "STRUCT<a:INT,e:STRUCT<>>")
            .toString());
    assertEquals(
        "{\"clock\":\"01:02:03.123456\",\"e\":null}",
        parser
            .parseJsonStringToDbStruct(
                "{\"clock\":\"01:02:03.123456\",\"e\":null}", "STRUCT<clock:TIME(6),e:STRUCT<>>")
            .toString());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void invalidTimeHasTheSamePolicyForBothComplexModes(boolean complex) throws Exception {
    ComplexDataTypeParser parser = new ComplexDataTypeParser();
    DatabricksParsingException exception =
        assertThrows(
            DatabricksParsingException.class,
            () -> {
              if (complex) parser.parseJsonStringToDbArray("[86400000000000]", "ARRAY<TIME(6)>");
              else
                parser.formatComplexTypeStringForResult(
                    "[86400000000000]", "ARRAY", "ARRAY<TIME(6)>");
            });
    assertEquals("Invalid TIME value", exception.getMessage());
    assertEquals(DatabricksDriverErrorCode.RESULT_SET_ERROR.name(), exception.getSQLState());
    assertEquals(1004, exception.getErrorCode());
    assertNotNull(exception.getCause());
  }

  @Test
  void unitScalingOverflowIsRejected() throws Exception {
    long overflowingMicros = 18446744073709552L;
    try (RootAllocator allocator = new RootAllocator();
        TimeMicroVector vector = new TimeMicroVector("t", allocator)) {
      vector.allocateNew(1);
      vector.setSafe(0, overflowingMicros);
      DatabricksSQLException exception =
          assertThrows(
              DatabricksSQLException.class,
              () ->
                  ArrowToJavaObjectConverter.convert(
                      vector, 0, ColumnInfoTypeName.TIME, "TIME(6)", new ColumnInfo()));
      assertInstanceOf(ArithmeticException.class, exception.getCause());
      assertEquals("Invalid TIME value", exception.getMessage());
    }
    Field child =
        new Field(
            "element", FieldType.nullable(new ArrowType.Time(TimeUnit.MICROSECOND, 64)), null);
    Field array =
        new Field(
            "array", FieldType.nullable(ArrowType.List.INSTANCE), Collections.singletonList(child));
    ComplexDataTypeParser parser = new ComplexDataTypeParser(true, null, array);
    DatabricksParsingException exception =
        assertThrows(
            DatabricksParsingException.class,
            () -> parser.parseJsonStringToDbArray("[" + overflowingMicros + "]", "ARRAY<TIME(6)>"));
    assertInstanceOf(ArithmeticException.class, exception.getCause());
  }

  @Test
  void mutatedContainersRenderNonTimeValuesWithoutThrowing() throws Exception {
    ComplexDataTypeParser parser = new ComplexDataTypeParser();
    DatabricksArray array =
        parser.parseJsonStringToDbArray("[\"01:02:03.123456\"]", "ARRAY<TIME(6)>");
    ((Object[]) array.getArray())[0] = "not a time";
    assertEquals("[\"not a time\"]", array.toString());
    DatabricksStruct struct =
        parser.parseJsonStringToDbStruct(
            "{\"clock\":\"01:02:03.123456\"}", "STRUCT<clock:TIME(6)>");
    struct.getAttributes()[0] = "not a time";
    assertEquals("{\"clock\":\"not a time\"}", struct.toString());
    DatabricksMap<String, Object> map =
        parser.parseJsonStringToDbMap("{\"a\":\"01:02:03.123456\"}", "MAP<STRING,TIME(6)>");
    map.put("a", "not a time");
    assertEquals("{\"a\":\"not a time\"}", map.toString());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void seaJsonInlineNestedTimeIsExact(boolean complex) throws Exception {
    try (DatabricksResultSet result =
        inlineResult("ARRAY<TIME(6)>", "[\"01:02:03.123456\",null]", false, complex)) {
      assertTrue(result.next());
      assertEquals("[\"01:02:03.123456\",null]", result.getString(1));
      assertEquals("[\"01:02:03.123456\",null]", result.getObject(1).toString());
    }
    try (DatabricksResultSet result =
        inlineResult(
            "MAP<TIME(6),INT>",
            "{\"01:02:03.0001\":\"1\",\"01:02:03.0002\":\"2\"}",
            false,
            complex)) {
      assertTrue(result.next());
      assertEquals("{\"01:02:03.0001\":\"1\",\"01:02:03.0002\":\"2\"}", result.getString(1));
      assertEquals("{\"01:02:03.0001\":1,\"01:02:03.0002\":2}", result.getObject(1).toString());
    }
  }

  static DatabricksResultSet inlineResult(
      String type, String value, boolean strict, boolean complex) throws Exception {
    IDatabricksSession session = mock(IDatabricksSession.class);
    IDatabricksConnectionContext context = mock(IDatabricksConnectionContext.class);
    when(session.getConnectionContext()).thenReturn(context);
    when(context.isStrictGetObjectConversionEnabled()).thenReturn(strict);
    when(context.isComplexDatatypeSupportEnabled()).thenReturn(complex);
    ColumnInfo column =
        new ColumnInfo()
            .setName("clock")
            .setTypeText(type)
            .setTypeName(DatabricksTypeUtil.getColumnInfoType(type));
    if (type.startsWith("ARRAY")) column.setTypeName(ColumnInfoTypeName.ARRAY);
    if (type.startsWith("MAP")) column.setTypeName(ColumnInfoTypeName.MAP);
    ResultManifest manifest =
        new ResultManifest()
            .setFormat(Format.JSON_ARRAY)
            .setTotalChunkCount(1L)
            .setTotalRowCount(1L)
            .setSchema(
                new ResultSchema()
                    .setColumnCount(1L)
                    .setColumns(Collections.singletonList(column)));
    return new DatabricksResultSet(
        new StatementStatus().setState(StatementState.SUCCEEDED),
        new StatementId("time-round-two"),
        new ResultData()
            .setChunkIndex(0L)
            .setDataArray(Collections.singletonList(Collections.singletonList(value))),
        manifest,
        StatementType.QUERY,
        session,
        null);
  }
}
