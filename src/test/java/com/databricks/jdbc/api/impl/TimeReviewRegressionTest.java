package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.impl.converters.ArrowToJavaObjectConverter;
import com.databricks.jdbc.api.impl.converters.JavaTimeConverter;
import com.databricks.jdbc.api.impl.converters.TimeConverter;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.*;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.service.sql.Format;
import com.databricks.sdk.service.sql.StatementState;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TimeReviewRegressionTest {
  @Test
  void schemasWithoutTimeDoNotRewriteMetadata() {
    for (String metadata :
        Arrays.asList(
            "ARRAY<INT>", "MAP<STRING,INT>", "STRUCT<`a time, b`:INT,tags:ARRAY<STRING>>")) {
      MetadataParser.TimeType parsed = new MetadataParser.TimeType(metadata, null);
      assertFalse(parsed.containsTime);
      assertSame(metadata, parsed.formattedMetadata);
    }
  }

  @Test
  void nestedMapStructAndArrayUseTheirOwnTimeUnits() throws Exception {
    Field key =
        new Field("key", FieldType.nullable(new ArrowType.Time(TimeUnit.MICROSECOND, 64)), null);
    Field at =
        new Field("at", FieldType.nullable(new ArrowType.Time(TimeUnit.MILLISECOND, 32)), null);
    Field element =
        new Field("element", FieldType.nullable(new ArrowType.Time(TimeUnit.SECOND, 32)), null);
    Field array =
        new Field(
            "array",
            FieldType.nullable(ArrowType.List.INSTANCE),
            Collections.singletonList(element));
    Field value =
        new Field("value", FieldType.nullable(ArrowType.Struct.INSTANCE), Arrays.asList(at, array));
    Field entries =
        new Field(
            "entries", FieldType.nullable(ArrowType.Struct.INSTANCE), Arrays.asList(key, value));
    Field map =
        new Field(
            "map",
            FieldType.nullable(new ArrowType.Map(false)),
            Collections.singletonList(entries));
    ComplexDataTypeParser parser = new ComplexDataTypeParser(true, null, map);
    String metadata = "MAP<TIME(6),STRUCT<at:TIME(3),array:ARRAY<TIME(0)>>>";
    String json =
        "[{\"key\":3723000100,\"value\":{\"at\":33300123,\"array\":[33300]}},"
            + "{\"key\":3723000200,\"value\":{\"at\":33300123,\"array\":[33300]}}]";
    String expected =
        "{\"01:02:03.0001\":{\"at\":\"09:15:00.123\",\"array\":[\"09:15:00\"]},"
            + "\"01:02:03.0002\":{\"at\":\"09:15:00.123\",\"array\":[\"09:15:00\"]}}";
    assertEquals(expected, parser.formatComplexTypeString(json, "MAP", metadata));
    DatabricksMap<?, ?> decoded = parser.parseJsonStringToDbMap(json, metadata);
    assertEquals(2, decoded.size());
    assertEquals(expected, decoded.toString());
  }

  @Test
  void unrequestedTimestampTargetIsRejected() {
    for (Object value :
        Arrays.asList(
            Timestamp.valueOf("2024-01-02 03:04:05"),
            java.time.OffsetDateTime.parse("2024-01-02T03:04:05Z"))) {
      assertThrows(
          DatabricksSQLException.class,
          () -> JavaTimeConverter.convert(LocalTime.class, Types.TIMESTAMP, "TIMESTAMP", value));
    }
  }

  @Test
  void timeMapKeysKeepDistinctFractionsAndQuotedText() throws Exception {
    ComplexDataTypeParser parser = new ComplexDataTypeParser();
    String json = "[{\"key\":3723000100000,\"value\":1},{\"key\":3723000200000,\"value\":2}]";
    String expected = "{\"01:02:03.0001\":1,\"01:02:03.0002\":2}";
    DatabricksMap<?, ?> map = parser.parseJsonStringToDbMap(json, "MAP<TIME(6),INT>");
    assertEquals(2, map.size());
    assertEquals(expected, map.toString());
    assertEquals(expected, parser.formatComplexTypeString(json, "MAP", "MAP<TIME(6),INT>"));
    for (boolean complex : new boolean[] {false, true}) {
      ColumnInfo column =
          new ColumnInfo()
              .setName("clocks")
              .setTypeName(ColumnInfoTypeName.MAP)
              .setTypeText("MAP<TIME(6),INT>");
      IDatabricksConnectionContext context = mock(IDatabricksConnectionContext.class);
      when(context.isComplexDatatypeSupportEnabled()).thenReturn(complex);
      DatabricksResultSetMetaData metadata =
          new DatabricksResultSetMetaData(
              new StatementId("time-review"),
              new ResultManifest()
                  .setSchema(
                      new ResultSchema()
                          .setColumnCount(1L)
                          .setColumns(Collections.singletonList(column))),
              false,
              context);
      IExecutionResult execution = mock(IExecutionResult.class);
      when(execution.getObject(0))
          .thenReturn(
              complex ? map : parser.formatComplexTypeString(json, "MAP", "MAP<TIME(6),INT>"));
      try (DatabricksResultSet result =
          new DatabricksResultSet(
              new StatementStatus().setState(StatementState.SUCCEEDED),
              new StatementId("time-review"),
              StatementType.QUERY,
              null,
              execution,
              metadata,
              complex)) {
        assertEquals(expected, result.getString(1));
        assertEquals(expected, result.getString("clocks"));
        if (complex) assertEquals(2, ((java.util.Map<?, ?>) result.getObject(1)).size());
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void localTimeRequestsOnOtherColumnsKeepExistingBehavior(boolean strict) throws Exception {
    IDatabricksSession session = mock(IDatabricksSession.class);
    IDatabricksConnectionContext context = mock(IDatabricksConnectionContext.class);
    when(session.getConnectionContext()).thenReturn(context);
    when(context.isStrictGetObjectConversionEnabled()).thenReturn(strict);
    java.util.List<ColumnInfo> columns =
        Arrays.asList(
            new ColumnInfo()
                .setName("ts")
                .setTypeName(ColumnInfoTypeName.TIMESTAMP)
                .setTypeText("TIMESTAMP"),
            new ColumnInfo()
                .setName("s")
                .setTypeName(ColumnInfoTypeName.STRING)
                .setTypeText("STRING"),
            new ColumnInfo().setName("i").setTypeName(ColumnInfoTypeName.INT).setTypeText("INT"));
    ResultManifest manifest =
        new ResultManifest()
            .setFormat(Format.JSON_ARRAY)
            .setTotalChunkCount(1L)
            .setTotalRowCount(1L)
            .setSchema(new ResultSchema().setColumns(columns).setColumnCount(3L));
    try (DatabricksResultSet result =
        new DatabricksResultSet(
            new StatementStatus().setState(StatementState.SUCCEEDED),
            new StatementId("time-review"),
            new ResultData()
                .setChunkIndex(0L)
                .setDataArray(
                    Collections.singletonList(
                        Arrays.asList("2024-01-02 03:04:05.678", "09:15:00", "5"))),
            manifest,
            StatementType.QUERY,
            session,
            null)) {
      assertTrue(result.next());
      for (int column = 1; column <= 3; column++) {
        final int index = column;
        if (strict) {
          DatabricksSQLException exception =
              assertThrows(
                  DatabricksSQLException.class, () -> result.getObject(index, LocalTime.class));
          assertEquals(
              DatabricksDriverErrorCode.UNSUPPORTED_OPERATION.name(), exception.getSQLState());
          assertEquals(1005, exception.getErrorCode());
        } else {
          Object value = result.getObject(column, LocalTime.class);
          assertEquals(result.getString(column), value);
        }
      }
    }
    assertNull(
        JavaTimeConverter.findJavaTimeMapping(
            Collections.singletonMap("TIMESTAMP", LocalTime.class), "TIMESTAMP"));
    DatabricksStruct struct =
        new ComplexDataTypeParser()
            .parseJsonStringToDbStruct("{\"ts\":1704164645000000}", "STRUCT<ts:TIMESTAMP>");
    assertInstanceOf(
        Timestamp.class,
        struct.getAttributes(Collections.singletonMap("TIMESTAMP", LocalTime.class))[0]);
  }

  @Test
  void describeAndArrayMetadataKeepTimePrecision() throws Exception {
    DatabricksResultSetMetaData describe =
        new DatabricksResultSetMetaData(
            new StatementId("time-review"),
            Collections.singletonList("clock"),
            Collections.singletonList("time(6)"),
            mock(IDatabricksConnectionContext.class));
    assertTimeMetadata(describe, 1);
    DatabricksArray array =
        new ComplexDataTypeParser().parseJsonStringToDbArray("[33300123456000]", "ARRAY<TIME(6)>");
    try (ResultSet result = array.getResultSet()) {
      assertTimeMetadata(result.getMetaData(), 2);
    }
  }

  private static void assertTimeMetadata(ResultSetMetaData metadata, int column) throws Exception {
    assertEquals(Types.TIME, metadata.getColumnType(column));
    assertEquals("java.sql.Time", metadata.getColumnClassName(column));
    assertEquals(15, metadata.getPrecision(column));
    assertEquals(6, metadata.getScale(column));
    assertEquals(15, metadata.getColumnDisplaySize(column));
  }

  @Test
  void timestampConversionUsesEpochDateAndExactClock() throws Exception {
    Timestamp expected = Timestamp.valueOf("1970-01-01 09:15:00.123456");
    assertEquals(expected, new TimeConverter().toTimestamp(LocalTime.parse("09:15:00.123456")));
  }

  @Test
  void nestedArrowTimeUsesChildUnits() throws Exception {
    try (RootAllocator allocator = new RootAllocator();
        ListVector vector = ListVector.empty("times", allocator)) {
      TimeMicroVector child =
          (TimeMicroVector)
              vector
                  .addOrGetVector(FieldType.nullable(new ArrowType.Time(TimeUnit.MICROSECOND, 64)))
                  .getVector();
      vector.allocateNew();
      vector.startNewValue(0);
      child.setSafe(0, 33300123456L);
      vector.endValue(0, 1);
      vector.setValueCount(1);
      Object result =
          ArrowToJavaObjectConverter.convert(
              vector, 0, ColumnInfoTypeName.ARRAY, "ARRAY<TIME(6)>", new ColumnInfo());
      assertEquals("[\"09:15:00.123456\"]", result.toString());
    }
  }

  @Test
  void scalarAndNestedDecodeErrorsAreSqlExceptionsWithoutCellText() throws Exception {
    try (RootAllocator allocator = new RootAllocator();
        BigIntVector vector = new BigIntVector("t", allocator)) {
      vector.allocateNew(1);
      vector.setSafe(0, 86400000000000L);
      DatabricksSQLException exception =
          assertThrows(
              DatabricksSQLException.class,
              () ->
                  ArrowToJavaObjectConverter.convert(
                      vector, 0, ColumnInfoTypeName.TIME, "TIME(6)", new ColumnInfo()));
      assertNotNull(exception.getCause());
      assertFalse(exception.getMessage().contains("86400000000000"));
      assertEquals(1005, exception.getErrorCode());
    }
    DatabricksParsingException exception =
        assertThrows(
            DatabricksParsingException.class,
            () ->
                new ComplexDataTypeParser()
                    .parseJsonStringToDbArray("[86400000000000]", "ARRAY<TIME(6)>"));
    assertNotNull(exception.getCause());
    assertFalse(exception.getMessage().contains("86400000000000"));
    DatabricksParsingException structError =
        assertThrows(
            DatabricksParsingException.class,
            () ->
                new ComplexDataTypeParser()
                    .parseJsonStringToDbStruct(
                        "{\"clock\":\"09:15:xx\"}", "STRUCT<clock:TIME(6)>"));
    assertNotNull(structError.getCause());
    assertEquals(1005, structError.getErrorCode());
    assertFalse(structError.getMessage().contains("09:15:xx"));
  }

  @Test
  void quotedFieldNamesAreNotTimeLeaves() {
    ComplexDataTypeParser parser = new ComplexDataTypeParser();
    assertEquals(
        "{\"a time, b\":1}",
        parser.formatComplexTypeString("{\"a time, b\":1}", "STRUCT", "STRUCT<`a time, b`:INT>"));
    assertEquals(
        "{\"a\":\"09:15:00.123456\"}",
        parser.formatComplexTypeString(
            "{\"a\":33300123456000}", "STRUCT", "STRUCT<a:TIME(6) COMMENT 'x'>"));
  }
}
