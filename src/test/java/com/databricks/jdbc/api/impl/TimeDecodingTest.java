package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.impl.converters.ArrowToJavaObjectConverter;
import com.databricks.jdbc.api.impl.converters.ConverterHelper;
import com.databricks.jdbc.api.impl.converters.JavaTimeConverter;
import com.databricks.jdbc.api.impl.converters.TimeConverter;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.util.DatabricksTypeUtil;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.client.thrift.generated.*;
import com.databricks.jdbc.model.core.*;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.service.sql.StatementState;
import java.sql.Time;
import java.sql.Types;
import java.time.LocalTime;
import java.util.Collections;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class TimeDecodingTest {
  @ParameterizedTest
  @CsvSource({
    "09:15:00,09:15:00",
    "09:15:00.123,09:15:00.123",
    "09:15:00.123456,09:15:00.123456",
    "23:59:59.999999,23:59:59.999999",
    "00:00:00,00:00:00",
    "09:15:00.120000,09:15:00.12"
  })
  void exactClockText(String input, String expected) throws Exception {
    assertEquals(expected, new TimeConverter().toString(LocalTime.parse(input)));
    assertEquals(expected, new TimeConverter().toString(input));
    assertEquals(expected, new TimeConverter().toString(LocalTime.parse(input).toNanoOfDay()));
  }

  @Test
  void physicalArrowTimeUnits() throws Exception {
    try (RootAllocator allocator = new RootAllocator();
        TimeSecVector seconds = new TimeSecVector("t", allocator);
        TimeMilliVector millis = new TimeMilliVector("t", allocator);
        TimeMicroVector micros = new TimeMicroVector("t", allocator);
        TimeNanoVector nanos = new TimeNanoVector("t", allocator)) {
      seconds.allocateNew(1);
      seconds.setSafe(0, 33300);
      millis.allocateNew(1);
      millis.setSafe(0, 33300123);
      micros.allocateNew(1);
      micros.setSafe(0, 33300123456L);
      nanos.allocateNew(1);
      nanos.setSafe(0, 33300123456789L);
      ValueVector[] vectors = {seconds, millis, micros, nanos};
      String[] clocks = {"09:15:00", "09:15:00.123", "09:15:00.123456", "09:15:00.123456789"};
      for (int index = 0; index < vectors.length; index++) {
        assertEquals(
            LocalTime.parse(clocks[index]),
            ArrowToJavaObjectConverter.convert(
                vectors[index], 0, ColumnInfoTypeName.STRING, "TIME(6)", new ColumnInfo()));
        assertEquals(
            LocalTime.parse(clocks[index]),
            ArrowToJavaObjectConverter.convert(vectors[index], 0, null, null, new ColumnInfo()));
      }
    }
  }

  @Test
  void arrowTimeMetadataFallback() {
    Field field =
        new Field(
            "t",
            new FieldType(
                true,
                new ArrowType.Time(TimeUnit.NANOSECOND, 64),
                null,
                Collections.singletonMap("SPARK::time::precision", "3")),
            null);
    assertEquals("TIME(3)", DatabricksTypeUtil.getArrowTypeText(field));
    assertFalse(DatabricksTypeUtil.isTimeType("TIMESTAMP_NTZ"));
    assertEquals(ColumnInfoTypeName.TIME, DatabricksTypeUtil.getColumnInfoType("time(3)"));
  }

  @Test
  void resultSetTimeGettersPreservePrecisionAndNulls() throws Exception {
    ColumnInfo column = new ColumnInfo().setName("clock").setTypeText("TIME(6)");
    DatabricksResultSetMetaData metadata =
        new DatabricksResultSetMetaData(
            new StatementId("time"),
            new ResultManifest()
                .setSchema(
                    new ResultSchema()
                        .setColumnCount(1L)
                        .setColumns(Collections.singletonList(column))),
            false,
            mock(IDatabricksConnectionContext.class));
    IExecutionResult execution = mock(IExecutionResult.class, CALLS_REAL_METHODS);
    LocalTime exact = LocalTime.parse("09:15:00.123456");
    when(execution.getObject(0)).thenReturn(exact);
    try (DatabricksResultSet result =
        new DatabricksResultSet(
            new StatementStatus().setState(StatementState.SUCCEEDED),
            new StatementId("time"),
            StatementType.QUERY,
            null,
            execution,
            metadata,
            false)) {
      assertEquals("09:15:00.123456", result.getString(1));
      assertEquals(result.getString(1), result.getString("clock"));
      assertEquals(exact, result.getObject(1, LocalTime.class));
      assertEquals(exact, result.getObject("clock", LocalTime.class));
      Time time = result.getTime(1);
      assertEquals(123, Math.floorMod(time.getTime(), 1000));
      assertEquals(time, result.getTime("clock"));
      assertEquals(time, result.getObject(1));
      assertEquals(time, result.getObject("clock"));
      assertFalse(result.wasNull());
      when(execution.getObject(0)).thenReturn(null);
      assertNull(result.getObject(1, LocalTime.class));
      assertTrue(result.wasNull());
      assertNull(result.getString(1));
      assertNull(result.getTime(1));
      assertNull(result.getObject(1));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 3, 6})
  void thriftEmptyAndNonEmptyTimeMetadata(int precision) throws Exception {
    IDatabricksConnectionContext context = mock(IDatabricksConnectionContext.class);
    TTypeDesc type =
        new TTypeDesc()
            .setTypes(
                Collections.singletonList(
                    TTypeEntry.primitiveEntry(new TPrimitiveTypeEntry(TTypeId.STRING_TYPE))));
    TColumnDesc column = new TColumnDesc().setColumnName("t").setTypeDesc(type).setPosition(0);
    TGetResultSetMetadataResp manifest =
        new TGetResultSetMetadataResp()
            .setSchema(new TTableSchema().setColumns(Collections.singletonList(column)));
    for (long rows : new long[] {0, 1}) {
      DatabricksResultSetMetaData metadata =
          new DatabricksResultSetMetaData(
              new StatementId("time"),
              manifest,
              rows,
              1,
              Collections.singletonList("TIME(" + precision + ")"),
              context);
      assertEquals(Types.TIME, metadata.getColumnType(1));
      assertEquals("TIME", metadata.getColumnTypeName(1));
      assertEquals("java.sql.Time", metadata.getColumnClassName(1));
      assertEquals(precision, metadata.getScale(1));
      assertEquals(precision == 0 ? 8 : 9 + precision, metadata.getColumnDisplaySize(1));
    }
  }

  @Test
  void invalidTimeUsesExistingErrorCode() {
    DatabricksSQLException exception =
        assertThrows(DatabricksSQLException.class, () -> new TimeConverter().toTime("not a time"));
    assertEquals(DatabricksDriverErrorCode.RESULT_SET_ERROR.name(), exception.getSQLState());
    assertEquals(DatabricksDriverErrorCode.RESULT_SET_ERROR.getCode(), exception.getErrorCode());
    assertFalse(exception.getMessage().contains("not a time"));
  }

  @Test
  void nestedTypedTimePreservesExactText() throws Exception {
    ComplexDataTypeParser parser = new ComplexDataTypeParser();
    DatabricksArray array =
        parser.parseJsonStringToDbArray(
            "[\"09:15:00.123456\",null,\"00:00:00\"]", "ARRAY<TIME(6)>");
    assertEquals(Types.TIME, array.getBaseType());
    assertInstanceOf(Time.class, ((Object[]) array.getArray())[0]);
    assertEquals(
        LocalTime.parse("09:15:00.123456"),
        ((Object[]) array.getArray(Collections.singletonMap("TIME", LocalTime.class)))[0]);
    assertEquals("[\"09:15:00.123456\",null,\"00:00:00\"]", array.toString());
    DatabricksMap<String, Object> map =
        parser.parseJsonStringToDbMap(
            "[{\"key\":\"a\",\"value\":\"09:15:00.123456\"},{\"key\":\"b\",\"value\":null}]",
            "MAP<STRING,TIME(6)>");
    assertInstanceOf(Time.class, map.get("a"));
    assertEquals("{\"a\":\"09:15:00.123456\",\"b\":null}", map.toString());
    DatabricksStruct struct =
        parser.parseJsonStringToDbStruct(
            "{\"at\":\"09:15:00.123456\",\"inner\":{\"at\":\"23:59:59.999999\"}}",
            "STRUCT<at:TIME(6),inner:STRUCT<at:TIME(6)>>");
    assertInstanceOf(Time.class, struct.getAttributes()[0]);
    assertEquals(
        LocalTime.parse("09:15:00.123456"),
        struct.getAttributes(Collections.singletonMap("TIME", LocalTime.class))[0]);
    assertEquals(
        "{\"at\":\"09:15:00.123456\",\"inner\":{\"at\":\"23:59:59.999999\"}}", struct.toString());
  }

  @Test
  void mutatedContainersDoNotReuseOriginalTime() throws Exception {
    ComplexDataTypeParser parser = new ComplexDataTypeParser();
    DatabricksArray array =
        parser.parseJsonStringToDbArray("[\"09:15:00.123456\"]", "ARRAY<TIME(6)>");
    ((Object[]) array.getArray())[0] = Time.valueOf("12:34:56");
    assertEquals("[\"12:34:56\"]", array.toString());
    assertEquals(
        LocalTime.parse("12:34:56"),
        ((Object[]) array.getArray(Collections.singletonMap("TIME", LocalTime.class)))[0]);
    DatabricksStruct struct =
        parser.parseJsonStringToDbStruct("{\"at\":\"09:15:00.123456\"}", "STRUCT<at:TIME(6)>");
    ((Time) struct.getAttributes()[0]).setTime(Time.valueOf("12:34:56").getTime());
    assertEquals("{\"at\":\"12:34:56\"}", struct.toString());
    assertEquals(
        LocalTime.parse("12:34:56"),
        struct.getAttributes(Collections.singletonMap("TIME", LocalTime.class))[0]);
    DatabricksMap<String, Object> map =
        parser.parseJsonStringToDbMap("{\"a\":\"09:15:00.123456\"}", "MAP<STRING,TIME(6)>");
    map.put("a", Time.valueOf("12:34:56"));
    assertEquals("{\"a\":\"12:34:56\"}", map.toString());
  }

  @Test
  void scalarAndNullArrowValues() throws Exception {
    try (RootAllocator allocator = new RootAllocator();
        BigIntVector vector = new BigIntVector("time", allocator)) {
      vector.allocateNew(3);
      vector.setSafe(0, 33300123456000L);
      vector.setSafe(1, 0L);
      vector.setNull(2);
      vector.setValueCount(3);
      assertEquals(
          LocalTime.parse("09:15:00.123456"),
          ArrowToJavaObjectConverter.convert(vector, 0, null, "TIME(6)", new ColumnInfo()));
      assertEquals(
          LocalTime.MIDNIGHT,
          ArrowToJavaObjectConverter.convert(
              vector, 1, ColumnInfoTypeName.STRING, "TIME(6)", new ColumnInfo()));
      assertNull(ArrowToJavaObjectConverter.convert(vector, 2, null, "TIME(6)", new ColumnInfo()));
    }
  }

  @Test
  void jdbcTimeSurfaces() throws Exception {
    Object exact = LocalTime.parse("09:15:00.123456");
    assertEquals(
        "09:15:00.123456",
        ConverterHelper.getConverterForColumnType(Types.TIME, "TIME").toString(exact));
    Time time = (Time) ConverterHelper.convertSqlTypeToJavaType(Types.TIME, exact);
    assertEquals("09:15:00", time.toString());
    assertEquals(123, Math.floorMod(time.getTime(), 1000));
    assertEquals(exact, JavaTimeConverter.convert(LocalTime.class, Types.TIME, "TIME", exact));
  }

  @Test
  void seaEmptyTimeMetadata() throws Exception {
    IDatabricksConnectionContext context = mock(IDatabricksConnectionContext.class);
    ColumnInfo column = new ColumnInfo().setName("t").setTypeText("TIME(6)");
    ResultManifest manifest =
        new ResultManifest()
            .setSchema(
                new ResultSchema().setColumnCount(1L).setColumns(Collections.singletonList(column)))
            .setTotalRowCount(0L);
    DatabricksResultSetMetaData metadata =
        new DatabricksResultSetMetaData(new StatementId("time"), manifest, false, context);
    assertEquals(Types.TIME, metadata.getColumnType(1));
    assertEquals("TIME", metadata.getColumnTypeName(1));
    assertEquals("java.sql.Time", metadata.getColumnClassName(1));
    assertEquals(
        Types.TIME,
        DatabricksTypeUtil.getColumnType(DatabricksTypeUtil.getColumnInfoType("TIME(6)")));
  }

  @Test
  void nestedTimeText() throws Exception {
    ComplexDataTypeParser parser = new ComplexDataTypeParser();
    assertEquals(
        "[\"09:15:30.5\",null]",
        parser.formatComplexTypeString("[\"09:15:30.5\",null]", "ARRAY", "ARRAY<TIME(6)>"));
    assertEquals(
        "{\"a\":\"09:15:30.5\",\"b\":null}",
        parser.formatComplexTypeString(
            "[{\"key\":\"a\",\"value\":\"09:15:30.5\"},{\"key\":\"b\",\"value\":null}]",
            "MAP",
            "MAP<STRING,TIME(6)>"));
    assertEquals(
        "{\"at\":\"09:15:30.5\",\"inner\":{\"at\":\"23:59:59.999999\"}}",
        parser.formatComplexTypeString(
            "{\"at\":\"09:15:30.5\",\"inner\":{\"at\":\"23:59:59.999999\"}}",
            "STRUCT",
            "STRUCT<at:TIME(6),inner:STRUCT<at:TIME(6)>>"));
  }
}
