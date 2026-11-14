package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.impl.converters.WKTConverter;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.model.core.ArrowBatch;
import com.databricks.jdbc.model.core.ColumnInfo;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.core.ResultSchema;
import com.databricks.jdbc.model.core.SqlExecResultFormat;
import com.databricks.jdbc.model.core.StatementStatus;
import com.databricks.sdk.service.sql.StatementState;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.SchemaUtility;
import org.junit.jupiter.api.Test;

/** APC bare batches with the complete manifest type matrix, read through JDBC. */
class ApcResultTypeMatrixTest {
  private static ResultManifest manifest() {
    List<ColumnInfo> columns = new ArrayList<>();
    add(columns, "b", ColumnInfoTypeName.BOOLEAN, "BOOLEAN", "boolean");
    add(columns, "tiny", ColumnInfoTypeName.TINYINT, "TINYINT", "byte");
    add(columns, "small", ColumnInfoTypeName.SMALLINT, "SMALLINT", "short");
    add(columns, "i", ColumnInfoTypeName.INT, "INT", "integer");
    add(columns, "big", ColumnInfoTypeName.BIGINT, "BIGINT", "long");
    add(columns, "f", ColumnInfoTypeName.FLOAT, "FLOAT", "float");
    add(columns, "d", ColumnInfoTypeName.DOUBLE, "DOUBLE", "double");
    add(columns, "s", ColumnInfoTypeName.STRING, "STRING", "string");
    add(columns, "bin", ColumnInfoTypeName.BINARY, "BINARY", "binary");
    add(columns, "dt", ColumnInfoTypeName.DATE, "DATE", "date");
    add(columns, "dec", ColumnInfoTypeName.DECIMAL, "DECIMAL(18,2)", "decimal(18,2)");
    add(columns, "ts", ColumnInfoTypeName.TIMESTAMP, "TIMESTAMP", "timestamp");
    add(columns, "ntz", ColumnInfoTypeName.TIMESTAMP_NTZ, "TIMESTAMP_NTZ", "timestamp_ntz");
    add(columns, "ym", ColumnInfoTypeName.INTERVAL, "INTERVAL YEAR TO MONTH", "yearMonthInterval");
    add(columns, "ds", ColumnInfoTypeName.INTERVAL, "INTERVAL DAY TO SECOND", "dayTimeInterval");
    add(columns, "geo", ColumnInfoTypeName.GEOGRAPHY, "GEOGRAPHY(4326)", "geography");
    add(columns, "geom", ColumnInfoTypeName.GEOMETRY, "GEOMETRY(4326)", "geometry");
    add(columns, "v", ColumnInfoTypeName.VARIANT, "VARIANT", "variant");
    add(
        columns,
        "arr",
        ColumnInfoTypeName.ARRAY,
        "ARRAY<INT>",
        "{\"type\":\"array\",\"elementType\":\"integer\",\"containsNull\":true}");
    add(
        columns,
        "map",
        ColumnInfoTypeName.MAP,
        "MAP<STRING,INT>",
        "{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"integer\",\"valueContainsNull\":true}");
    add(
        columns,
        "st",
        ColumnInfoTypeName.STRUCT,
        "STRUCT<x:INT>",
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"x\",\"type\":\"integer\",\"nullable\":true}]}");
    return new ResultManifest()
        .setFormat(SqlExecResultFormat.ARROW_BATCH)
        .setResultCompression(CompressionCodec.NONE)
        .setTotalChunkCount(0L)
        .setTotalRowCount(0L)
        .setSchema(new ResultSchema().setColumnCount((long) columns.size()).setColumns(columns));
  }

  private static void add(
      List<ColumnInfo> columns, String name, ColumnInfoTypeName type, String text, String json) {
    columns.add(
        new ColumnInfo()
            .setName(name)
            .setPosition((long) columns.size())
            .setTypeName(type)
            .setTypeText(text)
            .setTypeJson(json));
  }

  private static byte[] bareBatch() throws Exception {
    // Pin the physical Arrow fixture independently from ArrowBatchSchemaBuilder. The APC decoder
    // must reconstruct a schema that can read these bare batches from manifest type_json.
    Schema schema = pinnedPhysicalSchema();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (RootAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
      root.allocateNew();
      ((BitVector) root.getVector("b")).setSafe(0, 1);
      ((TinyIntVector) root.getVector("tiny")).setSafe(0, 4);
      ((SmallIntVector) root.getVector("small")).setSafe(0, 5);
      ((IntVector) root.getVector("i")).setSafe(0, 6);
      ((BigIntVector) root.getVector("big")).setSafe(0, 7);
      ((Float4Vector) root.getVector("f")).setSafe(0, 1.5f);
      ((Float8Vector) root.getVector("d")).setSafe(0, 2.5);
      ((VarCharVector) root.getVector("s")).setSafe(0, "hello".getBytes(StandardCharsets.UTF_8));
      ((VarBinaryVector) root.getVector("bin")).setSafe(0, new byte[] {1, 2});
      ((DateDayVector) root.getVector("dt")).setSafe(0, 0);
      ((DecimalVector) root.getVector("dec")).setSafe(0, new BigDecimal("12.34"));
      ((TimeStampMicroTZVector) root.getVector("ts")).setSafe(0, 0L);
      ((TimeStampMicroVector) root.getVector("ntz")).setSafe(0, 0L);
      ((IntervalYearVector) root.getVector("ym")).setSafe(0, 14);
      ((IntervalDayVector) root.getVector("ds")).setSafe(0, 1, 1000);
      setGeo((StructVector) root.getVector("geo"));
      setGeo((StructVector) root.getVector("geom"));
      ((VarCharVector) root.getVector("v")).setSafe(0, "1".getBytes(StandardCharsets.UTF_8));
      ListVector array = (ListVector) root.getVector("arr");
      int arrayStart = array.startNewValue(0);
      ((IntVector) array.getDataVector()).setSafe(arrayStart, 42);
      array.endValue(0, 1);
      MapVector map = (MapVector) root.getVector("map");
      int mapStart = map.startNewValue(0);
      StructVector entries = (StructVector) map.getDataVector();
      entries.setIndexDefined(mapStart);
      ((VarCharVector) entries.getChild("key"))
          .setSafe(mapStart, "k".getBytes(StandardCharsets.UTF_8));
      ((IntVector) entries.getChild("value")).setSafe(mapStart, 8);
      map.endValue(0, 1);
      StructVector st = (StructVector) root.getVector("st");
      st.setIndexDefined(0);
      ((IntVector) st.getChild("x")).setSafe(0, 9);
      root.getFieldVectors().forEach(vector -> vector.setValueCount(1));
      root.setRowCount(1);
      writer.start();
      writer.writeBatch();
      writer.end();
    }
    byte[] stream = out.toByteArray();
    return Arrays.copyOfRange(stream, SchemaUtility.serialize(schema).length, stream.length - 8);
  }

  private static Schema pinnedPhysicalSchema() {
    ArrowType.Int i8 = new ArrowType.Int(8, true);
    ArrowType.Int i16 = new ArrowType.Int(16, true);
    ArrowType.Int i32 = new ArrowType.Int(32, true);
    ArrowType.Int i64 = new ArrowType.Int(64, true);
    List<Field> geoChildren =
        List.of(
            new Field("srid", FieldType.notNullable(i32), null),
            new Field("wkb", FieldType.notNullable(ArrowType.Binary.INSTANCE), null));
    Field mapEntries =
        new Field(
            "entries",
            FieldType.notNullable(ArrowType.Struct.INSTANCE),
            List.of(
                new Field("key", FieldType.notNullable(ArrowType.Utf8.INSTANCE), null),
                new Field("value", FieldType.nullable(i32), null)));
    return new Schema(
        List.of(
            field("b", ArrowType.Bool.INSTANCE),
            field("tiny", i8),
            field("small", i16),
            field("i", i32),
            field("big", i64),
            field("f", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
            field("d", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            field("s", ArrowType.Utf8.INSTANCE),
            field("bin", ArrowType.Binary.INSTANCE),
            field("dt", new ArrowType.Date(DateUnit.DAY)),
            field("dec", new ArrowType.Decimal(18, 2, 128)),
            field("ts", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")),
            field("ntz", new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)),
            field("ym", new ArrowType.Interval(IntervalUnit.YEAR_MONTH)),
            field("ds", new ArrowType.Interval(IntervalUnit.DAY_TIME)),
            new Field("geo", FieldType.nullable(ArrowType.Struct.INSTANCE), geoChildren),
            new Field("geom", FieldType.nullable(ArrowType.Struct.INSTANCE), geoChildren),
            field("v", ArrowType.Utf8.INSTANCE),
            new Field(
                "arr", FieldType.nullable(ArrowType.List.INSTANCE), List.of(field("element", i32))),
            new Field("map", FieldType.nullable(new ArrowType.Map(false)), List.of(mapEntries)),
            new Field(
                "st", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(field("x", i32)))));
  }

  private static Field field(String name, ArrowType type) {
    return new Field(name, FieldType.nullable(type), null);
  }

  private static void setGeo(StructVector vector) throws Exception {
    vector.setIndexDefined(0);
    ((IntVector) vector.getChild("srid")).setSafe(0, 4326);
    ((VarBinaryVector) vector.getChild("wkb")).setSafe(0, WKTConverter.toWKB("POINT(1 2)"));
  }

  @Test
  void completeMatrixKeepsValuesAndJdbcMetadata() throws Exception {
    ResultManifest manifest = manifest();
    ResultData data =
        new ResultData()
            .setArrowBatches(List.of(new ArrowBatch().setBatch(bareBatch()).setRowCount(1L)));
    IDatabricksConnectionContext context = mock(IDatabricksConnectionContext.class);
    when(context.getHttpMaxConnectionsPerRoute()).thenReturn(100);
    when(context.isComplexDatatypeSupportEnabled()).thenReturn(true);
    when(context.isGeoSpatialSupportEnabled()).thenReturn(true);
    when(context.isTimestampNtzTypeNameEnabled()).thenReturn(true);
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getConnectionContext()).thenReturn(context);
    DatabricksResultSet result =
        new DatabricksResultSet(
            new StatementStatus().setState(StatementState.SUCCEEDED),
            new StatementId("statement"),
            data,
            manifest,
            StatementType.QUERY,
            session,
            null);
    try {
      assertTrue(result.next());
      assertEquals(21, result.getMetaData().getColumnCount());
      int[] thriftJdbcTypes = {
        Types.BOOLEAN,
        Types.TINYINT,
        Types.SMALLINT,
        Types.INTEGER,
        Types.BIGINT,
        Types.FLOAT,
        Types.DOUBLE,
        Types.VARCHAR,
        Types.BINARY,
        Types.DATE,
        Types.DECIMAL,
        Types.TIMESTAMP,
        Types.TIMESTAMP,
        Types.VARCHAR,
        Types.VARCHAR,
        Types.OTHER,
        Types.OTHER,
        Types.OTHER,
        Types.ARRAY,
        Types.VARCHAR,
        Types.STRUCT
      };
      ColumnInfo[] columns = manifest.getSchema().getColumns().toArray(new ColumnInfo[0]);
      for (int column = 1; column <= columns.length; column++) {
        assertEquals(columns[column - 1].getName(), result.getMetaData().getColumnName(column));
        assertEquals(thriftJdbcTypes[column - 1], result.getMetaData().getColumnType(column));
      }
      assertEquals("TIMESTAMP_NTZ", result.getMetaData().getColumnTypeName(13));
      assertEquals(true, result.getObject(1));
      assertEquals((byte) 4, result.getObject(2));
      assertEquals((short) 5, result.getObject(3));
      assertEquals(6, result.getObject(4));
      assertEquals(7L, result.getObject(5));
      assertEquals(1.5f, result.getObject(6));
      assertEquals(2.5d, result.getObject(7));
      assertEquals("hello", result.getObject(8));
      assertArrayEquals(new byte[] {1, 2}, (byte[]) result.getObject(9));
      assertEquals(Date.valueOf("1970-01-01"), result.getObject(10));
      assertEquals(new BigDecimal("12.34"), result.getObject(11));
      assertEquals(Timestamp.valueOf("1970-01-01 00:00:00"), result.getObject(12));
      assertEquals(Timestamp.valueOf("1970-01-01 00:00:00"), result.getObject(13));
      assertEquals("1-2", result.getObject(14));
      assertEquals("1 00:00:01.000000000", result.getObject(15));
      assertEquals("POINT(1 2)", ((DatabricksGeography) result.getObject(16)).getWKT());
      assertEquals("POINT(1 2)", ((DatabricksGeometry) result.getObject(17)).getWKT());
      assertEquals("1", result.getObject(18));
      assertArrayEquals(
          new Object[] {42}, (Object[]) ((DatabricksArray) result.getObject(19)).getArray());
      assertEquals(8, ((DatabricksMap<?, ?>) result.getObject(20)).get("k"));
      assertArrayEquals(
          new Object[] {9}, ((DatabricksStruct) result.getObject(21)).getAttributes());
      for (int column = 1; column <= 21; column++) {
        assertNotNull(result.getObject(column), columns[column - 1].getName());
      }
      assertTrue(result.isLast());
      assertFalse(result.isAfterLast());
      assertFalse(result.next());
      assertTrue(result.isAfterLast());
    } finally {
      result.close();
    }
  }
}
