package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
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
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/** Pins getter results for Arrow-backed SEA result sets, including nulls. */
@ExtendWith(MockitoExtension.class)
class DatabricksResultSetArrowConversionTest {
  private static final String JDBC_URL =
      "jdbc:databricks://sample-host.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;"
          + "AuthMech=3;httpPath=/sql/1.0/warehouses/99999999;";
  private static final String TIME_ZONE = "Etc/UTC";
  private static final long TS_MICROS = 1_700_000_000_123_456L;

  @Mock IDatabricksSession session;

  @Test
  void testPrimitiveAndCommonTypesThroughArrowStream() throws Exception {
    when(session.getConnectionContext())
        .thenReturn(DatabricksConnectionContextFactory.create(JDBC_URL, new Properties()));

    try (DatabricksResultSet rs = newResultSet()) {
      // Row 1: values
      assertTrue(rs.next());
      assertTrue(rs.getBoolean(1));
      assertFalse(rs.wasNull());
      assertEquals(Boolean.TRUE, rs.getObject(1));
      assertEquals(1, rs.getInt(1));
      assertEquals("true", rs.getString(1));

      assertEquals(7, rs.getInt(2));
      assertEquals(Integer.valueOf(7), rs.getObject(2));
      assertEquals(7L, rs.getLong(2));
      assertEquals(7.0, rs.getDouble(2));
      assertEquals("7", rs.getString(2));

      assertEquals(1_234_567_890_123L, rs.getLong(3));
      assertEquals(Long.valueOf(1_234_567_890_123L), rs.getObject(3));
      assertEquals("1234567890123", rs.getString(3));

      assertEquals(1.5, rs.getDouble(4));
      assertEquals(Double.valueOf(1.5), rs.getObject(4));
      assertEquals(1.5f, rs.getFloat(4));
      assertEquals(1, rs.getInt(4));
      assertEquals("1.5", rs.getString(4));

      assertEquals(2.25f, rs.getFloat(5));
      assertEquals(Float.valueOf(2.25f), rs.getObject(5));
      assertEquals(2.25, rs.getDouble(5));
      assertEquals("2.25", rs.getString(5));

      assertEquals("héllo", rs.getString(6));
      assertEquals("héllo", rs.getObject(6));

      Timestamp expectedTs =
          Timestamp.valueOf(
              LocalDateTime.ofInstant(
                  Instant.ofEpochSecond(TS_MICROS / 1_000_000, (TS_MICROS % 1_000_000) * 1_000),
                  ZoneId.of(TIME_ZONE)));
      assertEquals(expectedTs, rs.getTimestamp(7));
      assertEquals(expectedTs, rs.getObject(7));
      assertFalse(rs.wasNull());

      assertEquals(new BigDecimal("12345.67"), rs.getBigDecimal(8));
      assertEquals(new BigDecimal("12345.67"), rs.getObject(8));
      assertEquals(new BigDecimal("12345.7"), rs.getBigDecimal(8, 1));
      assertEquals(12345.67, rs.getDouble(8));
      assertEquals("12345.67", rs.getString(8));

      // Row 2: nulls in every column
      assertTrue(rs.next());
      assertFalse(rs.getBoolean(1));
      assertTrue(rs.wasNull());
      assertEquals(0, rs.getInt(2));
      assertTrue(rs.wasNull());
      assertEquals(0L, rs.getLong(3));
      assertTrue(rs.wasNull());
      assertEquals(0.0, rs.getDouble(4));
      assertTrue(rs.wasNull());
      assertEquals(0.0f, rs.getFloat(5));
      assertTrue(rs.wasNull());
      assertNull(rs.getString(6));
      assertTrue(rs.wasNull());
      assertNull(rs.getTimestamp(7));
      assertTrue(rs.wasNull());
      assertNull(rs.getBigDecimal(8));
      assertTrue(rs.wasNull());
      for (int col = 1; col <= 8; col++) {
        assertNull(rs.getObject(col), "column " + col);
        assertTrue(rs.wasNull());
      }

      // Row 3: negative and boundary values
      assertTrue(rs.next());
      assertFalse(rs.getBoolean(1));
      assertFalse(rs.wasNull());
      assertEquals(Integer.MIN_VALUE, rs.getInt(2));
      assertEquals(Long.MAX_VALUE, rs.getLong(3));
      assertEquals(-0.0, rs.getDouble(4));
      assertEquals(-0.5f, rs.getFloat(5));
      assertEquals("", rs.getString(6));
      assertFalse(rs.wasNull());
      assertEquals(Timestamp.valueOf(LocalDateTime.of(1970, 1, 1, 0, 0)), rs.getTimestamp(7));
      assertEquals(new BigDecimal("-0.01"), rs.getBigDecimal(8));

      assertFalse(rs.next());
    }
  }

  private DatabricksResultSet newResultSet() throws Exception {
    List<ColumnInfo> columns = new ArrayList<>();
    columns.add(column(0, ColumnInfoTypeName.BOOLEAN, "BOOLEAN"));
    columns.add(column(1, ColumnInfoTypeName.INT, "INT"));
    columns.add(column(2, ColumnInfoTypeName.LONG, "BIGINT"));
    columns.add(column(3, ColumnInfoTypeName.DOUBLE, "DOUBLE"));
    columns.add(column(4, ColumnInfoTypeName.FLOAT, "FLOAT"));
    columns.add(column(5, ColumnInfoTypeName.STRING, "STRING"));
    columns.add(column(6, ColumnInfoTypeName.TIMESTAMP, "TIMESTAMP"));
    columns.add(
        column(7, ColumnInfoTypeName.DECIMAL, "DECIMAL(18,2)")
            .setTypePrecision(18L)
            .setTypeScale(2L));
    ResultManifest manifest =
        new ResultManifest()
            .setFormat(SqlExecResultFormat.ARROW_STREAM)
            .setSchema(new ResultSchema().setColumnCount((long) columns.size()).setColumns(columns))
            .setTotalRowCount(3L)
            .setTotalChunkCount(1L);
    ResultData data = new ResultData();
    data.setAttachment(arrowBytes());
    return new DatabricksResultSet(
        new StatementStatus().setState(StatementState.SUCCEEDED),
        new StatementId("statement-id"),
        data,
        manifest,
        StatementType.QUERY,
        session,
        null);
  }

  private static ColumnInfo column(long position, ColumnInfoTypeName typeName, String typeText) {
    return new ColumnInfo()
        .setName("c" + position)
        .setPosition(position)
        .setTypeName(typeName)
        .setTypeText(typeText);
  }

  private static Field field(String name, ArrowType type, String sqlName) {
    return new Field(
        name,
        new FieldType(
            true, type, null, Collections.singletonMap("Spark:DataType:SqlName", sqlName)),
        null);
  }

  private static byte[] arrowBytes() throws Exception {
    List<Field> fields = new ArrayList<>();
    fields.add(field("c0", ArrowType.Bool.INSTANCE, "BOOLEAN"));
    fields.add(field("c1", new ArrowType.Int(32, true), "INT"));
    fields.add(field("c2", new ArrowType.Int(64, true), "BIGINT"));
    fields.add(field("c3", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), "DOUBLE"));
    fields.add(field("c4", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), "FLOAT"));
    fields.add(field("c5", ArrowType.Utf8.INSTANCE, "STRING"));
    fields.add(field("c6", new ArrowType.Timestamp(TimeUnit.MICROSECOND, TIME_ZONE), "TIMESTAMP"));
    fields.add(field("c7", new ArrowType.Decimal(18, 2, 128), "DECIMAL(18,2)"));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(fields), allocator);
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
      root.allocateNew();
      BitVector booleans = (BitVector) root.getVector(0);
      IntVector ints = (IntVector) root.getVector(1);
      BigIntVector bigints = (BigIntVector) root.getVector(2);
      Float8Vector doubles = (Float8Vector) root.getVector(3);
      Float4Vector floats = (Float4Vector) root.getVector(4);
      VarCharVector strings = (VarCharVector) root.getVector(5);
      TimeStampMicroTZVector timestamps = (TimeStampMicroTZVector) root.getVector(6);
      DecimalVector decimals = (DecimalVector) root.getVector(7);

      booleans.setSafe(0, 1);
      ints.setSafe(0, 7);
      bigints.setSafe(0, 1_234_567_890_123L);
      doubles.setSafe(0, 1.5);
      floats.setSafe(0, 2.25f);
      strings.setSafe(0, "héllo".getBytes(StandardCharsets.UTF_8));
      timestamps.setSafe(0, TS_MICROS);
      decimals.setSafe(0, new BigDecimal("12345.67"));
      // Row 1 is left null in every vector.
      booleans.setSafe(2, 0);
      ints.setSafe(2, Integer.MIN_VALUE);
      bigints.setSafe(2, Long.MAX_VALUE);
      doubles.setSafe(2, -0.0);
      floats.setSafe(2, -0.5f);
      strings.setSafe(2, new byte[0]);
      timestamps.setSafe(2, 0L);
      decimals.setSafe(2, new BigDecimal("-0.01"));
      root.getFieldVectors().forEach(v -> v.setValueCount(3));
      root.setRowCount(3);
      writer.start();
      writer.writeBatch();
      writer.end();
    }
    return out.toByteArray();
  }
}
