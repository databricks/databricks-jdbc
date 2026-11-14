package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.client.thrift.generated.TCLIServiceConstants;
import com.databricks.jdbc.model.client.thrift.generated.TColumn;
import com.databricks.jdbc.model.client.thrift.generated.TColumnDesc;
import com.databricks.jdbc.model.client.thrift.generated.TFetchResultsResp;
import com.databricks.jdbc.model.client.thrift.generated.TGetResultSetMetadataResp;
import com.databricks.jdbc.model.client.thrift.generated.TPrimitiveTypeEntry;
import com.databricks.jdbc.model.client.thrift.generated.TRowSet;
import com.databricks.jdbc.model.client.thrift.generated.TSparkArrowBatch;
import com.databricks.jdbc.model.client.thrift.generated.TSparkRowSetType;
import com.databricks.jdbc.model.client.thrift.generated.TStringColumn;
import com.databricks.jdbc.model.client.thrift.generated.TTableSchema;
import com.databricks.jdbc.model.client.thrift.generated.TTypeDesc;
import com.databricks.jdbc.model.client.thrift.generated.TTypeEntry;
import com.databricks.jdbc.model.client.thrift.generated.TTypeId;
import com.databricks.jdbc.model.client.thrift.generated.TTypeQualifierValue;
import com.databricks.jdbc.model.client.thrift.generated.TTypeQualifiers;
import com.databricks.jdbc.model.core.ColumnInfo;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import com.databricks.jdbc.model.core.ColumnVector;
import com.databricks.jdbc.model.core.ColumnarData;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.core.ResultSchema;
import com.databricks.jdbc.model.core.SqlExecResultFormat;
import com.databricks.jdbc.model.core.StatementStatus;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.service.sql.StatementState;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.ResultSetMetaData;
import java.sql.Struct;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ColumnarDataResultTest {
  @Mock IDatabricksSession session;
  @Mock DatabricksConnectionContext context;

  private static <T> ColumnVector.Values<T> values(List<T> values) {
    return new ColumnVector.Values<>(values);
  }

  private static ResultData chunk(ColumnVector... columns) {
    return new ResultData().setColumnarData(new ColumnarData().setColumns(List.of(columns)));
  }

  private static ResultManifest manifest(SqlExecResultFormat format, ColumnInfo... columns) {
    return new ResultManifest()
        .setFormat(format)
        .setSchema(
            new ResultSchema().setColumnCount((long) columns.length).setColumns(List.of(columns)));
  }

  private static ColumnInfo column(
      String name, ColumnInfoTypeName type, String text, int position) {
    return new ColumnInfo()
        .setName(name)
        .setTypeName(type)
        .setTypeText(text)
        .setPosition((long) position);
  }

  @Test
  void readsEveryPhysicalArmAndNullMasksAcrossByteBoundary() throws Exception {
    List<Integer> integers = IntStream.range(0, 10).boxed().collect(Collectors.toList());
    List<Long> longs = integers.stream().map(Integer::longValue).collect(Collectors.toList());
    List<Short> shorts = integers.stream().map(Integer::shortValue).collect(Collectors.toList());
    List<Double> doubles = integers.stream().map(Integer::doubleValue).collect(Collectors.toList());
    List<String> strings = integers.stream().map(String::valueOf).collect(Collectors.toList());
    List<byte[]> binaries =
        integers.stream().map(value -> new byte[] {value.byteValue()}).collect(Collectors.toList());
    ColumnVector bools =
        new ColumnVector()
            .setBoolValues(
                values(IntStream.range(0, 10).mapToObj(i -> true).collect(Collectors.toList())))
            .setNullMask(new byte[] {0x02, 0x01});
    ColumnVector bytes =
        new ColumnVector().setByteValues(values(integers)).setNullMask(new byte[] {0x01});
    ColumnVector i16 = new ColumnVector().setI16Values(values(shorts));
    ColumnVector i32 = new ColumnVector().setI32Values(values(integers));
    ColumnVector i64 = new ColumnVector().setI64Values(values(longs));
    ColumnVector dbl = new ColumnVector().setDoubleValues(values(doubles));
    ColumnVector str = new ColumnVector().setStringValues(values(strings));
    ColumnVector binary =
        new ColumnVector().setBinaryValues(values(binaries)).setNullMask(new byte[] {0, 0, -1});
    ColumnarDataResult result =
        new ColumnarDataResult(null, chunk(bools, bytes, i16, i32, i64, dbl, str, binary));

    assertEquals(10, result.getRowCount());
    for (int row = 0; row < 10; row++) {
      assertTrue(result.next());
      assertEquals(row, result.getCurrentRow());
      assertEquals(row == 1 || row == 8 ? null : true, result.getObject(0));
      assertEquals(row == 0 ? null : (byte) row, result.getObject(1));
      assertEquals((short) row, result.getObject(2));
      assertEquals(row, result.getObject(3));
      assertEquals((long) row, result.getObject(4));
      assertEquals((double) row, result.getObject(5));
      assertEquals(String.valueOf(row), result.getObject(6));
      assertArrayEquals(new byte[] {(byte) row}, (byte[]) result.getObject(7));
    }
    assertFalse(result.next());
    result.close();
    assertFalse(result.hasNext());
  }

  @Test
  void emptyVectorsHaveNoRows() throws Exception {
    ColumnarDataResult result =
        new ColumnarDataResult(
            null,
            chunk(
                new ColumnVector().setBoolValues(values(List.of())),
                new ColumnVector().setByteValues(values(List.of())),
                new ColumnVector().setI16Values(values(List.of())),
                new ColumnVector().setI32Values(values(List.of())),
                new ColumnVector().setI64Values(values(List.of())),
                new ColumnVector().setDoubleValues(values(List.of())),
                new ColumnVector().setStringValues(values(List.of())),
                new ColumnVector().setBinaryValues(values(List.of()))));
    assertEquals(0, result.getRowCount());
    assertFalse(result.next());
    assertEquals(0, result.getCurrentRow());

    when(session.getConnectionContext()).thenReturn(context);
    DatabricksResultSet jdbcResult =
        new DatabricksResultSet(
            new StatementStatus().setState(StatementState.SUCCEEDED),
            new StatementId("empty-statement"),
            chunk(new ColumnVector().setI32Values(values(List.of()))),
            manifest(
                SqlExecResultFormat.COLUMN_BASED_SET,
                column("v", ColumnInfoTypeName.INT, "INT", 0)),
            StatementType.QUERY,
            session,
            null);
    assertFalse(jdbcResult.isBeforeFirst());
    assertFalse(jdbcResult.isFirst());
    assertFalse(jdbcResult.next());
    assertFalse(jdbcResult.isAfterLast());
    assertFalse(jdbcResult.isLast());
    assertEquals(0, jdbcResult.getRow());
    jdbcResult.close();
  }

  @Test
  void cursorMovesPastLastAndCloseReleasesVectors() throws Exception {
    when(session.getConnectionContext()).thenReturn(context);
    ColumnVector vector = new ColumnVector().setI32Values(values(List.of(7)));
    for (Long totalRows : Arrays.asList(null, 1L)) {
      ResultManifest manifest =
          manifest(
                  SqlExecResultFormat.COLUMN_BASED_SET,
                  column("v", ColumnInfoTypeName.INT, "INT", 0))
              .setTotalRowCount(totalRows);
      DatabricksResultSet result =
          new DatabricksResultSet(
              new StatementStatus().setState(StatementState.SUCCEEDED),
              new StatementId("statement-1"),
              chunk(vector),
              manifest,
              StatementType.QUERY,
              session,
              null);
      assertTrue(result.isBeforeFirst());
      assertFalse(result.isAfterLast());
      assertEquals(0, result.getRow());
      assertTrue(result.next());
      assertTrue(result.isLast());
      assertEquals(1, result.getRow());
      assertEquals(7, result.getObject(1));
      assertFalse(result.next());
      assertFalse(result.isLast());
      assertTrue(result.isAfterLast());
      assertEquals(0, result.getRow());
      assertThrows(DatabricksSQLException.class, () -> result.getObject(1));
      result.close();
    }

    ColumnarDataResult reader = new ColumnarDataResult(null, chunk(vector));
    Field columnsField = ColumnarDataResult.class.getDeclaredField("columns");
    columnsField.setAccessible(true);
    assertEquals(1, ((List<?>) columnsField.get(reader)).size());
    reader.close();
    assertTrue(((List<?>) columnsField.get(reader)).isEmpty());
    assertThrows(DatabricksSQLException.class, () -> reader.getObject(0));
  }

  @Test
  void missingColumnarPayloadHasDriverErrorCode() {
    DatabricksSQLException error =
        assertThrows(
            DatabricksSQLException.class, () -> new ColumnarDataResult(null, new ResultData()));
    assertEquals(DatabricksDriverErrorCode.INVALID_STATE.name(), error.getSQLState());
    assertEquals(DatabricksDriverErrorCode.INVALID_STATE.getCode(), error.getErrorCode());
  }

  @Test
  void logicalStringVectorsMatchJdbcTypesAndValues() throws Exception {
    when(session.getConnectionContext()).thenReturn(context);
    when(context.isComplexDatatypeSupportEnabled()).thenReturn(true);
    ColumnInfo[] columns = {
      column("d", ColumnInfoTypeName.DECIMAL, "DECIMAL(5,2)", 0),
      column("a", ColumnInfoTypeName.ARRAY, "ARRAY<INT>", 1),
      column("m", ColumnInfoTypeName.MAP, "MAP<STRING,INT>", 2),
      column("s", ColumnInfoTypeName.STRUCT, "STRUCT<x:INT>", 3),
      column("u", ColumnInfoTypeName.UNION, "UNIONTYPE<INT,STRING>", 4),
      column("n", ColumnInfoTypeName.NULL, "NULL", 5)
    };
    ResultData data =
        chunk(
            new ColumnVector().setStringValues(values(List.of("123.45"))),
            new ColumnVector().setStringValues(values(List.of("[1,null]"))),
            new ColumnVector().setStringValues(values(List.of("{\"k\":1}"))),
            new ColumnVector().setStringValues(values(List.of("{\"x\":1}"))),
            new ColumnVector().setStringValues(values(List.of("union-value"))),
            new ColumnVector()
                .setStringValues(values(List.of("placeholder")))
                .setNullMask(new byte[] {1}));
    DatabricksResultSet result =
        new DatabricksResultSet(
            new StatementStatus().setState(StatementState.SUCCEEDED),
            new StatementId("statement-1"),
            data,
            manifest(SqlExecResultFormat.COLUMN_BASED_SET, columns),
            StatementType.QUERY,
            session,
            null);
    assertTrue(result.next());
    assertEquals(new BigDecimal("123.45"), result.getObject(1));
    Array array = assertInstanceOf(Array.class, result.getObject(2));
    assertArrayEquals(new Object[] {1, null}, (Object[]) array.getArray());
    Map<?, ?> map = assertInstanceOf(Map.class, result.getObject(3));
    assertEquals(1, map.get("k"));
    Struct struct = assertInstanceOf(Struct.class, result.getObject(4));
    assertArrayEquals(new Object[] {1}, struct.getAttributes());
    assertEquals("union-value", result.getObject(5));
    assertNull(result.getObject(6));
    assertTrue(result.wasNull());
    ResultSetMetaData metadata = result.getMetaData();
    assertEquals(Types.DECIMAL, metadata.getColumnType(1));
    assertEquals(5, metadata.getPrecision(1));
    assertEquals(2, metadata.getScale(1));
    assertEquals(Types.ARRAY, metadata.getColumnType(2));
    assertEquals("ARRAY<INT>", metadata.getColumnTypeName(2));
    assertEquals(Types.VARCHAR, metadata.getColumnType(3));
    assertEquals("MAP<STRING,INT>", metadata.getColumnTypeName(3));
    assertEquals(Types.STRUCT, metadata.getColumnType(4));
    assertEquals("STRUCT<x:INT>", metadata.getColumnTypeName(4));
    assertEquals(Types.OTHER, metadata.getColumnType(5));
    assertEquals(Types.VARCHAR, metadata.getColumnType(6));

    // Pinned Thrift Arrow fixture: wire schema and values are independent of SEA models.
    // The union uses Thrift columnar below because its Arrow converter has no UNION arm.
    DatabricksResultSet thrift =
        new DatabricksResultSet(
            new StatementStatus().setState(StatementState.SUCCEEDED),
            new StatementId("thrift-statement"),
            pinnedThriftArrowFixture(),
            StatementType.QUERY,
            null,
            session);
    assertTrue(thrift.next());
    for (int i : new int[] {1, 2, 3, 4, 6}) {
      Object seaValue = result.getObject(i);
      Object thriftValue = thrift.getObject(i == 6 ? 5 : i);
      if (seaValue instanceof Array) {
        assertArrayEquals(
            (Object[]) ((Array) thriftValue).getArray(), (Object[]) ((Array) seaValue).getArray());
      } else if (seaValue instanceof Map) {
        assertEquals(Map.copyOf((Map<?, ?>) thriftValue), Map.copyOf((Map<?, ?>) seaValue));
      } else if (seaValue instanceof Struct) {
        assertArrayEquals(
            ((Struct) thriftValue).getAttributes(), ((Struct) seaValue).getAttributes());
      } else {
        assertEquals(thriftValue, seaValue);
      }
      ResultSetMetaData thriftMetadata = thrift.getMetaData();
      int thriftIndex = i == 6 ? 5 : i;
      assertEquals(thriftMetadata.getColumnType(thriftIndex), metadata.getColumnType(i));
      assertEquals(thriftMetadata.getColumnTypeName(thriftIndex), metadata.getColumnTypeName(i));
      assertEquals(thriftMetadata.getColumnClassName(thriftIndex), metadata.getColumnClassName(i));
      assertEquals(thriftMetadata.getPrecision(thriftIndex), metadata.getPrecision(i));
      assertEquals(thriftMetadata.getScale(thriftIndex), metadata.getScale(i));
      assertEquals(
          thriftMetadata.getColumnDisplaySize(thriftIndex), metadata.getColumnDisplaySize(i));
    }
    assertTrue(thrift.wasNull());

    DatabricksResultSet thriftUnion =
        new DatabricksResultSet(
            new StatementStatus().setState(StatementState.SUCCEEDED),
            new StatementId("thrift-union"),
            pinnedThriftUnionFixture(),
            StatementType.QUERY,
            null,
            session);
    assertTrue(thriftUnion.next());
    assertEquals(thriftUnion.getObject(1), result.getObject(5));
    assertEquals(thriftUnion.getMetaData().getColumnType(1), metadata.getColumnType(5));
    assertEquals(thriftUnion.getMetaData().getColumnClassName(1), metadata.getColumnClassName(5));
    thrift.close();
    thriftUnion.close();
    result.close();
  }

  private static TColumnDesc thriftColumn(String name, TTypeId type, int position) {
    TPrimitiveTypeEntry primitive = new TPrimitiveTypeEntry().setType(type);
    if (type == TTypeId.DECIMAL_TYPE) {
      primitive.setTypeQualifiers(
          new TTypeQualifiers(
              Map.of(
                  TCLIServiceConstants.PRECISION, TTypeQualifierValue.i32Value(5),
                  TCLIServiceConstants.SCALE, TTypeQualifierValue.i32Value(2))));
    }
    TTypeEntry entry = new TTypeEntry();
    entry.setPrimitiveEntry(primitive);
    return new TColumnDesc()
        .setColumnName(name)
        .setPosition(position)
        .setTypeDesc(new TTypeDesc().setTypes(List.of(entry)));
  }

  private static TFetchResultsResp pinnedThriftArrowFixture() throws Exception {
    String[] names = {"d", "a", "m", "s", "n"};
    String[] types = {"DECIMAL(5,2)", "ARRAY<INT>", "MAP<STRING,INT>", "STRUCT<x:INT>", "NULL"};
    String[] values = {"123.45", "[1,null]", "{\"k\":1}", "{\"x\":1}", null};
    TTypeId[] thriftTypes = {
      TTypeId.DECIMAL_TYPE,
      TTypeId.ARRAY_TYPE,
      TTypeId.MAP_TYPE,
      TTypeId.STRUCT_TYPE,
      TTypeId.NULL_TYPE
    };
    List<TColumnDesc> columns = new ArrayList<>();
    try (BufferAllocator allocator = new RootAllocator();
        ByteArrayOutputStream output = new ByteArrayOutputStream()) {
      VarCharVector[] vectors = new VarCharVector[names.length];
      for (int i = 0; i < names.length; i++) {
        columns.add(thriftColumn(names[i], thriftTypes[i], i));
        FieldType fieldType =
            new FieldType(
                true, new ArrowType.Utf8(), null, Map.of("Spark:DataType:SqlName", types[i]));
        vectors[i] = new VarCharVector(names[i], fieldType, allocator);
        vectors[i].allocateNew();
        if (values[i] != null) {
          vectors[i].setSafe(0, values[i].getBytes(StandardCharsets.UTF_8));
        }
        vectors[i].setValueCount(1);
      }
      try (VectorSchemaRoot root = VectorSchemaRoot.of(vectors);
          ArrowStreamWriter writer = new ArrowStreamWriter(root, null, output)) {
        writer.start();
        int schemaLength = output.size();
        root.setRowCount(1);
        writer.writeBatch();
        writer.end();
        byte[] stream = output.toByteArray();
        TGetResultSetMetadataResp metadata =
            new TGetResultSetMetadataResp()
                .setResultFormat(TSparkRowSetType.ARROW_BASED_SET)
                .setSchema(new TTableSchema().setColumns(columns))
                .setArrowSchema(Arrays.copyOfRange(stream, 0, schemaLength));
        TRowSet rows =
            new TRowSet()
                .setArrowBatches(
                    List.of(
                        new TSparkArrowBatch()
                            .setRowCount(1)
                            .setBatch(Arrays.copyOfRange(stream, schemaLength, stream.length))));
        TFetchResultsResp fixture =
            new TFetchResultsResp().setResultSetMetadata(metadata).setResults(rows);
        fixture.hasMoreRows = false;
        return fixture;
      }
    }
  }

  private static TFetchResultsResp pinnedThriftUnionFixture() {
    TGetResultSetMetadataResp metadata =
        new TGetResultSetMetadataResp()
            .setResultFormat(TSparkRowSetType.COLUMN_BASED_SET)
            .setSchema(
                new TTableSchema().setColumns(List.of(thriftColumn("u", TTypeId.UNION_TYPE, 0))));
    TColumn unionColumn = new TColumn();
    unionColumn.setStringVal(new TStringColumn().setValues(List.of("union-value")));
    TRowSet rows = new TRowSet().setColumns(List.of(unionColumn));
    TFetchResultsResp fixture =
        new TFetchResultsResp().setResultSetMetadata(metadata).setResults(rows);
    fixture.hasMoreRows = false;
    return fixture;
  }

  @Test
  void selectsColumnarPerResponseEvenWhenManifestSaysArrowStream() throws Exception {
    ResultManifest manifest =
        manifest(SqlExecResultFormat.ARROW_STREAM, column("v", ColumnInfoTypeName.INT, "INT", 0));
    ColumnarDataResult result =
        assertInstanceOf(
            ColumnarDataResult.class,
            ExecutionResultFactory.getResultSet(
                chunk(new ColumnVector().setI32Values(values(List.of(7)))),
                manifest,
                new StatementId("statement-1"),
                session,
                null));
    assertTrue(result.next());
    assertEquals(7, result.getObject(0));
  }
}
