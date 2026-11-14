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
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.ResultSetMetaData;
import java.sql.Struct;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
