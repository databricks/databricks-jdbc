package com.databricks.jdbc.api.impl.arrow;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.impl.DatabricksResultSet;
import com.databricks.jdbc.api.impl.DatabricksResultSetMetaData;
import com.databricks.jdbc.api.impl.IExecutionResult;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.model.core.ColumnInfo;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.core.ResultSchema;
import com.databricks.jdbc.model.core.StatementStatus;
import com.databricks.sdk.service.sql.BaseChunkInfo;
import com.databricks.sdk.service.sql.StatementState;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Struct;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class NestedDecimalResultSetTest {
  private static final BigDecimal WIDE = new BigDecimal("12345678901234567890.0123456789");
  private static final FieldType DECIMAL = FieldType.nullable(new ArrowType.Decimal(38, 10, 128));

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void realArrowArrayKeepsDecimals(boolean complex) throws Exception {
    try (RootAllocator allocator = new RootAllocator();
        ListVector vector = ListVector.empty("arr", allocator)) {
      DecimalVector decimals = (DecimalVector) vector.addOrGetVector(DECIMAL).getVector();
      vector.allocateNew();
      vector.startNewValue(0);
      decimals.setSafe(0, WIDE);
      decimals.setNull(1);
      vector.endValue(0, 2);
      vector.setValueCount(1);
      try (ResultSet result =
          resultSet(vector, "ARRAY<DECIMAL(38,10)>", ColumnInfoTypeName.ARRAY, complex)) {
        assertEquals("[" + WIDE.toPlainString() + ",null]", result.getString(1));
        if (complex) {
          assertArrayEquals(new Object[] {WIDE, null}, (Object[]) result.getArray(1).getArray());
          assertArrayEquals(
              new Object[] {WIDE, null},
              (Object[]) ((java.sql.Array) result.getObject(1)).getArray());
        } else {
          assertEquals(result.getString(1), result.getObject(1));
          assertThrows(java.sql.SQLException.class, () -> result.getArray(1));
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void realArrowStructKeepsDecimals(boolean complex) throws Exception {
    try (RootAllocator allocator = new RootAllocator();
        StructVector vector = StructVector.empty("st", allocator)) {
      DecimalVector decimals = vector.addOrGet("d", DECIMAL, DecimalVector.class);
      vector.allocateNew();
      vector.setIndexDefined(0);
      decimals.setSafe(0, WIDE);
      vector.setValueCount(1);
      try (ResultSet result =
          resultSet(vector, "STRUCT<d:DECIMAL(38,10)>", ColumnInfoTypeName.STRUCT, complex)) {
        assertEquals("{\"d\":" + WIDE.toPlainString() + "}", result.getString(1));
        if (complex) {
          assertArrayEquals(new Object[] {WIDE}, ((Struct) result.getObject(1)).getAttributes());
        } else {
          assertEquals(result.getString(1), result.getObject(1));
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void realArrowMapKeepsDecimals(boolean complex) throws Exception {
    Field entries =
        new Field(
            "entries",
            FieldType.notNullable(new ArrowType.Struct()),
            Arrays.asList(
                new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null),
                new Field("value", DECIMAL, null)));
    Field field =
        new Field(
            "mp", FieldType.nullable(new ArrowType.Map(false)), Collections.singletonList(entries));
    try (RootAllocator allocator = new RootAllocator();
        MapVector vector = (MapVector) field.createVector(allocator)) {
      vector.allocateNew();
      StructVector data = (StructVector) vector.getDataVector();
      vector.startNewValue(0);
      data.setIndexDefined(0);
      data.getChild("key", VarCharVector.class).setSafe(0, new byte[] {'d'});
      data.getChild("value", DecimalVector.class).setSafe(0, WIDE);
      vector.endValue(0, 1);
      vector.setValueCount(1);
      try (ResultSet result =
          resultSet(vector, "MAP<STRING,DECIMAL(38,10)>", ColumnInfoTypeName.MAP, complex)) {
        assertEquals("{\"d\":" + WIDE.toPlainString() + "}", result.getString(1));
        if (complex) {
          assertEquals(WIDE, ((Map<?, ?>) result.getObject(1)).get("d"));
        } else {
          assertEquals(result.getString(1), result.getObject(1));
        }
      }
    }
  }

  private ResultSet resultSet(
      ValueVector vector, String metadata, ColumnInfoTypeName type, boolean complex)
      throws Exception {
    ArrowResultChunk chunk =
        ArrowResultChunk.builder()
            .withStatementId(new StatementId("nested-decimal"))
            .withChunkInfo(new BaseChunkInfo().setChunkIndex(0L).setRowOffset(0L).setRowCount(1L))
            .withChunkStatus(ChunkStatus.PROCESSING_SUCCEEDED)
            .build();
    chunk.recordBatchList = Collections.singletonList(Collections.singletonList(vector));
    chunk.arrowMetadata = Collections.singletonList(metadata);
    ArrowResultChunkIterator iterator = chunk.getChunkIterator();
    assertTrue(iterator.nextRow());
    IDatabricksSession session = mock(IDatabricksSession.class);
    IDatabricksConnectionContext context = mock(IDatabricksConnectionContext.class);
    when(session.getConnectionContext()).thenReturn(context);
    when(context.isComplexDatatypeSupportEnabled()).thenReturn(complex);
    ColumnInfo info =
        new ColumnInfo()
            .setName("decimal_column")
            .setPosition(0L)
            .setTypeName(type)
            .setTypeText(metadata);
    IExecutionResult execution = mock(IExecutionResult.class);
    when(execution.getObject(0))
        .thenAnswer(
            invocation ->
                ArrowStreamResult.getObjectWithComplexTypeHandling(
                    session, iterator, 0, type, metadata, info));
    DatabricksResultSetMetaData resultMetadata =
        new DatabricksResultSetMetaData(
            new StatementId("nested-decimal"),
            new ResultManifest()
                .setSchema(
                    new ResultSchema()
                        .setColumns(Collections.singletonList(info))
                        .setColumnCount(1L)),
            false,
            context);
    return new DatabricksResultSet(
        new StatementStatus().setState(StatementState.SUCCEEDED),
        new StatementId("nested-decimal"),
        StatementType.QUERY,
        null,
        execution,
        resultMetadata,
        complex);
  }
}
