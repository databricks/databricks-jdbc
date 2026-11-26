package com.databricks.jdbc.api.impl.arrow;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.impl.ComplexDataTypeParser;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.model.core.ColumnInfo;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import com.databricks.sdk.service.sql.BaseChunkInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class TimeColumnPlanTest {
  @Test
  void recoversTimeAndLeavesStringsAcrossBatches() throws Exception {
    try (RootAllocator allocator = new RootAllocator();
        TimeMicroVector times = new TimeMicroVector("clock", allocator);
        VarCharVector strings = new VarCharVector("text", allocator)) {
      times.allocateNew(2);
      times.setSafe(0, 33300123456L);
      times.setNull(1);
      times.setValueCount(2);
      strings.allocateNew();
      strings.setSafe(0, "TIME(6)".getBytes(java.nio.charset.StandardCharsets.UTF_8));
      strings.setSafe(1, "midnight".getBytes(java.nio.charset.StandardCharsets.UTF_8));
      strings.setValueCount(2);
      ArrowResultChunk chunk =
          ArrowResultChunk.builder()
              .withStatementId(new StatementId("time-scalar-plan"))
              .withChunkInfo(new BaseChunkInfo().setChunkIndex(0L).setRowOffset(0L).setRowCount(4L))
              .withChunkStatus(ChunkStatus.PROCESSING_SUCCEEDED)
              .build();
      chunk.recordBatchList =
          java.util.Arrays.asList(
              java.util.Arrays.asList(times, strings), java.util.Arrays.asList(times, strings));
      chunk.arrowMetadata = java.util.Arrays.asList("TIME(6)", "STRING");
      ArrowResultChunkIterator iterator = chunk.getChunkIterator();
      for (int row = 0; row < 4; row++) {
        assertTrue(iterator.nextRow());
        assertEquals(
            row % 2 == 0 ? java.time.LocalTime.parse("09:15:00.123456") : null,
            iterator.getColumnObjectAtCurrentRow(
                0, ColumnInfoTypeName.STRING, "TIME(6)", new ColumnInfo()));
        assertEquals(
            row % 2 == 0 ? "TIME(6)" : "midnight",
            iterator.getColumnObjectAtCurrentRow(
                1, ColumnInfoTypeName.STRING, "STRING", new ColumnInfo()));
      }
      assertFalse(iterator.nextRow());
    }
  }

  @ParameterizedTest
  @CsvSource({
    "SECOND,33300,09:15:00,false", "SECOND,33300,09:15:00,true",
    "MILLISECOND,33300123,09:15:00.123,false", "MILLISECOND,33300123,09:15:00.123,true",
    "MICROSECOND,33300123456,09:15:00.123456,false", "MICROSECOND,33300123456,09:15:00.123456,true",
    "NANOSECOND,33300123456000,09:15:00.123456,false",
        "NANOSECOND,33300123456000,09:15:00.123456,true"
  })
  void reusesColumnPlanAndChildUnits(TimeUnit unit, long count, String clock, boolean complex)
      throws Exception {
    try (RootAllocator allocator = new RootAllocator();
        ListVector vector = ListVector.empty("times", allocator)) {
      ValueVector child =
          vector
              .addOrGetVector(
                  FieldType.nullable(
                      new ArrowType.Time(
                          unit, unit == TimeUnit.SECOND || unit == TimeUnit.MILLISECOND ? 32 : 64)))
              .getVector();
      vector.allocateNew();
      for (int row = 0; row < 2; row++) {
        vector.startNewValue(row);
        if (child instanceof TimeSecVector) ((TimeSecVector) child).setSafe(row, (int) count);
        if (child instanceof TimeMilliVector) ((TimeMilliVector) child).setSafe(row, (int) count);
        if (child instanceof TimeMicroVector) ((TimeMicroVector) child).setSafe(row, count);
        if (child instanceof TimeNanoVector) ((TimeNanoVector) child).setSafe(row, count);
        vector.endValue(row, 1);
      }
      vector.setValueCount(2);
      ArrowResultChunk chunk =
          ArrowResultChunk.builder()
              .withStatementId(new StatementId("time-plan"))
              .withChunkInfo(new BaseChunkInfo().setChunkIndex(0L).setRowOffset(0L).setRowCount(2L))
              .withChunkStatus(ChunkStatus.PROCESSING_SUCCEEDED)
              .build();
      chunk.recordBatchList = Collections.singletonList(Collections.singletonList(vector));
      chunk.arrowMetadata = Collections.singletonList("ARRAY<TIME(6)>");
      ArrowResultChunkIterator iterator = spy(chunk.getChunkIterator());
      List<ComplexDataTypeParser> observed = new ArrayList<>();
      doAnswer(
              invocation -> {
                ComplexDataTypeParser parser = (ComplexDataTypeParser) invocation.callRealMethod();
                observed.add(parser);
                return parser;
              })
          .when(iterator)
          .getComplexParser(anyInt(), anyBoolean());
      IDatabricksSession session = mock(IDatabricksSession.class);
      IDatabricksConnectionContext context = mock(IDatabricksConnectionContext.class);
      when(session.getConnectionContext()).thenReturn(context);
      when(context.isComplexDatatypeSupportEnabled()).thenReturn(complex);
      for (int row = 0; row < 2; row++) {
        assertTrue(iterator.nextRow());
        Object value =
            ArrowStreamResult.getObjectWithComplexTypeHandling(
                session, iterator, 0, ColumnInfoTypeName.ARRAY, "ARRAY<TIME(6)>", new ColumnInfo());
        assertEquals("[\"" + clock + "\"]", value.toString());
      }
      assertEquals(2, observed.size());
      assertSame(observed.get(0), observed.get(1));
      if (complex) {
        when(context.isGeoSpatialSupportEnabled()).thenReturn(true);
        ArrowStreamResult.getObjectWithComplexTypeHandling(
            session, iterator, 0, ColumnInfoTypeName.ARRAY, "ARRAY<TIME(6)>", new ColumnInfo());
        assertNotSame(observed.get(0), observed.get(2));
        ArrowStreamResult.getObjectWithComplexTypeHandling(
            session, iterator, 0, ColumnInfoTypeName.ARRAY, "ARRAY<TIME(6)>", new ColumnInfo());
        assertSame(observed.get(2), observed.get(3));
      }
    }
  }
}
