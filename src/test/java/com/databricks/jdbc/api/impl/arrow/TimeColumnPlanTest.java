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
import java.util.Collections;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class TimeColumnPlanTest {
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
      ArrowResultChunkIterator iterator = chunk.getChunkIterator();
      IDatabricksSession session = mock(IDatabricksSession.class);
      IDatabricksConnectionContext context = mock(IDatabricksConnectionContext.class);
      when(session.getConnectionContext()).thenReturn(context);
      when(context.isComplexDatatypeSupportEnabled()).thenReturn(complex);
      ComplexDataTypeParser first = null;
      for (int row = 0; row < 2; row++) {
        assertTrue(iterator.nextRow());
        Object value =
            ArrowStreamResult.getObjectWithComplexTypeHandling(
                session, iterator, 0, ColumnInfoTypeName.ARRAY, "ARRAY<TIME(6)>", new ColumnInfo());
        assertEquals("[\"" + clock + "\"]", value.toString());
        ComplexDataTypeParser parser = iterator.getComplexParser(0, true);
        if (first == null) first = parser;
        else assertSame(first, parser);
      }
    }
  }
}
