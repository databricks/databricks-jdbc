package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.TestConstants.TEST_STATEMENT_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import com.databricks.jdbc.model.core.ColumnInfo;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import com.databricks.sdk.service.sql.BaseChunkInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Compares {@link ArrowResultChunkIterator} step by step with the per-row algorithm it replaced,
 * over batch layouts with empty batches and chunk row counts below, equal to and above the rows in
 * the batches.
 */
class ArrowResultChunkIteratorTest {
  private static final ColumnInfo INT_COLUMN = new ColumnInfo();
  private final BufferAllocator allocator = new RootAllocator();
  private final List<ValueVector> vectors = new ArrayList<>();

  @AfterEach
  void tearDown() {
    vectors.forEach(ValueVector::close);
    allocator.close();
  }

  @Test
  void matchesPreviousAlgorithmOnEdgeLayouts() throws Exception {
    int[][] layouts = {
      {},
      {0},
      {0, 0},
      {1},
      {3},
      {3, 2},
      {0, 3},
      {3, 0},
      {0, 2, 0, 2, 0},
      {2, 0, 0, 1},
      {1, 1, 1, 1},
      {4096, 4096, 17},
    };
    for (int[] layout : layouts) {
      long total = Arrays.stream(layout).sum();
      for (long numRows : new long[] {0, 1, total - 1, total, total + 1, total + 5}) {
        if (numRows >= 0) {
          assertSameSteps(layout, numRows, false);
          assertSameSteps(layout, numRows, true);
        }
      }
    }
  }

  @Test
  void matchesPreviousAlgorithmOnRandomLayouts() throws Exception {
    Random random = new Random(1785);
    for (int i = 0; i < 300; i++) {
      int[] layout = new int[random.nextInt(6)];
      for (int b = 0; b < layout.length; b++) {
        layout[b] = random.nextInt(4) == 0 ? 0 : random.nextInt(6);
      }
      long total = Arrays.stream(layout).sum();
      long numRows = random.nextInt((int) total + 4);
      assertSameSteps(layout, numRows, random.nextBoolean());
    }
  }

  @Test
  void stopsAtChunkRowCountInsideBatch() throws Exception {
    ArrowResultChunkIterator iterator = chunk(new int[] {5, 5}, 7).getChunkIterator();
    for (int expected : new int[] {0, 1, 2, 3, 4, 1000, 1001}) {
      assertEquals(true, iterator.nextRow());
      assertEquals(expected, valueAt(iterator));
    }
    assertFalse(iterator.hasNextRow());
    assertFalse(iterator.nextRow());
    assertEquals(1001, valueAt(iterator));
  }

  /**
   * Walks both iterators until they are exhausted, checking hasNextRow(), nextRow() and the current
   * row at every step. With {@code skipHasNext}, nextRow() is called without a preceding
   * hasNextRow(), as {@link ArrowStreamResult#next()} does.
   */
  private void assertSameSteps(int[] layout, long numRows, boolean skipHasNext) throws Exception {
    String ctx = Arrays.toString(layout) + " numRows=" + numRows + " skipHasNext=" + skipHasNext;
    ArrowResultChunk chunk = chunk(layout, numRows);
    ArrowResultChunkIterator actual = chunk.getChunkIterator();
    PreviousIterator expected = new PreviousIterator(chunk);
    for (int step = 0; step < numRows + 3; step++) {
      if (!skipHasNext) {
        assertEquals(expected.hasNextRow(), actual.hasNextRow(), ctx + " step " + step);
      }
      boolean expectedNext;
      try {
        expectedNext = expected.nextRow();
      } catch (IndexOutOfBoundsException e) {
        // The previous algorithm runs past trailing empty batches when the chunk claims more rows.
        assertThrows(IndexOutOfBoundsException.class, actual::nextRow, ctx);
        return;
      }
      assertEquals(expectedNext, actual.nextRow(), ctx + " step " + step);
      if (expectedNext) {
        assertEquals(expected.currentValue(), valueAt(actual), ctx + " step " + step);
      }
      assertEquals(expected.hasNextRow(), actual.hasNextRow(), ctx + " step " + step);
    }
    assertFalse(actual.hasNextRow(), ctx);
    assertFalse(actual.nextRow(), ctx);
  }

  /** One INT column; row r of batch b holds {@code b * 1000 + r}. */
  private ArrowResultChunk chunk(int[] layout, long numRows) throws Exception {
    ArrowResultChunk chunk =
        ArrowResultChunk.builder()
            .withStatementId(TEST_STATEMENT_ID)
            .withChunkInfo(
                new BaseChunkInfo().setChunkIndex(0L).setRowOffset(0L).setRowCount(numRows))
            .withChunkStatus(ChunkStatus.PROCESSING_SUCCEEDED)
            .build();
    List<List<ValueVector>> batches = new ArrayList<>();
    for (int b = 0; b < layout.length; b++) {
      IntVector vector = new IntVector("c", allocator);
      vector.allocateNew(Math.max(1, layout[b]));
      for (int r = 0; r < layout[b]; r++) {
        vector.set(r, b * 1000 + r);
      }
      vector.setValueCount(layout[b]);
      vectors.add(vector);
      batches.add(List.of(vector));
    }
    chunk.recordBatchList = batches;
    return chunk;
  }

  private static Object valueAt(ArrowResultChunkIterator iterator) throws Exception {
    return iterator.getColumnObjectAtCurrentRow(0, ColumnInfoTypeName.INT, "INT", INT_COLUMN);
  }

  /** The iterator's per-row algorithm before it tracked the last readable row of each batch. */
  private static final class PreviousIterator {
    private final AbstractArrowResultChunk resultChunk;
    private final int recordBatchesInChunk;
    private int recordBatchCursorInChunk = -1;
    private int rowsInRecordBatch = -1;
    private int rowCursorInRecordBatch = -1;
    private int rowsReadByIterator = 0;

    PreviousIterator(AbstractArrowResultChunk resultChunk) {
      this.resultChunk = resultChunk;
      this.recordBatchesInChunk = resultChunk.getRecordBatchCountInChunk();
    }

    boolean nextRow() {
      if (!hasNextRow()) {
        return false;
      }
      if (rowsInRecordBatch < 0 || ++rowCursorInRecordBatch == rowsInRecordBatch) {
        rowCursorInRecordBatch = 0;
        recordBatchCursorInChunk++;
        while (recordBatchCursorInChunk < recordBatchesInChunk
            && resultChunk.recordBatchList.get(recordBatchCursorInChunk).get(0).getValueCount()
                == 0) {
          recordBatchCursorInChunk++;
        }
        rowsInRecordBatch =
            resultChunk.recordBatchList.get(recordBatchCursorInChunk).get(0).getValueCount();
      }
      rowsReadByIterator++;
      return true;
    }

    boolean hasNextRow() {
      if (rowsReadByIterator >= resultChunk.numRows) return false;
      return (rowCursorInRecordBatch < rowsInRecordBatch - 1)
          || (recordBatchCursorInChunk < recordBatchesInChunk - 1);
    }

    Object currentValue() {
      if (recordBatchCursorInChunk < 0) {
        fail("no current row");
      }
      return ((IntVector) resultChunk.recordBatchList.get(recordBatchCursorInChunk).get(0))
          .getObject(rowCursorInRecordBatch);
    }
  }
}
