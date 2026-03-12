package com.databricks.jdbc.api.impl.thrift;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.client.thrift.generated.*;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.sql.SQLException;
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for StreamingColumnarResult. Tests functional parity with LazyThriftResult from the
 * consumer's perspective, along with streaming-specific behaviors (such as background prefetch).
 */
@ExtendWith(MockitoExtension.class)
public class StreamingColumnarResultTest {

  @Mock private IDatabricksSession session;
  @Mock private IDatabricksStatementInternal statement;
  @Mock private IDatabricksClient databricksClient;
  @Mock private IDatabricksConnectionContext connectionContext;

  @BeforeEach
  void setUp() throws SQLException {
    lenient().when(session.getDatabricksClient()).thenReturn(databricksClient);
    lenient().when(session.getConnectionContext()).thenReturn(connectionContext);
    lenient().when(connectionContext.getThriftMaxBatchesInMemory()).thenReturn(3);
    lenient().when(statement.getMaxRows()).thenReturn(0); // No limit by default
  }

  @Test
  void testBasicIteration() throws SQLException {
    TFetchResultsResp response =
        createResponseWithStringData(
            Arrays.asList("row1_col1", "row1_col2"),
            Arrays.asList("row2_col1", "row2_col2"),
            false);

    StreamingColumnarResult result = new StreamingColumnarResult(response, statement, session);

    try {
      // Initial state
      assertEquals(-1, result.getCurrentRow());
      assertEquals(2, result.getRowCount());
      assertTrue(result.hasNext());

      // First row
      assertTrue(result.next());
      assertEquals(0, result.getCurrentRow());
      assertEquals("row1_col1", result.getObject(0));
      assertEquals("row1_col2", result.getObject(1));

      // Second row
      assertTrue(result.next());
      assertEquals(1, result.getCurrentRow());
      assertEquals("row2_col1", result.getObject(0));
      assertEquals("row2_col2", result.getObject(1));

      // End
      assertFalse(result.hasNext());
      assertFalse(result.next());
    } finally {
      result.close();
    }
  }

  @Test
  void testMultiBatchFetching() throws SQLException, InterruptedException {
    TFetchResultsResp firstBatch =
        createResponseWithStringData(
            Arrays.asList("row1_col1", "row1_col2"), Arrays.asList("row2_col1", "row2_col2"), true);

    TFetchResultsResp secondBatch =
        createResponseWithStringData(
            Arrays.asList("row3_col1", "row3_col2"),
            Arrays.asList("row4_col1", "row4_col2"),
            false);

    when(databricksClient.getMoreResults(statement)).thenReturn(secondBatch);

    StreamingColumnarResult result = new StreamingColumnarResult(firstBatch, statement, session);

    try {
      // Give prefetch thread time to start
      Thread.sleep(100);

      // Consume first batch
      assertTrue(result.next());
      assertEquals("row1_col1", result.getObject(0));
      assertTrue(result.next());
      assertEquals("row2_col1", result.getObject(0));

      // Move to second batch
      assertTrue(result.next());
      assertEquals(2, result.getCurrentRow());
      assertEquals("row3_col1", result.getObject(0));

      assertTrue(result.next());
      assertEquals("row4_col1", result.getObject(0));

      // End
      assertFalse(result.next());

      verify(databricksClient, atLeastOnce()).getMoreResults(statement);
    } finally {
      result.close();
    }
  }

  @Test
  void testMaxRowsLimit() throws SQLException {
    when(statement.getMaxRows()).thenReturn(2);

    // Use hasMoreRows=false so the prefetch thread doesn't try to fetch
    TFetchResultsResp response =
        createResponseWithStringData(
            Arrays.asList("row1_col1", "row1_col2"),
            Arrays.asList("row2_col1", "row2_col2"),
            Arrays.asList("row3_col1", "row3_col2"),
            false);

    StreamingColumnarResult result = new StreamingColumnarResult(response, statement, session);

    try {
      // Should only get 2 rows due to maxRows limit
      assertTrue(result.next());
      assertEquals("row1_col1", result.getObject(0));

      assertTrue(result.next());
      assertEquals("row2_col1", result.getObject(0));

      // Should stop due to maxRows limit even though there's more data
      assertFalse(result.hasNext());
      assertFalse(result.next());
    } finally {
      result.close();
    }
  }

  @Test
  void testAccessAfterClose() throws SQLException {
    TFetchResultsResp response =
        createResponseWithStringData(Arrays.asList("row1_col1", "row1_col2"), false);

    StreamingColumnarResult result = new StreamingColumnarResult(response, statement, session);
    result.next();
    result.close();

    assertThrows(DatabricksSQLException.class, () -> result.getObject(0));
    assertFalse(result.hasNext());
    assertFalse(result.next());
  }

  @Test
  void testAccessBeforeFirstRow() throws SQLException {
    TFetchResultsResp response =
        createResponseWithStringData(Arrays.asList("row1_col1", "row1_col2"), false);

    StreamingColumnarResult result = new StreamingColumnarResult(response, statement, session);

    try {
      DatabricksSQLException thrown =
          assertThrows(DatabricksSQLException.class, () -> result.getObject(0));
      assertTrue(thrown.getMessage().contains("before first row"));
    } finally {
      result.close();
    }
  }

  @Test
  void testInvalidColumnIndex() throws SQLException {
    TFetchResultsResp response = createResponseWithStringData(Arrays.asList("col1", "col2"), false);

    StreamingColumnarResult result = new StreamingColumnarResult(response, statement, session);

    try {
      result.next();

      // Valid indices
      assertDoesNotThrow(() -> result.getObject(0));
      assertDoesNotThrow(() -> result.getObject(1));

      // Invalid indices
      assertThrows(DatabricksSQLException.class, () -> result.getObject(2));
      assertThrows(DatabricksSQLException.class, () -> result.getObject(-1));
    } finally {
      result.close();
    }
  }

  @Test
  void testNullHandling() throws SQLException {
    TFetchResultsResp response = createResponseWithNulls();

    StreamingColumnarResult result = new StreamingColumnarResult(response, statement, session);

    try {
      assertTrue(result.next());
      assertEquals("value1", result.getObject(0));
      assertNull(result.getObject(1));
    } finally {
      result.close();
    }
  }

  @Test
  void testChunkCount() throws SQLException {
    TFetchResultsResp response =
        createResponseWithStringData(Arrays.asList("row1_col1", "row1_col2"), false);

    StreamingColumnarResult result = new StreamingColumnarResult(response, statement, session);

    try {
      // StreamingColumnarResult doesn't use chunks like Arrow results
      assertEquals(0, result.getChunkCount());
    } finally {
      result.close();
    }
  }

  @Test
  void testSingleRowResult() throws SQLException {
    // Test single row iteration (empty results have different behavior in streaming due to init)
    TFetchResultsResp response =
        createResponseWithStringData(Arrays.asList("only_row_col1", "only_row_col2"), false);

    StreamingColumnarResult result = new StreamingColumnarResult(response, statement, session);

    try {
      assertEquals(1, result.getRowCount());
      assertTrue(result.hasNext());
      assertTrue(result.next());
      assertEquals(0, result.getCurrentRow());
      assertEquals("only_row_col1", result.getObject(0));

      assertFalse(result.hasNext());
      assertFalse(result.next());
    } finally {
      result.close();
    }
  }

  @Test
  void testErrorDuringFetch() throws SQLException, InterruptedException {
    TFetchResultsResp firstBatch =
        createResponseWithStringData(Arrays.asList("row1_col1", "row1_col2"), true);

    DatabricksSQLException expectedException =
        new DatabricksSQLException("Network error", DatabricksDriverErrorCode.CONNECTION_ERROR);
    when(databricksClient.getMoreResults(statement)).thenThrow(expectedException);

    // The prefetch thread starts during construction and may fail at any point.
    // The error can surface during construction (if prefetch thread runs fast) or during iteration.
    DatabricksSQLException caughtException = null;
    StreamingColumnarResult result = null;

    try {
      result = new StreamingColumnarResult(firstBatch, statement, session);
      // Consume rows until we hit the error
      while (result.next()) {
        // Keep iterating - error will be thrown when we need a batch that failed to prefetch
      }
    } catch (DatabricksSQLException e) {
      caughtException = e;
    } finally {
      if (result != null) {
        result.close();
      }
    }

    // Verify we caught the expected error
    assertNotNull(caughtException, "Expected DatabricksSQLException to be thrown");
    assertTrue(
        caughtException.getMessage().contains("Prefetch failed")
            || caughtException.getMessage().contains("Network error"),
        "Exception should contain error details: " + caughtException.getMessage());
  }

  @Test
  void testMaxRowsLimitAcrossBatches() throws SQLException, InterruptedException {
    // MaxRows limit of 3, spanning across 2 batches (2 rows each)
    when(statement.getMaxRows()).thenReturn(3);

    TFetchResultsResp firstBatch =
        createResponseWithStringData(
            Arrays.asList("row1_col1", "row1_col2"),
            Arrays.asList("row2_col1", "row2_col2"),
            true); // hasMoreRows = true

    TFetchResultsResp secondBatch =
        createResponseWithStringData(
            Arrays.asList("row3_col1", "row3_col2"),
            Arrays.asList("row4_col1", "row4_col2"),
            false);

    when(databricksClient.getMoreResults(statement)).thenReturn(secondBatch);

    StreamingColumnarResult result = new StreamingColumnarResult(firstBatch, statement, session);

    try {
      // Give prefetch thread time to start
      Thread.sleep(100);

      // Consume first batch (rows 0 and 1)
      assertTrue(result.next());
      assertEquals("row1_col1", result.getObject(0));
      assertTrue(result.next());
      assertEquals("row2_col1", result.getObject(0));

      // Get one row from second batch (row 2)
      assertTrue(result.next());
      assertEquals("row3_col1", result.getObject(0));

      // Should stop at maxRows=3
      assertFalse(result.hasNext());
      assertFalse(result.next());
    } finally {
      result.close();
    }
  }

  @Test
  void testBatchesInMemoryTracking() throws SQLException {
    TFetchResultsResp response =
        createResponseWithStringData(
            Arrays.asList("row1_col1", "row1_col2"),
            Arrays.asList("row2_col1", "row2_col2"),
            false);

    StreamingColumnarResult result = new StreamingColumnarResult(response, statement, session);

    try {
      // Should have at least initial batch
      assertTrue(result.getBatchesInMemory() > 0);
    } finally {
      result.close();
    }
    // Note: getBatchesInMemory() may return a small number after close due to cleanup timing
    // The important thing is that close() was called without exception
  }

  @Test
  void testGetTotalRowsFetched() throws SQLException, InterruptedException {
    TFetchResultsResp firstBatch =
        createResponseWithStringData(
            Arrays.asList("row1_col1", "row1_col2"), Arrays.asList("row2_col1", "row2_col2"), true);

    TFetchResultsResp secondBatch =
        createResponseWithStringData(
            Arrays.asList("row3_col1", "row3_col2"),
            Arrays.asList("row4_col1", "row4_col2"),
            false);

    when(databricksClient.getMoreResults(statement)).thenReturn(secondBatch);

    StreamingColumnarResult result = new StreamingColumnarResult(firstBatch, statement, session);

    try {
      // Initial batch has at least 2 rows (prefetch may have already fetched more)
      assertTrue(result.getTotalRowsFetched() >= 2);

      // Give prefetch thread time to fetch second batch
      Thread.sleep(100);

      // After prefetch completes, should have 4 total rows fetched
      assertEquals(4, result.getTotalRowsFetched());
    } finally {
      result.close();
    }
  }

  @Test
  void testIsCompletelyFetched() throws SQLException, InterruptedException {
    TFetchResultsResp firstBatch =
        createResponseWithStringData(
            Arrays.asList("row1_col1", "row1_col2"), true); // hasMoreRows = true

    TFetchResultsResp secondBatch =
        createResponseWithStringData(
            Arrays.asList("row2_col1", "row2_col2"), false); // hasMoreRows = false

    when(databricksClient.getMoreResults(statement)).thenReturn(secondBatch);

    StreamingColumnarResult result = new StreamingColumnarResult(firstBatch, statement, session);

    try {
      // Give prefetch thread time to fetch second batch
      Thread.sleep(100);

      // After fetching final batch (hasMoreRows=false), should be completely fetched
      assertTrue(result.isCompletelyFetched());
    } finally {
      result.close();
    }
  }

  @Test
  void testInitializationWithEmptyInitialBatch() throws SQLException, InterruptedException {
    // Initial batch is EMPTY but hasMoreRows=true
    TFetchResultsResp emptyInitial = createEmptyResponse(true);

    // Second batch has actual data
    TFetchResultsResp dataBatch =
        createResponseWithStringData(
            Arrays.asList("row1_col1", "row1_col2"),
            Arrays.asList("row2_col1", "row2_col2"),
            false);

    when(databricksClient.getMoreResults(statement)).thenReturn(dataBatch);

    StreamingColumnarResult result = new StreamingColumnarResult(emptyInitial, statement, session);

    try {
      // Give prefetch time to fetch the data batch
      Thread.sleep(100);

      // Should have skipped empty batch and positioned on data batch
      assertTrue(result.hasNext());
      assertTrue(result.next());
      assertEquals("row1_col1", result.getObject(0));
      assertEquals("row1_col2", result.getObject(1));

      assertTrue(result.next());
      assertEquals("row2_col1", result.getObject(0));
      assertEquals("row2_col2", result.getObject(1));

      assertFalse(result.next());
    } finally {
      result.close();
    }
  }

  @Test
  void testGetRowCount() throws SQLException {
    TFetchResultsResp response =
        createResponseWithStringData(
            Arrays.asList("row1_col1", "row1_col2"),
            Arrays.asList("row2_col1", "row2_col2"),
            Arrays.asList("row3_col1", "row3_col2"),
            false);

    StreamingColumnarResult result = new StreamingColumnarResult(response, statement, session);

    try {
      assertTrue(result.next());
      assertEquals(3, result.getRowCount());
    } finally {
      result.close();
    }
  }

  @Test
  void testDoubleClose() throws SQLException {
    TFetchResultsResp response =
        createResponseWithStringData(Arrays.asList("row1_col1", "row1_col2"), false);

    StreamingColumnarResult result = new StreamingColumnarResult(response, statement, session);

    // First close
    result.close();
    // Second close should not throw
    assertDoesNotThrow(result::close);
  }

  @Test
  void testEmptyResultNoMoreRows() throws SQLException {
    // Single row result to verify end-of-stream detection works
    TFetchResultsResp singleRowResponse =
        createResponseWithStringData(Arrays.asList("only_value"), false);

    StreamingColumnarResult result =
        new StreamingColumnarResult(singleRowResponse, statement, session);

    try {
      // Should have exactly one row
      assertTrue(result.hasNext());
      assertTrue(result.next());
      assertEquals("only_value", result.getObject(0));

      // No more rows
      assertFalse(result.hasNext());
      assertFalse(result.next());
    } finally {
      result.close();
    }
  }

  @Test
  void testHasNextAfterExhausted() throws SQLException {
    TFetchResultsResp response = createResponseWithStringData(Arrays.asList("value"), false);

    StreamingColumnarResult result = new StreamingColumnarResult(response, statement, session);

    try {
      // Consume all rows
      assertTrue(result.next());
      assertFalse(result.next());

      // hasNext should consistently return false after exhausted
      assertFalse(result.hasNext());
      assertFalse(result.hasNext());
      assertFalse(result.next());
    } finally {
      result.close();
    }
  }

  @Test
  void testMultipleConsecutiveEmptyBatches() throws SQLException, InterruptedException {
    // First batch empty
    TFetchResultsResp emptyBatch1 = createEmptyResponse(true);
    // Second batch also empty
    TFetchResultsResp emptyBatch2 = createEmptyResponse(true);
    // Third batch has data
    TFetchResultsResp dataBatch =
        createResponseWithStringData(
            Arrays.asList("row1_col1", "row1_col2"),
            Arrays.asList("row2_col1", "row2_col2"),
            false);

    when(databricksClient.getMoreResults(statement)).thenReturn(emptyBatch2).thenReturn(dataBatch);

    StreamingColumnarResult result = new StreamingColumnarResult(emptyBatch1, statement, session);

    try {
      // Give prefetch time
      Thread.sleep(100);

      // Should skip both empty batches and find data
      assertTrue(result.hasNext());
      assertTrue(result.next());
      assertEquals("row1_col1", result.getObject(0));

      assertTrue(result.next());
      assertEquals("row2_col1", result.getObject(0));

      assertFalse(result.next());
    } finally {
      result.close();
    }
  }

  @Test
  void testExhaustAllRowsInSingleBatch() throws SQLException {
    // Single batch with 3 rows, no more data
    TFetchResultsResp response =
        createResponseWithStringData(
            Arrays.asList("row1"), Arrays.asList("row2"), Arrays.asList("row3"), false);

    StreamingColumnarResult result = new StreamingColumnarResult(response, statement, session);

    try {
      // Consume all 3 rows
      assertTrue(result.next());
      assertEquals("row1", result.getObject(0));
      assertTrue(result.next());
      assertEquals("row2", result.getObject(0));
      assertTrue(result.next());
      assertEquals("row3", result.getObject(0));

      // No more rows - should return false
      assertFalse(result.next());
      // Calling again should still return false
      assertFalse(result.next());

      // hasNext should also be false
      assertFalse(result.hasNext());
    } finally {
      result.close();
    }
  }

  @Test
  void testExhaustAllRowsAcrossMultipleBatches() throws SQLException, InterruptedException {
    // First batch with 2 rows
    TFetchResultsResp firstBatch =
        createResponseWithStringData(
            Arrays.asList("batch1_row1"), Arrays.asList("batch1_row2"), true);

    // Second batch with 1 row - final batch
    TFetchResultsResp secondBatch =
        createResponseWithStringData(Arrays.asList("batch2_row1"), false);

    when(databricksClient.getMoreResults(statement)).thenReturn(secondBatch);

    StreamingColumnarResult result = new StreamingColumnarResult(firstBatch, statement, session);

    try {
      // Give prefetch time
      Thread.sleep(100);

      // Consume batch 1
      assertTrue(result.next());
      assertEquals("batch1_row1", result.getObject(0));
      assertTrue(result.next());
      assertEquals("batch1_row2", result.getObject(0));

      // Consume batch 2
      assertTrue(result.next());
      assertEquals("batch2_row1", result.getObject(0));

      // No more rows - triggers end of stream detection
      assertFalse(result.next());
      assertFalse(result.hasNext());

      // Multiple calls should consistently return false
      assertFalse(result.next());
      assertFalse(result.next());
    } finally {
      result.close();
    }
  }

  @Test
  void testBatchTransitionAtExactBoundary() throws SQLException, InterruptedException {
    // First batch with exactly 1 row
    TFetchResultsResp firstBatch =
        createResponseWithStringData(Arrays.asList("only_row_in_batch1"), true);

    // Second batch with 1 row
    TFetchResultsResp secondBatch =
        createResponseWithStringData(Arrays.asList("only_row_in_batch2"), false);

    when(databricksClient.getMoreResults(statement)).thenReturn(secondBatch);

    StreamingColumnarResult result = new StreamingColumnarResult(firstBatch, statement, session);

    try {
      // Give prefetch time
      Thread.sleep(100);

      // Row 1 - from batch 1
      assertTrue(result.next());
      assertEquals("only_row_in_batch1", result.getObject(0));
      assertEquals(0, result.getCurrentRow());

      // Row 2 - requires batch transition
      assertTrue(result.next());
      assertEquals("only_row_in_batch2", result.getObject(0));
      assertEquals(1, result.getCurrentRow());

      // End - no more batches
      assertFalse(result.next());
    } finally {
      result.close();
    }
  }

  @Test
  void testNextAfterEndReturnsConsistentFalse() throws SQLException {
    TFetchResultsResp response = createResponseWithStringData(Arrays.asList("single_row"), false);

    StreamingColumnarResult result = new StreamingColumnarResult(response, statement, session);

    try {
      // Consume the only row
      assertTrue(result.next());

      // Multiple calls to next() after exhaustion should all return false
      assertFalse(result.next());
      assertFalse(result.next());
      assertFalse(result.next());

      // hasReachedEnd should be set
      assertFalse(result.hasNext());
    } finally {
      result.close();
    }
  }

  // ==================== Helper Methods ====================

  private TFetchResultsResp createEmptyResponse(boolean hasMoreRows) {
    TFetchResultsResp response = new TFetchResultsResp();
    response.hasMoreRows = hasMoreRows;
    TRowSet emptyRowSet = new TRowSet();
    emptyRowSet.setColumns(Collections.emptyList());
    response.setResults(emptyRowSet);
    return response;
  }

  private TFetchResultsResp createResponseWithStringData(List<String> row, boolean hasMoreRows) {
    return createResponseWithStringRows(Arrays.asList(row), hasMoreRows);
  }

  private TFetchResultsResp createResponseWithStringData(
      List<String> row1, List<String> row2, boolean hasMoreRows) {
    return createResponseWithStringRows(Arrays.asList(row1, row2), hasMoreRows);
  }

  private TFetchResultsResp createResponseWithStringData(
      List<String> row1, List<String> row2, List<String> row3, boolean hasMoreRows) {
    return createResponseWithStringRows(Arrays.asList(row1, row2, row3), hasMoreRows);
  }

  private TFetchResultsResp createResponseWithStringRows(
      List<List<String>> rows, boolean hasMoreRows) {
    TFetchResultsResp response = new TFetchResultsResp();
    response.hasMoreRows = hasMoreRows;

    if (rows.isEmpty()) {
      return createEmptyResponse(hasMoreRows);
    }

    TRowSet rowSet = new TRowSet();
    int numColumns = rows.get(0).size();
    List<TColumn> columns = new ArrayList<>(numColumns);

    for (int col = 0; col < numColumns; col++) {
      TColumn column = new TColumn();
      TStringColumn stringCol = new TStringColumn();
      List<String> colValues = new ArrayList<>();

      for (List<String> row : rows) {
        colValues.add(col < row.size() ? row.get(col) : null);
      }

      stringCol.setValues(colValues);
      column.setStringVal(stringCol);
      columns.add(column);
    }

    rowSet.setColumns(columns);
    response.setResults(rowSet);
    return response;
  }

  private TFetchResultsResp createResponseWithNulls() {
    TFetchResultsResp response = new TFetchResultsResp();
    response.hasMoreRows = false;

    TRowSet rowSet = new TRowSet();
    List<TColumn> columns = new ArrayList<>();

    // First column - no nulls
    TColumn col1 = new TColumn();
    TStringColumn stringCol1 = new TStringColumn();
    stringCol1.setValues(Arrays.asList("value1"));
    col1.setStringVal(stringCol1);
    columns.add(col1);

    // Second column - with null
    TColumn col2 = new TColumn();
    TStringColumn stringCol2 = new TStringColumn();
    stringCol2.setValues(Arrays.asList("placeholder"));
    stringCol2.setNulls(new byte[] {0x01});
    col2.setStringVal(stringCol2);
    columns.add(col2);

    rowSet.setColumns(columns);
    response.setResults(rowSet);
    return response;
  }
}
