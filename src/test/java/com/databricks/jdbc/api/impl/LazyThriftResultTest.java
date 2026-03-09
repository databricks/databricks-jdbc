package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.lenient;

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

@ExtendWith(MockitoExtension.class)
public class LazyThriftResultTest {

  @Mock private IDatabricksSession session;
  @Mock private IDatabricksStatementInternal statement;
  @Mock private IDatabricksClient databricksClient;

  @BeforeEach
  void setUp() throws SQLException {
    // Lenient stubbing to avoid unnecessary strictness
    lenient().when(session.getDatabricksClient()).thenReturn(databricksClient);
    lenient().when(statement.getMaxRows()).thenReturn(0); // No limit by default
  }

  @Test
  void testConstructorWithEmptyResult() throws SQLException {
    TFetchResultsResp emptyResponse = createEmptyResponse(false);

    LazyThriftResult result = new LazyThriftResult(emptyResponse, statement, session);

    assertEquals(0, result.getRowCount());
    assertEquals(0, result.getTotalRowsFetched());
    assertFalse(result.hasNext());
    assertFalse(result.next());
    assertTrue(result.isCompletelyFetched());
    assertEquals(-1, result.getCurrentRow());
  }

  @Test
  void testBasicIteration() throws SQLException {
    TFetchResultsResp response =
        createResponseWithStringData(
            Arrays.asList("row1_col1", "row1_col2"),
            Arrays.asList("row2_col1", "row2_col2"),
            false); // hasMoreRows = false

    LazyThriftResult result = new LazyThriftResult(response, statement, session);

    // Test initial state
    assertEquals(-1, result.getCurrentRow());
    assertEquals(2, result.getRowCount());
    assertEquals(2, result.getTotalRowsFetched());
    assertTrue(result.hasNext());

    // Test first row
    assertTrue(result.next());
    assertEquals(0, result.getCurrentRow());
    assertEquals("row1_col1", result.getObject(0));
    assertEquals("row1_col2", result.getObject(1));

    // Test second row
    assertTrue(result.hasNext());
    assertTrue(result.next());
    assertEquals(1, result.getCurrentRow());
    assertEquals("row2_col1", result.getObject(0));
    assertEquals("row2_col2", result.getObject(1));

    // Test end of data
    assertFalse(result.hasNext());
    assertFalse(result.next());
    assertTrue(result.isCompletelyFetched());
  }

  @Test
  void testLazyFetching() throws SQLException {
    // Setup first batch
    TFetchResultsResp firstBatch =
        createResponseWithStringData(
            Arrays.asList("row1_col1", "row1_col2"),
            Arrays.asList("row2_col1", "row2_col2"),
            true); // hasMoreRows = true

    // Setup second batch
    TFetchResultsResp secondBatch =
        createResponseWithStringData(
            Arrays.asList("row3_col1", "row3_col2"),
            Arrays.asList("row4_col1", "row4_col2"),
            false); // hasMoreRows = false

    when(databricksClient.getMoreResults(statement)).thenReturn(secondBatch);

    LazyThriftResult result = new LazyThriftResult(firstBatch, statement, session);

    // Consume first batch
    assertTrue(result.next()); // row 1
    assertEquals("row1_col1", result.getObject(0));
    assertTrue(result.next()); // row 2
    assertEquals("row2_col1", result.getObject(0));
    assertEquals(2, result.getTotalRowsFetched()); // Only first batch loaded so far

    // This should trigger lazy loading of second batch
    assertTrue(result.next()); // row 3 (from second batch)
    verify(databricksClient, times(1)).getMoreResults(statement);
    assertEquals(2, result.getCurrentRow());
    assertEquals("row3_col1", result.getObject(0));
    assertEquals("row3_col2", result.getObject(1));
    assertEquals(4, result.getTotalRowsFetched()); // Total from both batches

    // Continue to last row
    assertTrue(result.next()); // row 4
    assertEquals(3, result.getCurrentRow());
    assertEquals("row4_col1", result.getObject(0));

    // Should be end of data
    assertFalse(result.next());
    assertTrue(result.isCompletelyFetched());
  }

  @Test
  void testEmptyBatchHandling() throws SQLException {
    // Setup first batch (empty)
    TFetchResultsResp emptyBatch = createEmptyResponse(true); // hasMoreRows = true

    // Setup second batch (with data)
    TFetchResultsResp dataBatch =
        createResponseWithStringData(
            Arrays.asList("row1_col1", "row1_col2"), false); // hasMoreRows = false

    when(databricksClient.getMoreResults(statement)).thenReturn(dataBatch);

    LazyThriftResult result = new LazyThriftResult(emptyBatch, statement, session);

    // Should have no data initially but hasNext should be true due to hasMoreRows
    assertEquals(0, result.getRowCount());
    assertTrue(result.hasNext()); // Should be true because hasMoreRows = true

    // This should fetch through the empty batch to get data
    assertTrue(result.next());
    verify(databricksClient, times(1)).getMoreResults(statement);
    assertEquals("row1_col1", result.getObject(0));
    assertEquals(1, result.getTotalRowsFetched());
  }

  @Test
  void testMultipleEmptyBatches() throws SQLException {
    // Setup first batch (empty)
    TFetchResultsResp emptyBatch1 = createEmptyResponse(true);

    // Setup second batch (empty)
    TFetchResultsResp emptyBatch2 = createEmptyResponse(true);

    // Setup third batch (with data)
    TFetchResultsResp dataBatch =
        createResponseWithStringData(
            Arrays.asList("final_col1", "final_col2"), false); // hasMoreRows = false

    when(databricksClient.getMoreResults(statement)).thenReturn(emptyBatch2).thenReturn(dataBatch);

    LazyThriftResult result = new LazyThriftResult(emptyBatch1, statement, session);

    // Should skip through multiple empty batches
    assertTrue(result.next());
    verify(databricksClient, times(2)).getMoreResults(statement);
    assertEquals("final_col1", result.getObject(0));
  }

  @Test
  void testMaxRowsLimit() throws SQLException {
    when(statement.getMaxRows()).thenReturn(2); // Limit to 2 rows

    TFetchResultsResp response =
        createResponseWithStringData(
            Arrays.asList("row1_col1", "row1_col2"),
            Arrays.asList("row2_col1", "row2_col2"),
            Arrays.asList("row3_col1", "row3_col2"),
            true); // hasMoreRows = true

    LazyThriftResult result = new LazyThriftResult(response, statement, session);

    // Should only get first 2 rows due to maxRows limit
    assertTrue(result.next());
    assertEquals("row1_col1", result.getObject(0));

    assertTrue(result.next());
    assertEquals("row2_col1", result.getObject(0));

    // Should not proceed to next row due to limit
    assertFalse(result.hasNext());
    assertFalse(result.next());

    // Should not attempt to fetch more results from server
    verify(databricksClient, never()).getMoreResults(any());
  }

  @Test
  void testMaxRowsLimitAcrossBatches() throws SQLException {
    when(statement.getMaxRows()).thenReturn(3); // Limit to 3 rows

    // First batch has 2 rows
    TFetchResultsResp firstBatch =
        createResponseWithStringData(
            Arrays.asList("row1_col1", "row1_col2"),
            Arrays.asList("row2_col1", "row2_col2"),
            true); // hasMoreRows = true

    // Second batch has 2 rows
    TFetchResultsResp secondBatch =
        createResponseWithStringData(
            Arrays.asList("row3_col1", "row3_col2"),
            Arrays.asList("row4_col1", "row4_col2"),
            false); // hasMoreRows = false

    when(databricksClient.getMoreResults(statement)).thenReturn(secondBatch);

    LazyThriftResult result = new LazyThriftResult(firstBatch, statement, session);

    // Consume first batch
    assertTrue(result.next()); // row 1
    assertTrue(result.next()); // row 2

    // Should get one more row from second batch then stop
    assertTrue(result.next()); // row 3
    assertEquals("row3_col1", result.getObject(0));

    // Should stop due to maxRows limit
    assertFalse(result.hasNext());
    assertFalse(result.next());
    assertEquals(4, result.getTotalRowsFetched()); // 2 from first batch + 2 from second batch
  }

  @Test
  void testErrorHandlingDuringFetch() throws SQLException {
    TFetchResultsResp firstBatch =
        createResponseWithStringData(
            Arrays.asList("row1_col1", "row1_col2"), true); // hasMoreRows = true

    DatabricksSQLException expectedException =
        new DatabricksSQLException("Network error", DatabricksDriverErrorCode.CONNECTION_ERROR);
    when(databricksClient.getMoreResults(statement)).thenThrow(expectedException);

    LazyThriftResult result = new LazyThriftResult(firstBatch, statement, session);

    // Consume first batch
    assertTrue(result.next());

    // This should trigger the exception
    DatabricksSQLException thrown = assertThrows(DatabricksSQLException.class, result::next);
    assertEquals("Network error", thrown.getMessage());
    assertTrue(result.isCompletelyFetched()); // Should be marked as complete due to error
  }

  @Test
  void testClosedResultAccess() throws SQLException {
    TFetchResultsResp response =
        createResponseWithStringData(Arrays.asList("row1_col1", "row1_col2"), false);

    LazyThriftResult result = new LazyThriftResult(response, statement, session);
    result.next(); // Position on first row
    result.close();

    // Should throw exception when trying to access after close
    assertThrows(DatabricksSQLException.class, () -> result.getObject(0));
    assertFalse(result.hasNext());
    assertFalse(result.next());
  }

  @Test
  void testInvalidColumnIndex() throws SQLException {
    TFetchResultsResp response =
        createResponseWithStringData(
            Arrays.asList("col1", "col2"), // 2 columns
            false);

    LazyThriftResult result = new LazyThriftResult(response, statement, session);
    result.next(); // Move to first row

    // Valid indices
    assertDoesNotThrow(() -> result.getObject(0));
    assertDoesNotThrow(() -> result.getObject(1));

    // Invalid indices
    assertThrows(DatabricksSQLException.class, () -> result.getObject(2));
    assertThrows(DatabricksSQLException.class, () -> result.getObject(-1));
  }

  @Test
  void testAccessBeforeFirstRow() throws SQLException {
    TFetchResultsResp response =
        createResponseWithStringData(Arrays.asList("row1_col1", "row1_col2"), false);

    LazyThriftResult result = new LazyThriftResult(response, statement, session);

    // Should throw exception when trying to access before calling next()
    DatabricksSQLException thrown =
        assertThrows(DatabricksSQLException.class, () -> result.getObject(0));
    assertTrue(thrown.getMessage().contains("before first row"));
  }

  @Test
  void testNullHandling() throws SQLException {
    TFetchResultsResp response = createResponseWithNulls();

    LazyThriftResult result = new LazyThriftResult(response, statement, session);

    assertTrue(result.next());
    assertEquals("value1", result.getObject(0));
    assertNull(result.getObject(1)); // Should be null
  }

  @Test
  void testChunkCount() throws SQLException {
    TFetchResultsResp response = createEmptyResponse(false);
    LazyThriftResult result = new LazyThriftResult(response, statement, session);

    // LazyThriftResult doesn't use chunks like Arrow results
    assertEquals(0, result.getChunkCount());
  }

  // Helper methods for creating test data

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

    // Create a column for each column index
    for (int col = 0; col < numColumns; col++) {
      TColumn column = new TColumn();
      TStringColumn stringCol = new TStringColumn();
      List<String> colValues = new ArrayList<>();

      // Extract values for this column from all rows
      for (List<String> row : rows) {
        if (col < row.size()) {
          colValues.add(row.get(col));
        } else {
          colValues.add(null);
        }
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
    stringCol2.setValues(Arrays.asList("placeholder")); // Actual value doesn't matter
    stringCol2.setNulls(new byte[] {0x01}); // First bit set = null
    col2.setStringVal(stringCol2);
    columns.add(col2);

    rowSet.setColumns(columns);
    response.setResults(rowSet);

    return response;
  }
}
