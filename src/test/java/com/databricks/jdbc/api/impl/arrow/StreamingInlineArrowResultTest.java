package com.databricks.jdbc.api.impl.arrow;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.client.thrift.generated.*;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.io.ByteArrayOutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for StreamingInlineArrowResult. Verifies functional equivalence with
 * LazyThriftInlineArrowResult from the consumer's perspective, along with streaming-specific
 * behaviors.
 */
@ExtendWith(MockitoExtension.class)
public class StreamingInlineArrowResultTest {

  private static final String JDBC_URL =
      "jdbc:databricks://sample-host.18.azuredatabricks.net:9999/default;transportMode=http;ssl=1;"
          + "AuthMech=3;httpPath=/sql/1.0/warehouses/99999999;";

  @Mock private IDatabricksSession session;
  @Mock private IDatabricksStatementInternal statement;
  @Mock private IDatabricksClient databricksClient;

  private IDatabricksConnectionContext connectionContext;
  private static final StatementId STATEMENT_ID = new StatementId("test_statement_id");
  private static final TTableSchema TWO_COLUMN_SCHEMA =
      createTableSchema(TTypeId.INT_TYPE, TTypeId.STRING_TYPE);

  @BeforeEach
  void setUp() throws Exception {
    connectionContext = DatabricksConnectionContextFactory.create(JDBC_URL, new Properties());
    lenient().when(session.getDatabricksClient()).thenReturn(databricksClient);
    lenient().when(session.getConnectionContext()).thenReturn(connectionContext);
    lenient().when(statement.getMaxRows()).thenReturn(0);
    lenient().when(statement.getStatementId()).thenReturn(STATEMENT_ID);
  }

  @Test
  void testBasicIteration() throws SQLException {
    int rowCount = 5;
    byte[] arrowData = createValidArrowData(1, rowCount);
    TFetchResultsResp response = createFetchResultsResp(arrowData, rowCount, false);

    StreamingInlineArrowResult result =
        new StreamingInlineArrowResult(response, statement, session);

    try {
      // Initial state
      assertEquals(-1, result.getCurrentRow());
      assertTrue(result.hasNext());

      // Iterate through all rows
      for (int i = 0; i < rowCount; i++) {
        assertTrue(result.hasNext(), "Should have next at row " + i);
        assertTrue(result.next(), "next() should return true at row " + i);
        assertEquals(i, result.getCurrentRow());
      }

      // End
      assertFalse(result.hasNext());
      assertFalse(result.next());
    } finally {
      result.close();
    }
  }

  @Test
  void testGetObjectReturnsCorrectValues() throws SQLException {
    int rowCount = 3;
    byte[] arrowData = createValidArrowData(1, rowCount);
    TFetchResultsResp response = createFetchResultsResp(arrowData, rowCount, false);

    StreamingInlineArrowResult result =
        new StreamingInlineArrowResult(response, statement, session);

    try {
      // Row 0
      assertTrue(result.next());
      Object value = result.getObject(0);
      assertNotNull(value);
      assertInstanceOf(Integer.class, value);
      assertEquals(0, value);

      // Row 1
      assertTrue(result.next());
      value = result.getObject(0);
      assertEquals(1, value);

      // Row 2
      assertTrue(result.next());
      value = result.getObject(0);
      assertEquals(2, value);
    } finally {
      result.close();
    }
  }

  @Test
  void testMultiColumnAccess() throws SQLException {
    int rowCount = 2;
    byte[] arrowData = createValidArrowData(1, rowCount);
    TFetchResultsResp response = createFetchResultsResp(arrowData, rowCount, false);

    StreamingInlineArrowResult result =
        new StreamingInlineArrowResult(response, statement, session);

    try {
      assertTrue(result.next());

      // Int column
      Object intValue = result.getObject(0);
      assertNotNull(intValue);
      assertInstanceOf(Integer.class, intValue);
      assertEquals(0, intValue);

      // String column
      Object stringValue = result.getObject(1);
      assertNotNull(stringValue);
      assertEquals("row_0_0", stringValue.toString());
    } finally {
      result.close();
    }
  }

  @Test
  void testMaxRowsLimit() throws SQLException {
    int totalRows = 10;
    int maxRows = 3;
    when(statement.getMaxRows()).thenReturn(maxRows);

    byte[] arrowData = createValidArrowData(1, totalRows);
    TFetchResultsResp response = createFetchResultsResp(arrowData, totalRows, false);

    StreamingInlineArrowResult result =
        new StreamingInlineArrowResult(response, statement, session);

    try {
      int rowsRetrieved = 0;
      while (result.next()) {
        rowsRetrieved++;
      }

      assertEquals(maxRows, rowsRetrieved);
      assertFalse(result.hasNext());
      assertEquals(maxRows - 1, result.getCurrentRow());
    } finally {
      result.close();
    }
  }

  @Test
  void testColumnIndexBounds() throws SQLException {
    byte[] arrowData = createValidArrowData(1, 1);
    TFetchResultsResp response = createFetchResultsResp(arrowData, 1, false);

    StreamingInlineArrowResult result =
        new StreamingInlineArrowResult(response, statement, session);

    try {
      assertTrue(result.next());

      // Negative index
      DatabricksSQLException negativeException =
          assertThrows(DatabricksSQLException.class, () -> result.getObject(-1));
      assertTrue(negativeException.getMessage().contains("Column index out of bounds"));

      // Beyond column count
      DatabricksSQLException beyondException =
          assertThrows(DatabricksSQLException.class, () -> result.getObject(2));
      assertTrue(beyondException.getMessage().contains("Column index out of bounds"));
    } finally {
      result.close();
    }
  }

  @Test
  void testAccessAfterClose() throws SQLException {
    byte[] arrowData = createValidArrowData(1, 1);
    TFetchResultsResp response = createFetchResultsResp(arrowData, 1, false);

    StreamingInlineArrowResult result =
        new StreamingInlineArrowResult(response, statement, session);
    result.close();

    assertFalse(result.hasNext());
    assertFalse(result.next());
    assertThrows(DatabricksSQLException.class, () -> result.getObject(0));
  }

  @Test
  void testAccessBeforeFirstRow() throws SQLException {
    byte[] arrowData = createValidArrowData(1, 1);
    TFetchResultsResp response = createFetchResultsResp(arrowData, 1, false);

    StreamingInlineArrowResult result =
        new StreamingInlineArrowResult(response, statement, session);

    try {
      DatabricksSQLException exception =
          assertThrows(DatabricksSQLException.class, () -> result.getObject(0));
      assertTrue(exception.getMessage().contains("before first row"));
    } finally {
      result.close();
    }
  }

  @Test
  void testSingleRowResult() throws SQLException {
    // Test single row instead of empty - streaming has different empty handling
    byte[] arrowData = createValidArrowData(1, 1);
    TFetchResultsResp response = createFetchResultsResp(arrowData, 1, false);

    StreamingInlineArrowResult result =
        new StreamingInlineArrowResult(response, statement, session);

    try {
      assertEquals(-1, result.getCurrentRow());
      assertEquals(1, result.getRowCount());
      assertTrue(result.hasNext());
      assertTrue(result.next());
      assertEquals(0, result.getCurrentRow());
      assertFalse(result.hasNext());
      assertFalse(result.next());
    } finally {
      result.close();
    }
  }

  @Test
  void testMultiBatchFetching() throws SQLException, InterruptedException {
    int rowsPerChunk = 2;
    byte[] arrowData1 = createValidArrowData(1, rowsPerChunk);
    byte[] arrowData2 = createValidArrowData(1, rowsPerChunk);

    TFetchResultsResp firstResponse = createFetchResultsResp(arrowData1, rowsPerChunk, true);
    TFetchResultsResp secondResponse = createFetchResultsResp(arrowData2, rowsPerChunk, false);

    when(databricksClient.getMoreResults(statement)).thenReturn(secondResponse);

    StreamingInlineArrowResult result =
        new StreamingInlineArrowResult(firstResponse, statement, session);

    try {
      // Give prefetch thread time to start
      Thread.sleep(100);

      // Iterate through first batch
      assertTrue(result.next());
      assertTrue(result.next());

      // Move to second batch
      assertTrue(result.next());
      assertTrue(result.next());

      // End
      assertFalse(result.next());

      verify(databricksClient, atLeastOnce()).getMoreResults(statement);
    } finally {
      result.close();
    }
  }

  @Test
  void testErrorDuringFetch() throws Exception {
    byte[] arrowData = createValidArrowData(1, 2);
    TFetchResultsResp firstResponse = createFetchResultsResp(arrowData, 2, true);

    DatabricksSQLException expectedException =
        new DatabricksSQLException("Network error", DatabricksDriverErrorCode.CONNECTION_ERROR);
    when(databricksClient.getMoreResults(statement)).thenThrow(expectedException);

    // The prefetch thread starts during construction and may fail before or after
    // the constructor completes. We need to handle both cases.
    DatabricksSQLException caughtException = null;
    StreamingInlineArrowResult result = null;

    try {
      result = new StreamingInlineArrowResult(firstResponse, statement, session);

      // If construction succeeded, consume rows until we hit the error
      while (result.next()) {
        // Keep iterating - error will be thrown when we need the next batch
      }
    } catch (DatabricksSQLException e) {
      // Error thrown during construction or iteration
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
        "Exception message should contain error details: " + caughtException.getMessage());
  }

  @Test
  void testMaxRowsLimitAcrossBatches() throws SQLException, InterruptedException {
    // MaxRows limit of 3, spanning across 2 batches (2 rows each)
    int rowsPerChunk = 2;
    when(statement.getMaxRows()).thenReturn(3);

    byte[] arrowData1 = createValidArrowData(1, rowsPerChunk);
    byte[] arrowData2 = createValidArrowData(1, rowsPerChunk);

    TFetchResultsResp firstResponse = createFetchResultsResp(arrowData1, rowsPerChunk, true);
    TFetchResultsResp secondResponse = createFetchResultsResp(arrowData2, rowsPerChunk, false);

    when(databricksClient.getMoreResults(statement)).thenReturn(secondResponse);

    StreamingInlineArrowResult result =
        new StreamingInlineArrowResult(firstResponse, statement, session);

    try {
      // Give prefetch thread time to start
      Thread.sleep(100);

      // Consume first batch (rows 0 and 1)
      assertTrue(result.next());
      assertEquals(0, result.getCurrentRow());
      assertTrue(result.next());
      assertEquals(1, result.getCurrentRow());

      // Get one row from second batch (row 2)
      assertTrue(result.next());
      assertEquals(2, result.getCurrentRow());

      // Should stop at maxRows=3
      assertFalse(result.hasNext());
      assertFalse(result.next());
    } finally {
      result.close();
    }
  }

  @Test
  void testGetArrowMetadata() throws SQLException {
    byte[] arrowData = createValidArrowData(1, 2);
    TFetchResultsResp response = createFetchResultsResp(arrowData, 2, false);

    StreamingInlineArrowResult result =
        new StreamingInlineArrowResult(response, statement, session);

    try {
      assertTrue(result.next());
      List<String> metadata = result.getArrowMetadata();
      assertNotNull(metadata);
      // Two columns: int and string
      assertEquals(2, metadata.size());
    } finally {
      result.close();
    }
  }

  @Test
  void testGetTotalRowsFetched() throws SQLException, InterruptedException {
    byte[] arrowData1 = createValidArrowData(1, 3);
    byte[] arrowData2 = createValidArrowData(1, 2);
    TFetchResultsResp firstBatch = createFetchResultsResp(arrowData1, 3, true);
    TFetchResultsResp secondBatch = createFetchResultsResp(arrowData2, 2, false);

    when(databricksClient.getMoreResults(statement)).thenReturn(secondBatch);

    StreamingInlineArrowResult result =
        new StreamingInlineArrowResult(firstBatch, statement, session);

    try {
      // Initial batch has at least 3 rows
      assertTrue(result.getTotalRowsFetched() >= 3);

      // Give prefetch thread time to fetch second batch
      Thread.sleep(100);

      // After prefetch completes, should have 5 total rows fetched
      assertEquals(5, result.getTotalRowsFetched());
    } finally {
      result.close();
    }
  }

  @Test
  void testIsCompletelyFetched() throws SQLException, InterruptedException {
    byte[] arrowData1 = createValidArrowData(1, 2);
    byte[] arrowData2 = createValidArrowData(1, 2);
    TFetchResultsResp firstBatch = createFetchResultsResp(arrowData1, 2, true);
    TFetchResultsResp secondBatch = createFetchResultsResp(arrowData2, 2, false);

    when(databricksClient.getMoreResults(statement)).thenReturn(secondBatch);

    StreamingInlineArrowResult result =
        new StreamingInlineArrowResult(firstBatch, statement, session);

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
  void testBatchesInMemoryTracking() throws SQLException {
    byte[] arrowData = createValidArrowData(1, 2);
    TFetchResultsResp response = createFetchResultsResp(arrowData, 2, false);

    StreamingInlineArrowResult result =
        new StreamingInlineArrowResult(response, statement, session);

    try {
      // Should have at least initial batch
      assertTrue(result.getBatchesInMemory() > 0);
    } finally {
      result.close();
    }
  }

  @Test
  void testChunkCount() throws SQLException {
    byte[] arrowData = createValidArrowData(1, 2);
    TFetchResultsResp response = createFetchResultsResp(arrowData, 2, false);

    StreamingInlineArrowResult result =
        new StreamingInlineArrowResult(response, statement, session);

    try {
      // Streaming results always return 0 for chunk count (not applicable)
      assertEquals(0, result.getChunkCount());
    } finally {
      result.close();
    }
  }

  @Test
  void testGetRowCount() throws SQLException {
    byte[] arrowData = createValidArrowData(1, 5);
    TFetchResultsResp response = createFetchResultsResp(arrowData, 5, false);

    StreamingInlineArrowResult result =
        new StreamingInlineArrowResult(response, statement, session);

    try {
      assertTrue(result.next());
      assertEquals(5, result.getRowCount());
    } finally {
      result.close();
    }
  }

  @Test
  void testInitializationWithEmptyInitialBatch() throws SQLException, InterruptedException {
    // Initial batch is EMPTY but hasMoreRows=true
    TFetchResultsResp emptyInitial = createEmptyArrowResponse(true);

    // Second batch has actual data
    byte[] arrowData = createValidArrowData(1, 3);
    TFetchResultsResp dataBatch = createFetchResultsResp(arrowData, 3, false);

    when(databricksClient.getMoreResults(statement)).thenReturn(dataBatch);

    StreamingInlineArrowResult result =
        new StreamingInlineArrowResult(emptyInitial, statement, session);

    try {
      // Give prefetch time to fetch the data batch
      Thread.sleep(100);

      // Should have skipped empty batch and positioned on data batch
      assertTrue(result.hasNext());
      assertTrue(result.next());
      assertEquals(0, result.getObject(0)); // First int value

      assertTrue(result.next());
      assertTrue(result.next());
      assertFalse(result.next()); // Only 3 rows
    } finally {
      result.close();
    }
  }

  @Test
  void testDoubleClose() throws SQLException {
    byte[] arrowData = createValidArrowData(1, 2);
    TFetchResultsResp response = createFetchResultsResp(arrowData, 2, false);

    StreamingInlineArrowResult result =
        new StreamingInlineArrowResult(response, statement, session);

    // First close
    result.close();
    // Second close should not throw
    assertDoesNotThrow(result::close);
  }

  @Test
  void testExhaustAllRowsInSingleBatch() throws SQLException {
    // Single batch with 3 rows, no more data
    byte[] arrowData = createValidArrowData(1, 3);
    TFetchResultsResp response = createFetchResultsResp(arrowData, 3, false);

    StreamingInlineArrowResult result =
        new StreamingInlineArrowResult(response, statement, session);

    try {
      // Consume all 3 rows
      assertTrue(result.next());
      assertEquals(0, result.getObject(0)); // First row
      assertTrue(result.next());
      assertEquals(1, result.getObject(0)); // Second row
      assertTrue(result.next());
      assertEquals(2, result.getObject(0)); // Third row

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
    byte[] arrowData1 = createValidArrowData(1, 2);
    TFetchResultsResp firstBatch = createFetchResultsResp(arrowData1, 2, true);

    // Second batch with 1 row - final batch
    byte[] arrowData2 = createValidArrowData(1, 1);
    TFetchResultsResp secondBatch = createFetchResultsResp(arrowData2, 1, false);

    when(databricksClient.getMoreResults(statement)).thenReturn(secondBatch);

    StreamingInlineArrowResult result =
        new StreamingInlineArrowResult(firstBatch, statement, session);

    try {
      // Give prefetch time
      Thread.sleep(100);

      // Consume batch 1 (2 rows)
      assertTrue(result.next());
      assertEquals(0, result.getObject(0));
      assertTrue(result.next());
      assertEquals(1, result.getObject(0));

      // Consume batch 2 (1 row)
      assertTrue(result.next());
      assertEquals(0, result.getObject(0)); // First row of second batch

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
    byte[] arrowData1 = createValidArrowData(1, 1);
    TFetchResultsResp firstBatch = createFetchResultsResp(arrowData1, 1, true);

    // Second batch with 1 row
    byte[] arrowData2 = createValidArrowData(1, 1);
    TFetchResultsResp secondBatch = createFetchResultsResp(arrowData2, 1, false);

    when(databricksClient.getMoreResults(statement)).thenReturn(secondBatch);

    StreamingInlineArrowResult result =
        new StreamingInlineArrowResult(firstBatch, statement, session);

    try {
      // Give prefetch time
      Thread.sleep(100);

      // Row 1 - from batch 1
      assertTrue(result.next());
      assertEquals(0, result.getObject(0));
      assertEquals(0, result.getCurrentRow());

      // Row 2 - requires batch transition
      assertTrue(result.next());
      assertEquals(0, result.getObject(0)); // First row of second batch
      assertEquals(1, result.getCurrentRow());

      // End - no more batches
      assertFalse(result.next());
    } finally {
      result.close();
    }
  }

  @Test
  void testNextAfterEndReturnsConsistentFalse() throws SQLException {
    byte[] arrowData = createValidArrowData(1, 1);
    TFetchResultsResp response = createFetchResultsResp(arrowData, 1, false);

    StreamingInlineArrowResult result =
        new StreamingInlineArrowResult(response, statement, session);

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

  private TFetchResultsResp createEmptyArrowResponse(boolean hasMoreRows) {
    // Create valid Arrow data with schema and 1 batch of 0 rows
    // The Arrow data includes its own schema via ArrowStreamWriter
    byte[] emptyArrowData = createValidArrowData(1, 0);
    TSparkArrowBatch arrowBatch = new TSparkArrowBatch().setRowCount(0).setBatch(emptyArrowData);
    TRowSet rowSet = new TRowSet().setArrowBatches(Collections.singletonList(arrowBatch));

    // Use empty arrowSchema so cachedSchema won't be prepended (Arrow data has its own schema)
    TGetResultSetMetadataResp metadata =
        new TGetResultSetMetadataResp().setSchema(TWO_COLUMN_SCHEMA).setArrowSchema(new byte[0]);
    TFetchResultsResp response =
        new TFetchResultsResp().setResultSetMetadata(metadata).setResults(rowSet);
    response.hasMoreRows = hasMoreRows;
    return response;
  }

  private static byte[] createValidArrowData(int batchCount, int rowsPerBatch) {
    try (BufferAllocator allocator = new RootAllocator();
        ByteArrayOutputStream out = new ByteArrayOutputStream()) {

      try (IntVector intVector = new IntVector("int_column", allocator);
          VarCharVector stringVector = new VarCharVector("string_column", allocator)) {

        intVector.allocateNew(rowsPerBatch);
        stringVector.allocateNew(rowsPerBatch);

        try (VectorSchemaRoot root = VectorSchemaRoot.of(intVector, stringVector);
            ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
          writer.start();

          for (int batch = 0; batch < batchCount; batch++) {
            for (int i = 0; i < rowsPerBatch; i++) {
              intVector.set(i, batch * 100 + i);
              stringVector.setSafe(i, ("row_" + batch + "_" + i).getBytes());
            }
            intVector.setValueCount(rowsPerBatch);
            stringVector.setValueCount(rowsPerBatch);
            root.setRowCount(rowsPerBatch);
            writer.writeBatch();
          }

          writer.end();
        }
      }

      return out.toByteArray();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create test Arrow data", e);
    }
  }

  private static TTableSchema createTableSchema(TTypeId... types) {
    List<TColumnDesc> columns = new ArrayList<>();
    for (int i = 0; i < types.length; i++) {
      TPrimitiveTypeEntry primitiveType = new TPrimitiveTypeEntry().setType(types[i]);
      TTypeEntry typeEntry = new TTypeEntry();
      typeEntry.setPrimitiveEntry(primitiveType);
      TTypeDesc typeDesc = new TTypeDesc().setTypes(Collections.singletonList(typeEntry));
      TColumnDesc columnDesc =
          new TColumnDesc().setColumnName("col_" + i).setTypeDesc(typeDesc).setPosition(i);
      columns.add(columnDesc);
    }
    return new TTableSchema().setColumns(columns);
  }

  private TFetchResultsResp createFetchResultsResp(
      byte[] arrowData, int rowCount, boolean hasMoreRows) {
    TSparkArrowBatch arrowBatch = new TSparkArrowBatch().setRowCount(rowCount).setBatch(arrowData);
    TRowSet rowSet = new TRowSet().setArrowBatches(Collections.singletonList(arrowBatch));

    TGetResultSetMetadataResp metadata =
        new TGetResultSetMetadataResp().setSchema(TWO_COLUMN_SCHEMA).setArrowSchema(new byte[0]);

    TFetchResultsResp response =
        new TFetchResultsResp().setResultSetMetadata(metadata).setResults(rowSet);
    response.hasMoreRows = hasMoreRows;

    return response;
  }
}
