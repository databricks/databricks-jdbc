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

@ExtendWith(MockitoExtension.class)
public class LazyThriftInlineArrowResultTest {

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
  }

  /**
   * Creates valid Arrow IPC format bytes with two columns (int and string). This creates a complete
   * Arrow IPC stream (with embedded schema) that can be parsed by ArrowStreamReader.
   *
   * <p>Int column values: batch_index * 100 + row_index (e.g., 0, 1, 2... for batch 0) String
   * column values: "row_{batch_index}_{row_index}" (e.g., "row_0_0", "row_0_1"...)
   *
   * @param batchCount Number of record batches to create
   * @param rowsPerBatch Number of rows in each batch
   * @return Valid Arrow IPC bytes
   */
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

  /**
   * Creates a TTableSchema with the specified column types. The schema structure matches what
   * hiveSchemaToArrowSchema expects.
   */
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

  /**
   * Creates a TFetchResultsResp with valid Arrow data. Uses empty arrowSchema bytes so that the
   * complete Arrow IPC stream (with embedded schema) is used as-is.
   *
   * @param arrowData Valid Arrow IPC bytes (should include schema)
   * @param rowCount Row count for the batch
   * @param hasMoreRows Whether there are more rows to fetch
   * @return Configured TFetchResultsResp
   */
  private TFetchResultsResp createFetchResultsResp(
      byte[] arrowData, int rowCount, boolean hasMoreRows) {
    TSparkArrowBatch arrowBatch = new TSparkArrowBatch().setRowCount(rowCount).setBatch(arrowData);
    TRowSet rowSet = new TRowSet().setArrowBatches(Collections.singletonList(arrowBatch));

    // Use empty arrowSchema - this causes getSerializedSchema to return empty bytes,
    // which effectively means nothing is prepended to our complete Arrow IPC stream
    TGetResultSetMetadataResp metadata =
        new TGetResultSetMetadataResp().setSchema(TWO_COLUMN_SCHEMA).setArrowSchema(new byte[0]);

    TFetchResultsResp response =
        new TFetchResultsResp().setResultSetMetadata(metadata).setResults(rowSet);
    response.hasMoreRows = hasMoreRows;

    return response;
  }

  @Test
  void testEmptyResultSet() throws SQLException {
    byte[] arrowData = createValidArrowData(1, 0);
    TFetchResultsResp initialResponse = createFetchResultsResp(arrowData, 0, false);

    when(statement.getMaxRows()).thenReturn(0);
    when(statement.getStatementId()).thenReturn(STATEMENT_ID);

    LazyThriftInlineArrowResult result =
        new LazyThriftInlineArrowResult(initialResponse, statement, session);

    // Verify all initial state for empty result
    assertEquals(-1, result.getCurrentRow());
    assertEquals(0, result.getRowCount());
    assertEquals(0, result.getTotalRowsFetched());
    assertEquals(0, result.getChunkCount());
    assertFalse(result.hasNext());
    assertFalse(result.next());
    assertTrue(result.isCompletelyFetched());
  }

  @Test
  void testGetObjectThrowsWhenClosed() throws SQLException {
    byte[] arrowData = createValidArrowData(1, 1);
    TFetchResultsResp initialResponse = createFetchResultsResp(arrowData, 1, false);

    when(statement.getMaxRows()).thenReturn(0);
    when(statement.getStatementId()).thenReturn(STATEMENT_ID);

    LazyThriftInlineArrowResult result =
        new LazyThriftInlineArrowResult(initialResponse, statement, session);
    result.close();

    // Verify close behavior
    assertFalse(result.hasNext());
    assertFalse(result.next());
    assertEquals(0, result.getRowCount());

    DatabricksSQLException exception =
        assertThrows(DatabricksSQLException.class, () -> result.getObject(0));
    assertEquals("Result is already closed", exception.getMessage());
    assertEquals(DatabricksDriverErrorCode.STATEMENT_CLOSED.name(), exception.getSQLState());
  }

  @Test
  void testGetObjectThrowsWhenBeforeFirstRow() throws SQLException {
    byte[] arrowData = createValidArrowData(1, 1);
    TFetchResultsResp initialResponse = createFetchResultsResp(arrowData, 1, false);

    when(statement.getMaxRows()).thenReturn(0);
    when(statement.getStatementId()).thenReturn(STATEMENT_ID);

    LazyThriftInlineArrowResult result =
        new LazyThriftInlineArrowResult(initialResponse, statement, session);

    DatabricksSQLException exception =
        assertThrows(DatabricksSQLException.class, () -> result.getObject(0));
    assertEquals("Cursor is before first row", exception.getMessage());
    assertEquals(DatabricksDriverErrorCode.INVALID_STATE.name(), exception.getSQLState());
  }

  @Test
  void testIsCompletelyFetchedWithMoreRows() throws SQLException {
    byte[] arrowData = createValidArrowData(1, 0);
    TFetchResultsResp initialResponse = createFetchResultsResp(arrowData, 0, true);

    when(statement.getMaxRows()).thenReturn(0);
    when(statement.getStatementId()).thenReturn(STATEMENT_ID);

    LazyThriftInlineArrowResult result =
        new LazyThriftInlineArrowResult(initialResponse, statement, session);

    assertFalse(result.isCompletelyFetched());
    assertTrue(result.hasNext()); // hasNext is true because hasMoreRows is true
  }

  @Test
  void testIterateThroughRowsWithValidArrowData() throws SQLException {
    int rowCount = 5;
    byte[] arrowData = createValidArrowData(1, rowCount);
    TFetchResultsResp initialResponse = createFetchResultsResp(arrowData, rowCount, false);

    when(statement.getMaxRows()).thenReturn(0);
    when(statement.getStatementId()).thenReturn(STATEMENT_ID);

    LazyThriftInlineArrowResult result =
        new LazyThriftInlineArrowResult(initialResponse, statement, session);

    // Verify initial state
    assertEquals(-1, result.getCurrentRow());
    assertTrue(result.hasNext());

    // Iterate through all rows
    for (int i = 0; i < rowCount; i++) {
      assertTrue(result.hasNext(), "Should have next at row " + i);
      assertTrue(result.next(), "next() should return true at row " + i);
      assertEquals(i, result.getCurrentRow());
    }

    // After all rows
    assertFalse(result.hasNext());
    assertFalse(result.next());
    assertEquals(rowCount, result.getTotalRowsFetched());
  }

  @Test
  void testGetObjectReturnsCorrectIntegerValue() throws SQLException {
    int rowCount = 3;
    byte[] arrowData = createValidArrowData(1, rowCount);
    TFetchResultsResp initialResponse = createFetchResultsResp(arrowData, rowCount, false);

    when(statement.getMaxRows()).thenReturn(0);
    when(statement.getStatementId()).thenReturn(STATEMENT_ID);
    when(session.getConnectionContext()).thenReturn(connectionContext);

    LazyThriftInlineArrowResult result =
        new LazyThriftInlineArrowResult(initialResponse, statement, session);

    // Move to first row and get value
    assertTrue(result.next());
    Object value = result.getObject(0);
    assertNotNull(value);
    assertInstanceOf(Integer.class, value);
    assertEquals(0, value); // First row: batch_0 * 100 + row_0 = 0

    // Move to second row
    assertTrue(result.next());
    value = result.getObject(0);
    assertEquals(1, value); // Second row: batch_0 * 100 + row_1 = 1

    // Move to third row
    assertTrue(result.next());
    value = result.getObject(0);
    assertEquals(2, value); // Third row: batch_0 * 100 + row_2 = 2
  }

  @Test
  void testGetObjectWithTwoColumns() throws SQLException {
    int rowCount = 2;
    byte[] arrowData = createValidArrowData(1, rowCount);
    TFetchResultsResp initialResponse = createFetchResultsResp(arrowData, rowCount, false);

    when(statement.getMaxRows()).thenReturn(0);
    when(statement.getStatementId()).thenReturn(STATEMENT_ID);
    when(session.getConnectionContext()).thenReturn(connectionContext);

    LazyThriftInlineArrowResult result =
        new LazyThriftInlineArrowResult(initialResponse, statement, session);

    assertTrue(result.next());

    // Get integer column
    Object intValue = result.getObject(0);
    assertNotNull(intValue);
    assertInstanceOf(Integer.class, intValue);
    assertEquals(0, intValue);

    // Get string column
    Object stringValue = result.getObject(1);
    assertNotNull(stringValue);
    assertEquals("row_0_0", stringValue.toString());
  }

  @Test
  void testGetObjectThrowsForColumnIndexOutOfBounds() throws SQLException {
    int rowCount = 1;
    byte[] arrowData = createValidArrowData(1, rowCount);
    TFetchResultsResp initialResponse = createFetchResultsResp(arrowData, rowCount, false);

    when(statement.getMaxRows()).thenReturn(0);
    when(statement.getStatementId()).thenReturn(STATEMENT_ID);
    // Note: session.getConnectionContext() is not stubbed here because the column index
    // validation happens before the connection context is accessed

    LazyThriftInlineArrowResult result =
        new LazyThriftInlineArrowResult(initialResponse, statement, session);

    assertTrue(result.next());

    // Test negative index
    DatabricksSQLException negativeException =
        assertThrows(DatabricksSQLException.class, () -> result.getObject(-1));
    assertTrue(negativeException.getMessage().contains("Column index out of bounds"));
    assertEquals(DatabricksDriverErrorCode.INVALID_STATE.name(), negativeException.getSQLState());

    // Test index beyond column count (we have 2 columns: 0 and 1)
    DatabricksSQLException beyondException =
        assertThrows(DatabricksSQLException.class, () -> result.getObject(2));
    assertTrue(beyondException.getMessage().contains("Column index out of bounds"));
    assertEquals(DatabricksDriverErrorCode.INVALID_STATE.name(), beyondException.getSQLState());
  }

  @Test
  void testMaxRowsLimitEnforced() throws SQLException {
    int totalRows = 10;
    int maxRows = 3;
    byte[] arrowData = createValidArrowData(1, totalRows);
    TFetchResultsResp initialResponse = createFetchResultsResp(arrowData, totalRows, false);

    when(statement.getMaxRows()).thenReturn(maxRows);
    when(statement.getStatementId()).thenReturn(STATEMENT_ID);

    LazyThriftInlineArrowResult result =
        new LazyThriftInlineArrowResult(initialResponse, statement, session);

    // Should only be able to iterate up to maxRows
    int rowsRetrieved = 0;
    while (result.next()) {
      rowsRetrieved++;
    }

    assertEquals(maxRows, rowsRetrieved);
    assertFalse(result.hasNext());
    assertEquals(maxRows - 1, result.getCurrentRow()); // 0-based index
  }

  @Test
  void testGetArrowMetadataReturnsMetadata() throws SQLException {
    int rowCount = 1;
    byte[] arrowData = createValidArrowData(1, rowCount);
    TFetchResultsResp initialResponse = createFetchResultsResp(arrowData, rowCount, false);

    when(statement.getMaxRows()).thenReturn(0);
    when(statement.getStatementId()).thenReturn(STATEMENT_ID);

    LazyThriftInlineArrowResult result =
        new LazyThriftInlineArrowResult(initialResponse, statement, session);

    List<String> metadata = result.getArrowMetadata();
    assertNotNull(metadata);
    // The metadata list should have one entry per column (2 columns: int and string)
    assertEquals(2, metadata.size());
  }

  @Test
  void testFetchNextChunkFromServer() throws SQLException {
    int rowsPerChunk = 2;
    byte[] arrowData1 = createValidArrowData(1, rowsPerChunk);
    byte[] arrowData2 = createValidArrowData(1, rowsPerChunk);

    // First chunk with hasMoreRows = true
    TFetchResultsResp initialResponse = createFetchResultsResp(arrowData1, rowsPerChunk, true);

    // Second chunk with hasMoreRows = false
    TFetchResultsResp secondResponse = createFetchResultsResp(arrowData2, rowsPerChunk, false);

    when(statement.getMaxRows()).thenReturn(0);
    when(statement.getStatementId()).thenReturn(STATEMENT_ID);
    when(session.getDatabricksClient()).thenReturn(databricksClient);
    when(databricksClient.getMoreResults(statement)).thenReturn(secondResponse);

    LazyThriftInlineArrowResult result =
        new LazyThriftInlineArrowResult(initialResponse, statement, session);

    // Iterate through first chunk
    assertTrue(result.next());
    assertTrue(result.next());
    assertFalse(result.isCompletelyFetched()); // Still has more rows

    // This should trigger fetch of next chunk
    assertTrue(result.next());
    assertTrue(result.next());

    // After all rows
    assertFalse(result.next());
    assertTrue(result.isCompletelyFetched());
    assertEquals(rowsPerChunk * 2, result.getTotalRowsFetched());

    // Verify that getMoreResults was called
    verify(databricksClient).getMoreResults(statement);
  }

  @Test
  void testGetRowCountReturnsCurrentChunkRowCount() throws SQLException {
    int rowCount = 5;
    byte[] arrowData = createValidArrowData(1, rowCount);
    TFetchResultsResp initialResponse = createFetchResultsResp(arrowData, rowCount, false);

    when(statement.getMaxRows()).thenReturn(0);
    when(statement.getStatementId()).thenReturn(STATEMENT_ID);

    LazyThriftInlineArrowResult result =
        new LazyThriftInlineArrowResult(initialResponse, statement, session);

    assertEquals(rowCount, result.getRowCount());
  }
}
