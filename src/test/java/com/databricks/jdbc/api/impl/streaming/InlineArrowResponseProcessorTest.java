package com.databricks.jdbc.api.impl.streaming;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.impl.arrow.ArrowResultChunk;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.client.thrift.generated.*;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.junit.jupiter.api.Test;

/** Unit tests for InlineArrowResponseProcessor. Tests schema caching and Arrow chunk creation. */
public class InlineArrowResponseProcessorTest {

  private static final StatementId STATEMENT_ID = new StatementId("test_statement");

  @Test
  void testProcessInitialResponseCachesSchema() throws DatabricksSQLException {
    InlineArrowResponseProcessor processor = new InlineArrowResponseProcessor(STATEMENT_ID);

    TFetchResultsResp initialResponse = createArrowFetchResponse(2, true);
    StreamingBatch<ArrowResultChunk> batch = processor.processInitialResponse(initialResponse);

    assertNotNull(batch);
    assertEquals(0, batch.getBatchIndex());
    assertEquals(0, batch.getRowOffset());
    assertEquals(2, batch.getRowCount());
    assertTrue(batch.hasMoreRows());
    assertTrue(batch.isReady());

    ArrowResultChunk chunk = batch.getData();
    assertNotNull(chunk);

    // Clean up native memory
    batch.release();
  }

  @Test
  void testProcessSubsequentResponseUsesSchemaCache() throws DatabricksSQLException {
    InlineArrowResponseProcessor processor = new InlineArrowResponseProcessor(STATEMENT_ID);

    // First, process initial response to cache schema
    TFetchResultsResp initialResponse = createArrowFetchResponse(2, true);
    StreamingBatch<ArrowResultChunk> batch1 = processor.processInitialResponse(initialResponse);

    // Now process a subsequent response (simulating second batch)
    // The schema should be cached from the initial response
    TFetchResultsResp subsequentResponse = createArrowFetchResponseNoSchema(3, false);
    StreamingBatch<ArrowResultChunk> batch2 = processor.processResponse(subsequentResponse, 1, 100);

    assertNotNull(batch2);
    assertEquals(1, batch2.getBatchIndex());
    assertEquals(100, batch2.getRowOffset());
    assertEquals(3, batch2.getRowCount());
    assertFalse(batch2.hasMoreRows());

    // Clean up native memory
    batch1.release();
    batch2.release();
  }

  @Test
  void testReleaseActionReleasesNativeMemory() throws DatabricksSQLException {
    InlineArrowResponseProcessor processor = new InlineArrowResponseProcessor(STATEMENT_ID);

    TFetchResultsResp response = createArrowFetchResponse(1, false);
    StreamingBatch<ArrowResultChunk> batch = processor.processInitialResponse(response);

    ArrowResultChunk chunk = batch.getData();
    assertNotNull(chunk);

    // Release should clean up native memory without throwing
    batch.release();

    assertEquals(StreamingBatch.Status.RELEASED, batch.getStatus());
    assertNull(batch.getData());
  }

  @Test
  void testGetReleaseAction() throws DatabricksSQLException {
    InlineArrowResponseProcessor processor = new InlineArrowResponseProcessor(STATEMENT_ID);

    var releaseAction = processor.getReleaseAction();
    assertNotNull(releaseAction);

    // Should handle null gracefully
    assertDoesNotThrow(() -> releaseAction.accept(null));

    // Create a real chunk and verify release works
    TFetchResultsResp response = createArrowFetchResponse(1, false);
    StreamingBatch<ArrowResultChunk> batch = processor.processInitialResponse(response);
    ArrowResultChunk chunk = batch.getData();

    // Release should work without throwing
    assertDoesNotThrow(() -> releaseAction.accept(chunk));
  }

  @Test
  void testProcessInitialResponseWithNullStatementId() throws DatabricksSQLException {
    // Processor should work even with null statement ID
    InlineArrowResponseProcessor processor = new InlineArrowResponseProcessor(null);

    TFetchResultsResp response = createArrowFetchResponse(1, false);
    StreamingBatch<ArrowResultChunk> batch = processor.processInitialResponse(response);

    assertNotNull(batch);
    assertTrue(batch.isReady());

    batch.release();
  }

  @Test
  void testBatchMetadataCorrectlyPropagated() throws DatabricksSQLException {
    InlineArrowResponseProcessor processor = new InlineArrowResponseProcessor(STATEMENT_ID);

    // Test with hasMoreRows = true
    TFetchResultsResp responseWithMore = createArrowFetchResponse(5, true);
    StreamingBatch<ArrowResultChunk> batch1 = processor.processInitialResponse(responseWithMore);
    assertEquals(5, batch1.getRowCount());
    assertTrue(batch1.hasMoreRows());

    // Test with hasMoreRows = false
    TFetchResultsResp responseNoMore = createArrowFetchResponseNoSchema(3, false);
    StreamingBatch<ArrowResultChunk> batch2 = processor.processResponse(responseNoMore, 1, 5);
    assertEquals(3, batch2.getRowCount());
    assertFalse(batch2.hasMoreRows());

    batch1.release();
    batch2.release();
  }

  // ==================== Helper Methods ====================

  /** Creates an Arrow response with full schema (for initial response). */
  private TFetchResultsResp createArrowFetchResponse(int rowCount, boolean hasMoreRows) {
    TFetchResultsResp response = new TFetchResultsResp();
    response.hasMoreRows = hasMoreRows;

    byte[] arrowData = createArrowBytes(rowCount);

    TGetResultSetMetadataResp metadata = new TGetResultSetMetadataResp();
    metadata.setResultFormat(TSparkRowSetType.ARROW_BASED_SET);
    metadata.setArrowSchema(new byte[0]); // Empty - schema is in the IPC stream
    metadata.setSchema(createTableSchema());
    response.setResultSetMetadata(metadata);

    TSparkArrowBatch arrowBatch = new TSparkArrowBatch();
    arrowBatch.setRowCount(rowCount);
    arrowBatch.setBatch(arrowData);

    TRowSet rowSet = new TRowSet();
    rowSet.setArrowBatches(Collections.singletonList(arrowBatch));
    response.setResults(rowSet);

    return response;
  }

  /**
   * Creates an Arrow response for subsequent batches (minimal metadata, schema cached from
   * initial).
   */
  private TFetchResultsResp createArrowFetchResponseNoSchema(int rowCount, boolean hasMoreRows) {
    TFetchResultsResp response = new TFetchResultsResp();
    response.hasMoreRows = hasMoreRows;

    byte[] arrowData = createArrowBytes(rowCount);

    // Minimal metadata required for processing (processor uses cached schema)
    TGetResultSetMetadataResp metadata = new TGetResultSetMetadataResp();
    metadata.setResultFormat(TSparkRowSetType.ARROW_BASED_SET);
    response.setResultSetMetadata(metadata);

    TSparkArrowBatch arrowBatch = new TSparkArrowBatch();
    arrowBatch.setRowCount(rowCount);
    arrowBatch.setBatch(arrowData);

    TRowSet rowSet = new TRowSet();
    rowSet.setArrowBatches(Collections.singletonList(arrowBatch));
    response.setResults(rowSet);

    return response;
  }

  /** Creates valid Arrow IPC format bytes with the specified row count. */
  private byte[] createArrowBytes(int rowCount) {
    try (BufferAllocator allocator = new RootAllocator();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IntVector intVector = new IntVector("col_0", allocator)) {

      intVector.allocateNew(rowCount);
      for (int i = 0; i < rowCount; i++) {
        intVector.set(i, i * 100);
      }
      intVector.setValueCount(rowCount);

      try (VectorSchemaRoot root = VectorSchemaRoot.of(intVector);
          ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
        writer.start();
        root.setRowCount(rowCount);
        writer.writeBatch();
        writer.end();
      }

      return out.toByteArray();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create Arrow data", e);
    }
  }

  /** Creates a TTableSchema with a single INT column. */
  private TTableSchema createTableSchema() {
    List<TColumnDesc> columns = new ArrayList<>();
    TPrimitiveTypeEntry primitiveType = new TPrimitiveTypeEntry().setType(TTypeId.INT_TYPE);
    TTypeEntry typeEntry = new TTypeEntry();
    typeEntry.setPrimitiveEntry(primitiveType);
    TTypeDesc typeDesc = new TTypeDesc().setTypes(Collections.singletonList(typeEntry));
    TColumnDesc columnDesc =
        new TColumnDesc().setColumnName("col_0").setTypeDesc(typeDesc).setPosition(0);
    columns.add(columnDesc);
    return new TTableSchema().setColumns(columns);
  }
}
