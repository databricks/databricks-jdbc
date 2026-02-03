package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.impl.arrow.ArrowStreamResult;
import com.databricks.jdbc.api.impl.arrow.LazyThriftInlineArrowResult;
import com.databricks.jdbc.api.impl.arrow.StreamingInlineArrowResult;
import com.databricks.jdbc.api.impl.thrift.StreamingColumnarResult;
import com.databricks.jdbc.api.impl.volume.VolumeOperationResult;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksSQLFeatureNotSupportedException;
import com.databricks.jdbc.model.client.thrift.generated.*;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.core.ResultSchema;
import com.databricks.sdk.service.sql.Format;
import java.io.ByteArrayOutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ExecutionResultFactoryTest {

  private static final StatementId STATEMENT_ID = new StatementId("statementId");
  @Mock DatabricksSession session;
  @Mock IDatabricksStatementInternal parentStatement;
  @Mock IDatabricksConnectionContext connectionContext;
  @Mock TGetResultSetMetadataResp resultSetMetadataResp;
  @Mock TRowSet tRowSet;
  @Mock TFetchResultsResp fetchResultsResp;

  @Test
  public void testGetResultSet_jsonInline() throws DatabricksSQLException {
    ResultManifest manifest = new ResultManifest();
    manifest.setFormat(Format.JSON_ARRAY);
    ResultData data = new ResultData();
    IExecutionResult result =
        ExecutionResultFactory.getResultSet(data, manifest, STATEMENT_ID, session, parentStatement);

    assertInstanceOf(InlineJsonResult.class, result);
  }

  @Test
  public void testGetResultSet_externalLink() throws DatabricksSQLException {
    when(connectionContext.getConnectionUuid()).thenReturn("sample-uuid");
    when(connectionContext.getHttpMaxConnectionsPerRoute()).thenReturn(100);
    when(session.getConnectionContext()).thenReturn(connectionContext);
    when(session.getConnectionContext().getCloudFetchThreadPoolSize()).thenReturn(16);
    ResultManifest manifest = new ResultManifest();
    manifest.setFormat(Format.ARROW_STREAM);
    manifest.setTotalChunkCount(0L);
    manifest.setTotalRowCount(0L);
    manifest.setSchema(new ResultSchema().setColumnCount(0L));
    ResultData data = new ResultData();
    IExecutionResult result =
        ExecutionResultFactory.getResultSet(data, manifest, STATEMENT_ID, session, parentStatement);

    assertInstanceOf(ArrowStreamResult.class, result);
  }

  @Test
  public void testGetResultSet_volumeOperation() throws DatabricksSQLException {
    when(connectionContext.getConnectionUuid()).thenReturn("sample-uuid");
    when(session.getConnectionContext()).thenReturn(connectionContext);
    ResultData data = new ResultData();
    ResultManifest manifest =
        new ResultManifest()
            .setIsVolumeOperation(true)
            .setFormat(Format.JSON_ARRAY)
            .setTotalRowCount(1L)
            .setSchema(new ResultSchema().setColumnCount(4L));
    IExecutionResult result =
        ExecutionResultFactory.getResultSet(data, manifest, STATEMENT_ID, session, parentStatement);

    assertInstanceOf(VolumeOperationResult.class, result);
  }

  @Test
  public void testGetResultSet_volumeOperationThriftResp() throws Exception {
    when(connectionContext.getConnectionUuid()).thenReturn("sample-uuid");
    when(connectionContext.getHttpMaxConnectionsPerRoute()).thenReturn(100);
    when(session.getConnectionContext()).thenReturn(connectionContext);
    when(fetchResultsResp.getResultSetMetadata()).thenReturn(resultSetMetadataResp);
    when(resultSetMetadataResp.getResultFormat()).thenReturn(TSparkRowSetType.COLUMN_BASED_SET);
    when(resultSetMetadataResp.isSetIsStagingOperation()).thenReturn(true);
    when(resultSetMetadataResp.isIsStagingOperation()).thenReturn(true);
    when(resultSetMetadataResp.getSchema()).thenReturn(new TTableSchema());
    IExecutionResult result =
        ExecutionResultFactory.getResultSet(fetchResultsResp, session, parentStatement);

    assertInstanceOf(VolumeOperationResult.class, result);
  }

  @Test
  public void testGetResultSet_thriftColumnar() throws SQLException {
    when(resultSetMetadataResp.getResultFormat()).thenReturn(TSparkRowSetType.COLUMN_BASED_SET);
    when(fetchResultsResp.getResultSetMetadata()).thenReturn(resultSetMetadataResp);
    when(session.getConnectionContext()).thenReturn(connectionContext);
    when(connectionContext.isInlineStreamingEnabled()).thenReturn(false);
    IExecutionResult result =
        ExecutionResultFactory.getResultSet(fetchResultsResp, session, parentStatement);
    assertInstanceOf(LazyThriftResult.class, result);
  }

  @Test
  public void testGetResultSet_thriftRow() {
    when(resultSetMetadataResp.getResultFormat()).thenReturn(TSparkRowSetType.ROW_BASED_SET);
    when(fetchResultsResp.getResultSetMetadata()).thenReturn(resultSetMetadataResp);
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> ExecutionResultFactory.getResultSet(fetchResultsResp, session, parentStatement));
  }

  @Test
  public void testGetResultSet_thriftURL() throws SQLException {
    when(connectionContext.getConnectionUuid()).thenReturn("sample-uuid");
    when(resultSetMetadataResp.getResultFormat()).thenReturn(TSparkRowSetType.URL_BASED_SET);
    when(fetchResultsResp.getResultSetMetadata()).thenReturn(resultSetMetadataResp);
    when(fetchResultsResp.getResults()).thenReturn(tRowSet);
    when(session.getConnectionContext()).thenReturn(connectionContext);
    when(session.getConnectionContext().getCloudFetchThreadPoolSize()).thenReturn(16);
    IExecutionResult result =
        ExecutionResultFactory.getResultSet(fetchResultsResp, session, parentStatement);
    assertInstanceOf(ArrowStreamResult.class, result);
  }

  @Test
  public void testGetResultSet_thriftInlineArrow() throws SQLException {
    when(resultSetMetadataResp.getResultFormat()).thenReturn(TSparkRowSetType.ARROW_BASED_SET);
    when(fetchResultsResp.getResultSetMetadata()).thenReturn(resultSetMetadataResp);
    when(fetchResultsResp.getResults()).thenReturn(tRowSet);
    when(session.getConnectionContext()).thenReturn(connectionContext);
    when(connectionContext.isInlineStreamingEnabled()).thenReturn(false);
    IExecutionResult result =
        ExecutionResultFactory.getResultSet(fetchResultsResp, session, parentStatement);
    assertInstanceOf(LazyThriftInlineArrowResult.class, result);
  }

  @Test
  public void testGetResultSet_thriftColumnarWithStreamingEnabled() throws SQLException {
    // Create real TFetchResultsResp with valid columnar data and hasMoreRows=false
    // so the prefetch thread exits immediately
    TFetchResultsResp realFetchResp = createColumnarFetchResponse();

    when(session.getConnectionContext()).thenReturn(connectionContext);
    when(connectionContext.isInlineStreamingEnabled()).thenReturn(true);
    when(connectionContext.getThriftMaxBatchesInMemory()).thenReturn(3);

    IExecutionResult result =
        ExecutionResultFactory.getResultSet(realFetchResp, session, parentStatement);

    assertInstanceOf(StreamingColumnarResult.class, result);
    result.close(); // Clean up prefetch thread
  }

  @Test
  public void testGetResultSet_thriftInlineArrowWithStreamingEnabled() throws SQLException {
    // Create real TFetchResultsResp with valid Arrow data and hasMoreRows=false
    // so the prefetch thread exits immediately
    TFetchResultsResp realFetchResp = createArrowFetchResponse();

    when(session.getConnectionContext()).thenReturn(connectionContext);
    when(connectionContext.isInlineStreamingEnabled()).thenReturn(true);
    when(connectionContext.getThriftMaxBatchesInMemory()).thenReturn(3);
    when(parentStatement.getStatementId()).thenReturn(STATEMENT_ID);

    IExecutionResult result =
        ExecutionResultFactory.getResultSet(realFetchResp, session, parentStatement);

    assertInstanceOf(StreamingInlineArrowResult.class, result);
    result.close(); // Clean up prefetch thread
  }

  // ==================== Helper Methods ====================

  /** Creates a valid TFetchResultsResp with columnar data for streaming tests. */
  private TFetchResultsResp createColumnarFetchResponse() {
    TFetchResultsResp response = new TFetchResultsResp();
    response.hasMoreRows = false; // Prefetch thread will exit immediately

    // Create metadata
    TGetResultSetMetadataResp metadata = new TGetResultSetMetadataResp();
    metadata.setResultFormat(TSparkRowSetType.COLUMN_BASED_SET);
    response.setResultSetMetadata(metadata);

    // Create columnar data with one row
    TRowSet rowSet = new TRowSet();
    TColumn column = new TColumn();
    TStringColumn stringCol = new TStringColumn();
    stringCol.setValues(Arrays.asList("test_value"));
    column.setStringVal(stringCol);
    rowSet.setColumns(Collections.singletonList(column));
    response.setResults(rowSet);

    return response;
  }

  /** Creates a valid TFetchResultsResp with Arrow data for streaming tests. */
  private TFetchResultsResp createArrowFetchResponse() {
    TFetchResultsResp response = new TFetchResultsResp();
    response.hasMoreRows = false; // Prefetch thread will exit immediately

    // Create Arrow batch data
    byte[] arrowData = createArrowBytes();

    // Create metadata with schema
    TGetResultSetMetadataResp metadata = new TGetResultSetMetadataResp();
    metadata.setResultFormat(TSparkRowSetType.ARROW_BASED_SET);
    metadata.setArrowSchema(new byte[0]); // Empty - schema is in the IPC stream
    metadata.setSchema(createTableSchema());
    response.setResultSetMetadata(metadata);

    // Create Arrow batch
    TSparkArrowBatch arrowBatch = new TSparkArrowBatch();
    arrowBatch.setRowCount(1);
    arrowBatch.setBatch(arrowData);

    TRowSet rowSet = new TRowSet();
    rowSet.setArrowBatches(Collections.singletonList(arrowBatch));
    response.setResults(rowSet);

    return response;
  }

  /** Creates valid Arrow IPC format bytes with one row. */
  private byte[] createArrowBytes() {
    try (BufferAllocator allocator = new RootAllocator();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IntVector intVector = new IntVector("col_0", allocator)) {

      intVector.allocateNew(1);
      intVector.set(0, 42);
      intVector.setValueCount(1);

      try (VectorSchemaRoot root = VectorSchemaRoot.of(intVector);
          ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
        writer.start();
        root.setRowCount(1);
        writer.writeBatch();
        writer.end();
      }

      return out.toByteArray();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create Arrow data", e);
    }
  }

  /** Creates a TTableSchema for Arrow result metadata. */
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
