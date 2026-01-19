package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.impl.arrow.ArrowStreamResult;
import com.databricks.jdbc.api.impl.arrow.LazyThriftInlineArrowResult;
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
import java.sql.SQLException;
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
    IExecutionResult result =
        ExecutionResultFactory.getResultSet(fetchResultsResp, session, parentStatement);
    assertInstanceOf(LazyThriftInlineArrowResult.class, result);
  }
}
