package com.databricks.jdbc.core;

import static com.databricks.jdbc.TestConstants.TEST_STATEMENT_ID;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.client.impl.thrift.generated.TGetResultSetMetadataResp;
import com.databricks.jdbc.client.impl.thrift.generated.TRowSet;
import com.databricks.jdbc.client.impl.thrift.generated.TSparkRowSetType;
import com.databricks.jdbc.client.sqlexec.ResultData;
import com.databricks.jdbc.client.sqlexec.ResultManifest;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.service.sql.Format;
import com.databricks.sdk.service.sql.ResultSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ExecutionResultFactoryTest {

  @Mock DatabricksSession session;
  @Mock IDatabricksConnectionContext connectionContext;

  @Mock TGetResultSetMetadataResp resultSetMetadataResp;
  @Mock TRowSet tRowSet;
  @Mock IDatabricksConnectionContext context;

  @Test
  public void testGetResultSet_jsonInline() {
    ResultManifest manifest = new ResultManifest();
    manifest.setFormat(Format.JSON_ARRAY);
    ResultData data = new ResultData();
    IExecutionResult result =
        ExecutionResultFactory.getResultSet(data, manifest, "statementId", session);

    assertInstanceOf(InlineJsonResult.class, result);
  }

  @Test
  public void testGetResultSet_externalLink() {
    when(session.getConnectionContext()).thenReturn(connectionContext);
    when(session.getConnectionContext().getCloudFetchThreadPoolSize()).thenReturn(16);
    ResultManifest manifest = new ResultManifest();
    manifest.setFormat(Format.ARROW_STREAM);
    manifest.setTotalChunkCount(0L);
    manifest.setSchema(new ResultSchema().setColumnCount(0L));
    ResultData data = new ResultData();
    IExecutionResult result =
        ExecutionResultFactory.getResultSet(data, manifest, "statementId", session);

    assertInstanceOf(ArrowStreamResult.class, result);
  }

  @Test
  public void testGetResultSet_volumeOperation() {
    when(session.getConnectionContext()).thenReturn(connectionContext);

    ResultData data = new ResultData();
    ResultManifest manifest = new ResultManifest().setIsVolumeOperation(true);
    IExecutionResult result =
        ExecutionResultFactory.getResultSet(data, manifest, "statementId", session);

    assertInstanceOf(VolumeOperationResult.class, result);
  }

  @Test
  public void testGetResultSet_thriftColumnar() throws DatabricksSQLException {
    when(resultSetMetadataResp.getResultFormat()).thenReturn(TSparkRowSetType.COLUMN_BASED_SET);
    IExecutionResult result =
        ExecutionResultFactory.getResultSet(
            tRowSet, resultSetMetadataResp, TEST_STATEMENT_ID, session);
    assertInstanceOf(InlineJsonResult.class, result);
  }

  @Test
  public void testGetResultSet_thriftRow() {
    when(resultSetMetadataResp.getResultFormat()).thenReturn(TSparkRowSetType.ROW_BASED_SET);
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () ->
            ExecutionResultFactory.getResultSet(
                tRowSet, resultSetMetadataResp, TEST_STATEMENT_ID, session));
  }

  @Test
  public void testGetResultSet_thriftURL() throws DatabricksSQLException {
    when(resultSetMetadataResp.getResultFormat()).thenReturn(TSparkRowSetType.URL_BASED_SET);
    when(session.getConnectionContext()).thenReturn(context);
    when(session.getConnectionContext().getCloudFetchThreadPoolSize()).thenReturn(16);
    IExecutionResult result =
        ExecutionResultFactory.getResultSet(
            tRowSet, resultSetMetadataResp, TEST_STATEMENT_ID, session);
    assertInstanceOf(ArrowStreamResult.class, result);
  }

  @Test
  public void testGetResultSet_thriftInlineArrow() throws DatabricksSQLException {
    when(resultSetMetadataResp.getResultFormat()).thenReturn(TSparkRowSetType.ARROW_BASED_SET);
    IExecutionResult result =
        ExecutionResultFactory.getResultSet(
            tRowSet, resultSetMetadataResp, TEST_STATEMENT_ID, session);
    assertInstanceOf(ArrowStreamResult.class, result);
  }
}
