package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.TestConstants.TEST_STATEMENT_ID;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.IDatabricksResultSet;
import com.databricks.jdbc.api.IDatabricksStatement;
import com.databricks.jdbc.api.impl.volume.VolumeOperationResult;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksSQLFeatureNotSupportedException;
import com.databricks.jdbc.model.client.thrift.generated.TGetResultSetMetadataResp;
import com.databricks.jdbc.model.client.thrift.generated.TRowSet;
import com.databricks.jdbc.model.client.thrift.generated.TSparkRowSetType;
import com.databricks.jdbc.model.client.thrift.generated.TTableSchema;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
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
  @Mock IDatabricksStatement statement;
  @Mock IDatabricksResultSet resultSet;

  @Test
  public void testGetResultSet_jsonInline() {
    ResultManifest manifest = new ResultManifest();
    manifest.setFormat(Format.JSON_ARRAY);
    ResultData data = new ResultData();
    IExecutionResult result =
        ExecutionResultFactory.getResultSet(
            data, manifest, "statementId", session, statement, resultSet);

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
        ExecutionResultFactory.getResultSet(
            data, manifest, "statementId", session, statement, resultSet);

    assertInstanceOf(ArrowStreamResult.class, result);
  }

  @Test
  public void testGetResultSet_volumeOperation() {
    when(session.getConnectionContext()).thenReturn(connectionContext);

    ResultData data = new ResultData();
    ResultManifest manifest =
        new ResultManifest()
            .setIsVolumeOperation(true)
            .setFormat(Format.JSON_ARRAY)
            .setTotalRowCount(1L)
            .setSchema(new ResultSchema().setColumnCount(4L));
    IExecutionResult result =
        ExecutionResultFactory.getResultSet(
            data, manifest, "statementId", session, statement, resultSet);

    assertInstanceOf(VolumeOperationResult.class, result);
  }

  @Test
  public void testGetResultSet_volumeOperationThriftResp() throws Exception {
    when(session.getConnectionContext()).thenReturn(connectionContext);
    when(resultSetMetadataResp.getResultFormat()).thenReturn(TSparkRowSetType.COLUMN_BASED_SET);
    when(resultSetMetadataResp.isSetIsStagingOperation()).thenReturn(true);
    when(resultSetMetadataResp.isIsStagingOperation()).thenReturn(true);
    when(resultSetMetadataResp.getSchema()).thenReturn(new TTableSchema());

    ResultData data = new ResultData();
    IExecutionResult result =
        ExecutionResultFactory.getResultSet(
            tRowSet, resultSetMetadataResp, "statementId", session, statement, resultSet);

    assertInstanceOf(VolumeOperationResult.class, result);
  }

  @Test
  public void testGetResultSet_thriftColumnar() throws DatabricksSQLException {
    when(resultSetMetadataResp.getResultFormat()).thenReturn(TSparkRowSetType.COLUMN_BASED_SET);
    IExecutionResult result =
        ExecutionResultFactory.getResultSet(
            tRowSet, resultSetMetadataResp, TEST_STATEMENT_ID, session, statement, resultSet);
    assertInstanceOf(InlineJsonResult.class, result);
  }

  @Test
  public void testGetResultSet_thriftRow() {
    when(resultSetMetadataResp.getResultFormat()).thenReturn(TSparkRowSetType.ROW_BASED_SET);
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () ->
            ExecutionResultFactory.getResultSet(
                tRowSet, resultSetMetadataResp, TEST_STATEMENT_ID, session, statement, resultSet));
  }

  @Test
  public void testGetResultSet_thriftURL() throws DatabricksSQLException {
    when(resultSetMetadataResp.getResultFormat()).thenReturn(TSparkRowSetType.URL_BASED_SET);
    when(session.getConnectionContext()).thenReturn(context);
    when(session.getConnectionContext().getCloudFetchThreadPoolSize()).thenReturn(16);
    IExecutionResult result =
        ExecutionResultFactory.getResultSet(
            tRowSet, resultSetMetadataResp, TEST_STATEMENT_ID, session, statement, resultSet);
    assertInstanceOf(ArrowStreamResult.class, result);
  }

  @Test
  public void testGetResultSet_thriftInlineArrow() throws DatabricksSQLException {
    when(resultSetMetadataResp.getResultFormat()).thenReturn(TSparkRowSetType.ARROW_BASED_SET);
    IExecutionResult result =
        ExecutionResultFactory.getResultSet(
            tRowSet, resultSetMetadataResp, TEST_STATEMENT_ID, session, statement, resultSet);
    assertInstanceOf(ArrowStreamResult.class, result);
  }
}
