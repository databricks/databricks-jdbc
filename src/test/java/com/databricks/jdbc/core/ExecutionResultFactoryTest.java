package com.databricks.jdbc.core;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.client.sqlexec.ExternalLink;
import com.databricks.jdbc.client.sqlexec.ResultData;
import com.databricks.jdbc.client.sqlexec.ResultManifest;
import com.databricks.jdbc.client.sqlexec.VolumeOperationInfo;
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

    ResultManifest manifest = new ResultManifest();
    manifest.setIsVolumeOperation(true);
    ResultData data = new ResultData();
    data.setVolumeOperationInfo(
        new VolumeOperationInfo()
            .setVolumeOperationType("INVALID")
            .setPresignedUrl(new ExternalLink().setExternalLink("url")));
    IExecutionResult result =
        ExecutionResultFactory.getResultSet(data, manifest, "statementId", session);

    assertInstanceOf(VolumeOperationResult.class, result);
  }
}
