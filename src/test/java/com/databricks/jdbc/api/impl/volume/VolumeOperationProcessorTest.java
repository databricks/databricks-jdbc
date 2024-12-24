package com.databricks.jdbc.api.impl.volume;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.common.util.VolumeUtil;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksHttpException;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.InputStreamEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class VolumeOperationProcessorTest {

  private static final String PRE_SIGNED_URL = "http://example.com/upload";

  @Mock private DatabricksHttpClient databricksHttpClient;
  @Mock private CloseableHttpResponse mockStream;
  @Mock private InputStreamEntity mockInputStreamEntity;
  @Mock private StatusLine mockStatusLine;

  @Test
  void testExecuteGetOperationStream_UnsuccessfulHttpResponse() throws Exception {
    VolumeOperationProcessor volumeOperationProcessor =
        VolumeOperationProcessor.Builder.createBuilder()
            .operationUrl(PRE_SIGNED_URL)
            .isAllowedInputStreamForVolumeOperation(true)
            .databricksHttpClient(databricksHttpClient)
            .getStreamReceiver((entity) -> {})
            .build();

    when(databricksHttpClient.execute(any())).thenReturn(mockStream);
    when(mockStream.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(400);
    volumeOperationProcessor.executeGetOperation();

    assertEquals(volumeOperationProcessor.getStatus(), VolumeUtil.VolumeOperationStatus.FAILED);
  }

  @Test
  void testExecuteGetOperationStream_HttpException() throws Exception {
    VolumeOperationProcessor volumeOperationProcessor =
        VolumeOperationProcessor.Builder.createBuilder()
            .operationUrl(PRE_SIGNED_URL)
            .isAllowedInputStreamForVolumeOperation(true)
            .databricksHttpClient(databricksHttpClient)
            .getStreamReceiver((entity) -> {})
            .build();

    DatabricksHttpException mockException = new DatabricksHttpException("Test Exeception");
    doThrow(mockException).when(databricksHttpClient).execute(any());

    volumeOperationProcessor.executeGetOperation();
    assertEquals(volumeOperationProcessor.getStatus(), VolumeUtil.VolumeOperationStatus.FAILED);
  }

  @Test
  void testExecutePutOperationStream_HttpException() throws Exception {
    VolumeOperationProcessor volumeOperationProcessor =
        VolumeOperationProcessor.Builder.createBuilder()
            .operationUrl(PRE_SIGNED_URL)
            .isAllowedInputStreamForVolumeOperation(true)
            .inputStream(mockInputStreamEntity)
            .databricksHttpClient(databricksHttpClient)
            .build();

    DatabricksHttpException mockException = new DatabricksHttpException("Test Exeception");
    doThrow(mockException).when(databricksHttpClient).execute(any());

    volumeOperationProcessor.executePutOperation();
    assertEquals(volumeOperationProcessor.getStatus(), VolumeUtil.VolumeOperationStatus.FAILED);
  }
}
