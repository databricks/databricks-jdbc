package com.databricks.jdbc.telemetry;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksHttpException;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksMetricsTest {
  @Mock IDatabricksConnectionContext connectionContext;
  @Mock IDatabricksComputeResource computeResource;
  @Mock private DatabricksHttpClient httpClient;
  @Mock private CloseableHttpResponse httpResponse;
  @Mock StatusLine statusLine;

  @BeforeEach
  void setUp() throws DatabricksHttpException {
    when(connectionContext.enableTelemetry()).thenReturn(true);
    when(connectionContext.getComputeResource()).thenReturn(computeResource);
    when(computeResource.getWorkspaceId()).thenReturn("workspaceId");
    when(httpResponse.getStatusLine()).thenReturn(statusLine);
    when(statusLine.getStatusCode()).thenReturn(200);
    when(httpClient.executeWithoutCertVerification(any(HttpPost.class))).thenReturn(httpResponse);
  }

  @Test
  void testExportRuntimeMetrics() throws DatabricksHttpException {
    runTestWithMockedClient(
        () -> {
          DatabricksMetrics metricsExporter = new DatabricksMetrics(connectionContext);
          metricsExporter.record("metricName", 1.0);
          metricsExporter.increment("metricName", 1.0);
        });

    verify(httpClient, atLeast(2)).executeWithoutCertVerification(any(HttpPost.class));
  }

  @Test
  void testExportUsageMetrics() throws DatabricksHttpException {
    runTestWithMockedClient(
        () -> {
          DatabricksMetrics metricsExporter = new DatabricksMetrics(connectionContext);
          metricsExporter.exportUsageMetrics(
              "jvmName",
              "jvmSpecVersion",
              "jvmImplVersion",
              "jvmVendor",
              "osName",
              "osVersion",
              "osArch",
              "localeName",
              "charsetEncoding");
        });

    verify(httpClient, atLeast(1))
        .executeWithoutCertVerification(
            argThat(request -> request != null && request.getFirstHeader("workspace_id") != null));
  }

  @Test
  void testExportErrorLogs() throws DatabricksHttpException {
    runTestWithMockedClient(
        () -> {
          DatabricksMetrics metricsExporter = new DatabricksMetrics(connectionContext);
          metricsExporter.exportError("errorName", "statementId", 100);
        });

    verify(httpClient, atLeast(1))
        .executeWithoutCertVerification(
            argThat(request -> request != null && request.getFirstHeader("workspace_id") != null));
  }

  private void runTestWithMockedClient(Runnable testLogic) {
    try (MockedStatic<DatabricksHttpClient> mockedStatic = mockStatic(DatabricksHttpClient.class)) {
      mockedStatic
          .when(() -> DatabricksHttpClient.getInstance(any(IDatabricksConnectionContext.class)))
          .thenReturn(httpClient);

      assertDoesNotThrow(
          () -> {
            try (DatabricksMetrics ignored = new DatabricksMetrics(connectionContext)) {
              testLogic.run();
            }
          });
    }
  }
}
