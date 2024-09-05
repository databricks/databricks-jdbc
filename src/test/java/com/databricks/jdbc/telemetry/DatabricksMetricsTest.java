package com.databricks.jdbc.telemetry;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksMetricsTest {
  @Mock IDatabricksConnectionContext connectionContext;
  @Mock IDatabricksComputeResource computeResource;

  @Test
  void testExportMetrics() {
    when(connectionContext.enableTelemetry()).thenReturn(true);
    when(connectionContext.getComputeResource()).thenReturn(computeResource);
    when(computeResource.getWorkspaceId()).thenReturn("workspaceId");

    // Runtime metrics
    assertDoesNotThrow(
        () -> {
          try (DatabricksMetrics metricsExporter = new DatabricksMetrics(connectionContext)) {
            metricsExporter.record("metricName", 1.0);
            metricsExporter.increment("metricName", 1.0);
          }
        });

    // Usage metrics
    assertDoesNotThrow(
        () -> {
          try (DatabricksMetrics metricsExporter = new DatabricksMetrics(connectionContext)) {
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
          }
        });

    // Error logs
    assertDoesNotThrow(
        () -> {
          try (DatabricksMetrics metricsExporter = new DatabricksMetrics(connectionContext)) {
            metricsExporter.exportError("errorName", "statementId", 100);
          }
        });
  }
}
