package com.databricks.jdbc.telemetry;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.jdbc.core.types.ComputeResource;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksMetricsTest {
  @Mock IDatabricksConnectionContext connectionContext;
  @Mock ComputeResource computeResource;

  @Test
  void testExport() throws DatabricksSQLException {
    when(connectionContext.enableTelemetry()).thenReturn(true);
    when(connectionContext.getComputeResource()).thenReturn(computeResource);
    when(computeResource.getWorkspaceId()).thenReturn("workspaceId");

    assertDoesNotThrow(
        () -> {
          try (DatabricksMetrics metricsExporter = new DatabricksMetrics(connectionContext)) {
            metricsExporter.record("metricName", 1.0);
            metricsExporter.increment("metricName", 1.0);
          }
        });
  }
}
