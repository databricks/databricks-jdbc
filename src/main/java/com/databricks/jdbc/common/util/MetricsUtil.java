package com.databricks.jdbc.common.util;

import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.telemetry.DatabricksMetrics;

public class MetricsUtil {

  /** Exports error to Databricks using MetricsExporter */
  public static void exportError(
      IDatabricksSession session, String errorName, String sqlQueryId, int errCode) {
    DatabricksMetrics metricsExporter = session.getMetricsExporter();
    if (metricsExporter != null) {
      metricsExporter.exportError(errorName, sqlQueryId, errCode);
    }
  }

  public static void exportError(
      DatabricksMetrics metricsExporter, String errorName, String sqlQueryId, int errCode) {
    if (metricsExporter != null) {
      metricsExporter.exportError(errorName, sqlQueryId, errCode);
    }
  }

  /** Exports errors without auth. Useful where connection is not yet established */
  public static void exportErrorWithoutAuth(String errorName, String sqlQueryId, int errCode) {
    // TODO (PECO-1937): Implement this
  }
}
