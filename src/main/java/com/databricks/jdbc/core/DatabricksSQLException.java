package com.databricks.jdbc.core;

import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.jdbc.telemetry.DatabricksMetrics;
import java.sql.SQLException;

/** Top level exception for Databricks driver */
public class DatabricksSQLException extends SQLException {
  private void exportError(
      IDatabricksConnectionContext connectionContext,
      String errorName,
      String sqlQueryId,
      int errorCode) {
    DatabricksMetrics metricsExporter = connectionContext.getMetricsExporter();
    if (metricsExporter != null) {
      metricsExporter.exportError(errorName, sqlQueryId, errorCode);
    }
  }

  public DatabricksSQLException(String reason, String sqlState, int vendorCode) {
    super(reason, sqlState, vendorCode);
  }

  public DatabricksSQLException(String reason) {
    // TODO: Add proper error code
    super(reason, null, 0);
  }

  public DatabricksSQLException(String reason, int vendorCode) {
    super(reason, null, vendorCode);
  }

  public DatabricksSQLException(
      String reason,
      IDatabricksConnectionContext connectionContext,
      String errorName,
      String sqlQueryId,
      int errorCode) {
    super(reason, null, errorCode);
    exportError(connectionContext, errorName, sqlQueryId, errorCode);
  }

  public DatabricksSQLException(
      String reason,
      Throwable cause,
      IDatabricksConnectionContext connectionContext,
      String errorName,
      String sqlQueryId,
      int errorCode) {
    super(reason, sqlQueryId, errorCode, cause);
    exportError(connectionContext, errorName, sqlQueryId, errorCode);
  }

  public DatabricksSQLException(String reason, Throwable cause) {
    super(reason, cause);
  }
}
