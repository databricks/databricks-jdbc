package com.databricks.jdbc.client;

import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.jdbc.telemetry.DatabricksMetrics;
import java.io.IOException;

public class DatabricksRetryHandlerException extends IOException {
  private int errCode = 0;

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

  public DatabricksRetryHandlerException(String message, int errCode) {
    super(message);
    this.errCode = errCode;
  }

  public DatabricksRetryHandlerException(
      String message,
      int errCode,
      IDatabricksConnectionContext connectionContext,
      String errorName,
      String sqlQueryId) {
    super(message);
    this.errCode = errCode;
    exportError(connectionContext, errorName, sqlQueryId, errCode);
  }

  public int getErrCode() {
    return errCode;
  }
}
