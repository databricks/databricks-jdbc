package com.databricks.jdbc.client;

import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import java.io.IOException;

public class DatabricksRetryHandlerException extends IOException {
  private int errCode = 0;

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
    connectionContext
        .getMetricsExporter()
        .exportError(connectionContext, errorName, sqlQueryId, errCode);
  }

  public int getErrCode() {
    return errCode;
  }
}
