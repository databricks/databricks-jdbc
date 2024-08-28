package com.databricks.jdbc.exception;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.ErrorTypes;
import java.sql.SQLTimeoutException;

/** Top level exception for Databricks driver */
public class DatabricksTimeoutException extends SQLTimeoutException {
  public DatabricksTimeoutException(String message) {
    super(message);
  }

  public DatabricksTimeoutException(
      String reason,
      Throwable cause,
      IDatabricksConnectionContext connectionContext,
      String sqlQueryId,
      int errorCode) {
    super(reason, cause);
    connectionContext
        .getMetricsExporter()
        .exportError(ErrorTypes.TIMEOUT_ERROR, sqlQueryId, errorCode);
  }
}
