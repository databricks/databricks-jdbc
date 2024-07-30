package com.databricks.jdbc.core;

import com.databricks.jdbc.commons.ErrorTypes;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
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
