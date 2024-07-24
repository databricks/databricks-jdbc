package com.databricks.jdbc.core;

import com.databricks.jdbc.commons.ErrorTypes;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.jdbc.telemetry.DatabricksErrorLogging;
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
    DatabricksErrorLogging.exportError(
        connectionContext, ErrorTypes.TIMEOUT_ERROR, sqlQueryId, errorCode);
  }
}
