package com.databricks.jdbc.core;

import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import java.sql.SQLException;

/** Top level exception for Databricks driver */
public class DatabricksSQLException extends SQLException {

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
    connectionContext
        .getMetricsExporter()
        .exportError(connectionContext, errorName, sqlQueryId, errorCode);
  }

  public DatabricksSQLException(
      String reason,
      Throwable cause,
      IDatabricksConnectionContext connectionContext,
      String errorName,
      String sqlQueryId,
      int errorCode) {
    super(reason, cause);
    connectionContext
        .getMetricsExporter()
        .exportError(connectionContext, errorName, sqlQueryId, errorCode);
  }

  public DatabricksSQLException(String reason, Throwable cause) {
    super(reason, cause);
  }
}
