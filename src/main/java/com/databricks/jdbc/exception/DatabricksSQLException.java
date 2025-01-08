package com.databricks.jdbc.exception;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.sql.SQLException;

import static com.databricks.jdbc.telemetry.TelemetryHelper.exportFailureLog;

/** Top level exception for Databricks driver */
public class DatabricksSQLException extends SQLException {
  public DatabricksSQLException(String reason, DatabricksDriverErrorCode internalError, IDatabricksConnectionContext connectionContext) {
    this(reason, null, internalError, connectionContext);
  }

  public DatabricksSQLException(
      String reason, Throwable cause, DatabricksDriverErrorCode internalError, IDatabricksConnectionContext connectionContext) {
    this(reason, cause, internalError.toString(), connectionContext);
  }

  public DatabricksSQLException(
          String reason, Throwable cause, String sqlState, IDatabricksConnectionContext connectionContext) {
    super(reason, sqlState, cause);
    exportFailureLog(connectionContext, DatabricksDriverErrorCode.CONNECTION_ERROR.name(), reason);
  }
  public DatabricksSQLException(
          String reason, String sqlState, IDatabricksConnectionContext connectionContext) {
    super(reason, sqlState);
    exportFailureLog(connectionContext, sqlState, reason);
  }
}
