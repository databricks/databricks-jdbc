package com.databricks.jdbc.exception;

import static com.databricks.jdbc.telemetry.TelemetryHelper.exportFailureLog;

import com.databricks.jdbc.common.util.DatabricksConnectionContextHolder;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.sql.SQLException;

/** Top level exception for Databricks driver */
public class DatabricksSQLException extends SQLException {
  public DatabricksSQLException(String reason, DatabricksDriverErrorCode internalError) {
    this(reason, null, internalError);
  }

  public DatabricksSQLException(
      String reason, Throwable cause, DatabricksDriverErrorCode internalError) {
    this(reason, cause, internalError.toString());
  }

  public DatabricksSQLException(String reason, Throwable cause, String sqlState) {
    super(reason, sqlState, cause);
    exportFailureLog(
        DatabricksConnectionContextHolder.getConnectionContext(),
        DatabricksDriverErrorCode.CONNECTION_ERROR.name(),
        reason);
  }

  public DatabricksSQLException(String reason, String sqlState) {
    super(reason, sqlState);
    exportFailureLog(DatabricksConnectionContextHolder.getConnectionContext(), sqlState, reason);
  }
}
