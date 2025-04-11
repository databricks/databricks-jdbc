package com.databricks.jdbc.exception;

import static com.databricks.jdbc.telemetry.TelemetryHelper.exportFailureLog;

import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;

/** Top level exception for Databricks driver */
public class DatabricksSQLRuntimeException extends RuntimeException {
  public DatabricksSQLRuntimeException(String reason, DatabricksDriverErrorCode internalError) {
    this(reason, internalError.name());
  }

  public DatabricksSQLRuntimeException(
      String reason, Throwable cause, DatabricksDriverErrorCode internalError) {
    this(reason, cause, internalError.toString());
  }

  public DatabricksSQLRuntimeException(String reason, Throwable cause, String sqlState) {
    super(reason, cause);
    exportFailureLog(DatabricksThreadContextHolder.getConnectionContext(), sqlState, reason);
  }

  public DatabricksSQLRuntimeException(String reason, String sqlState) {
    super(reason);
    exportFailureLog(DatabricksThreadContextHolder.getConnectionContext(), sqlState, reason);
  }
}
