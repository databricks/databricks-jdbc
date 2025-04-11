package com.databricks.jdbc.exception;

import static com.databricks.jdbc.telemetry.TelemetryHelper.exportFailureLog;

import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;

/** Top level exception for Databricks driver */
public class DatabricksDriverException extends RuntimeException {
  public DatabricksDriverException(String reason, DatabricksDriverErrorCode internalError) {
    this(reason, internalError.name());
  }

  public DatabricksDriverException(
      String reason, Throwable cause, DatabricksDriverErrorCode internalError) {
    this(reason, cause, internalError.toString());
  }

  public DatabricksDriverException(String reason, Throwable cause, String sqlState) {
    super(reason, cause);
    exportFailureLog(DatabricksThreadContextHolder.getConnectionContext(), sqlState, reason);
  }

  public DatabricksDriverException(String reason, String sqlState) {
    super(reason);
    exportFailureLog(DatabricksThreadContextHolder.getConnectionContext(), sqlState, reason);
  }
}
