package com.databricks.jdbc.exception;

import static com.databricks.jdbc.telemetry.TelemetryHelper.exportFailureLog;

import com.databricks.jdbc.common.TelemetryLogLevel;
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
    this(reason, cause, sqlState, false);
  }

  public DatabricksDriverException(
      String reason,
      Throwable cause,
      DatabricksDriverErrorCode internalError,
      boolean silentExceptions) {
    this(reason, cause, internalError.toString(), silentExceptions);
  }

  private DatabricksDriverException(
      String reason, Throwable cause, String sqlState, boolean silentExceptions) {
    super(reason, cause);
    if (!silentExceptions) {
      exportFailureLog(
          DatabricksThreadContextHolder.getConnectionContext(),
          sqlState,
          reason,
          TelemetryLogLevel.ERROR);
    }
  }

  public DatabricksDriverException(String reason, String sqlState) {
    super(reason);
    exportFailureLog(
        DatabricksThreadContextHolder.getConnectionContext(),
        sqlState,
        reason,
        TelemetryLogLevel.ERROR);
  }
}
