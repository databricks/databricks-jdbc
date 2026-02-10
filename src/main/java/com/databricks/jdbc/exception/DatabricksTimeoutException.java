package com.databricks.jdbc.exception;

import com.databricks.jdbc.common.TelemetryLogLevel;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.jdbc.telemetry.TelemetryHelper;
import java.sql.SQLTimeoutException;

/** Top level exception for Databricks driver */
public class DatabricksTimeoutException extends SQLTimeoutException {

  public DatabricksTimeoutException(
      String reason, Throwable cause, DatabricksDriverErrorCode internalError) {
    super(reason, internalError.toString(), cause);
    TelemetryHelper.exportFailureLog(
        DatabricksThreadContextHolder.getConnectionContext(),
        internalError.name(),
        reason,
        TelemetryLogLevel.ERROR);
  }

  public DatabricksTimeoutException(
      String reason, String sqlState, Throwable cause, DatabricksDriverErrorCode internalError) {
    super(reason, sqlState, cause);
    TelemetryHelper.exportFailureLog(
        DatabricksThreadContextHolder.getConnectionContext(),
        internalError.name(),
        reason,
        TelemetryLogLevel.ERROR);
  }
}
