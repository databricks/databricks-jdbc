package com.databricks.jdbc.exception;

import static com.databricks.jdbc.telemetry.TelemetryHelper.exportFailureLog;

import com.databricks.jdbc.common.TelemetryLogLevel;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.sql.SQLException;

/** Top level exception for Databricks driver */
public class DatabricksSQLException extends SQLException {
  public DatabricksSQLException(String reason, DatabricksDriverErrorCode internalError) {
    this(reason, internalError.name());
  }

  public DatabricksSQLException(
      String reason, DatabricksDriverErrorCode internalError, boolean silentExceptions) {
    this(reason, internalError.name(), silentExceptions);
  }

  public DatabricksSQLException(
      String reason, Throwable cause, DatabricksDriverErrorCode internalError) {
    this(reason, cause, internalError.toString());
  }

  public DatabricksSQLException(String reason, Throwable cause, String sqlState) {
    this(reason, sqlState, DatabricksVendorCode.getVendorCode(cause), cause);
  }

  // Chunk-download path: routes statementId + chunkIndex into the telemetry record so failures
  // can be bucketed per chunk. Other constructors do not carry chunkIndex.
  public DatabricksSQLException(
      String reason, Throwable cause, String statementId, Long chunkIndex, String sqlState) {
    super(reason, sqlState, DatabricksVendorCode.getVendorCode(cause), cause);
    exportFailureLog(
        DatabricksThreadContextHolder.getConnectionContext(),
        sqlState,
        reason,
        statementId,
        chunkIndex,
        TelemetryLogLevel.ERROR);
  }

  public DatabricksSQLException(String reason, String sqlState) {
    this(reason, sqlState, false);
  }

  public DatabricksSQLException(String reason, String sqlState, boolean silentExceptions) {
    this(reason, sqlState, DatabricksVendorCode.getVendorCode(reason), silentExceptions);
  }

  public DatabricksSQLException(
      String reason, String sqlState, DatabricksDriverErrorCode internalError) {
    this(reason, sqlState, internalError, false);
  }

  public DatabricksSQLException(
      String reason,
      String sqlState,
      DatabricksDriverErrorCode internalError,
      boolean silentExceptions) {
    super(reason, sqlState, internalError.getCode());
    logTelemetryEvent(sqlState, reason, silentExceptions);
  }

  public DatabricksSQLException(String reason, String sqlState, int vendorCode) {
    this(reason, sqlState, vendorCode, false);
  }

  public DatabricksSQLException(
      String reason, String sqlState, int vendorCode, boolean silentExceptions) {
    super(reason, sqlState, vendorCode);
    logTelemetryEvent(sqlState, reason, silentExceptions);
  }

  public DatabricksSQLException(String reason, String sqlState, int vendorCode, Throwable cause) {
    super(reason, sqlState, vendorCode, cause);
    logTelemetryEvent(sqlState, reason, false);
  }

  private void logTelemetryEvent(String sqlState, String reason, boolean silentExceptions) {
    if (!silentExceptions) {
      exportFailureLog(
          DatabricksThreadContextHolder.getConnectionContext(),
          sqlState,
          reason,
          TelemetryLogLevel.ERROR);
    } else {
      // These are errors that are thrown to call a fallback method (e.g. metadata column not
      // present in SEA response). Use TRACE so they are filtered out by the default telemetry
      // log level (DEBUG) and only visible when telemetryLogLevel=TRACE is explicitly set.
      exportFailureLog(
          DatabricksThreadContextHolder.getConnectionContext(),
          sqlState,
          reason,
          TelemetryLogLevel.TRACE);
    }
  }
}
