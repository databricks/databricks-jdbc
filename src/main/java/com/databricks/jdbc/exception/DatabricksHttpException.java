package com.databricks.jdbc.exception;

import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;

/** Exception class to handle http errors while downloading chunk data from external links. */
public class DatabricksHttpException extends DatabricksSQLException {

  public DatabricksHttpException(
      String message, Throwable cause, DatabricksDriverErrorCode sqlCode) {
    // Keep the driver error code and the original cause for telemetry and callers.
    super(message, sqlCode.name(), sqlCode.getCode(), cause);
  }

  public DatabricksHttpException(String message, DatabricksDriverErrorCode internalCode) {
    super(message, null, internalCode.toString());
  }

  public DatabricksHttpException(
      String message, String sqlState, DatabricksDriverErrorCode internalCode) {
    super(message, sqlState, internalCode);
  }

  public DatabricksHttpException(String message, String sqlState) {
    super(message, null, sqlState);
  }

  public DatabricksHttpException(String message, Throwable throwable, String sqlState) {
    super(message, throwable, sqlState);
  }
}
