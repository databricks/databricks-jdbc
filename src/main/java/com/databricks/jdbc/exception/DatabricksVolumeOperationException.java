package com.databricks.jdbc.exception;

import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;

/** Exception class to handle volume operation errors. */
public class DatabricksVolumeOperationException extends DatabricksSQLException {

  public DatabricksVolumeOperationException(
      String message, Throwable cause, DatabricksDriverErrorCode internalErrorCode) {
    super(message, cause, internalErrorCode);
  }

  public DatabricksVolumeOperationException(
      String message, DatabricksDriverErrorCode internalErrorCode) {
    super(message, internalErrorCode);
  }
}
