package com.databricks.jdbc.exception;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;

/** Exception class to handle volume operation errors. */
public class DatabricksVolumeOperationException extends DatabricksSQLException {

  public DatabricksVolumeOperationException(
      String message,
      Throwable cause,
      DatabricksDriverErrorCode internalErrorCode,
      IDatabricksConnectionContext connectionContext) {
    super(message, cause, internalErrorCode, connectionContext);
  }

  public DatabricksVolumeOperationException(
      String message,
      DatabricksDriverErrorCode internalErrorCode,
      IDatabricksConnectionContext connectionContext) {
    super(message, internalErrorCode, connectionContext);
  }
}
