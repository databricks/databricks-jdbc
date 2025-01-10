package com.databricks.jdbc.exception;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;

public class DatabricksParsingException extends DatabricksSQLException {

  public DatabricksParsingException(
      String message,
      DatabricksDriverErrorCode errorCode,
      IDatabricksConnectionContext connectionContext) {
    super(message, errorCode, connectionContext);
  }

  public DatabricksParsingException(
      String message,
      Throwable cause,
      DatabricksDriverErrorCode errorCode,
      IDatabricksConnectionContext connectionContext) {
    super(message, cause, errorCode, connectionContext);
  }

  public DatabricksParsingException(
      String message,
      Throwable cause,
      String internalErrorCode,
      IDatabricksConnectionContext connectionContext) {
    super(message, cause, internalErrorCode, connectionContext);
  }
}
