package com.databricks.jdbc.exception;

import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;

public class DatabricksParsingException extends DatabricksSQLException {

  public DatabricksParsingException(String message, DatabricksDriverErrorCode errorCode) {
    super(message, errorCode);
  }

  public DatabricksParsingException(
      String message, Throwable cause, DatabricksDriverErrorCode errorCode) {
    super(message, cause, errorCode);
  }

  public DatabricksParsingException(String message, Throwable cause, String internalErrorCode) {
    super(message, cause, internalErrorCode);
  }
}
