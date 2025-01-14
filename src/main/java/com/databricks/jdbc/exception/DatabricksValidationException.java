package com.databricks.jdbc.exception;

import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;

/** Top level exception for Databricks driver */
public class DatabricksValidationException extends DatabricksSQLException {

  public DatabricksValidationException(String reason, Throwable e) {
    super(reason, e, DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR);
  }

  public DatabricksValidationException(String reason) {
    super(reason, DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR);
  }
}
