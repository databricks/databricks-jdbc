package com.databricks.jdbc.exception;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;

/** Top level exception for Databricks driver */
public class DatabricksValidationException extends DatabricksSQLException {

  public DatabricksValidationException(String reason, Throwable e, IDatabricksConnectionContext connectionContext) {
    super(reason, e, DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR, connectionContext);
  }
  public DatabricksValidationException(String reason, IDatabricksConnectionContext connectionContext) {
    super(reason, DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR, connectionContext);
  }
}
