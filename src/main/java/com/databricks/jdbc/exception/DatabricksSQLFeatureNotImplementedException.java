package com.databricks.jdbc.exception;

import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;

public class DatabricksSQLFeatureNotImplementedException extends DatabricksSQLException {

  public DatabricksSQLFeatureNotImplementedException(String reason) {
    super(reason, DatabricksDriverErrorCode.NOT_IMPLEMENTED_OPERATION);
  }
}
