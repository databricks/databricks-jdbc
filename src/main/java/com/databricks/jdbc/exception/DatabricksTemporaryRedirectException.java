package com.databricks.jdbc.exception;

import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;

public class DatabricksTemporaryRedirectException extends DatabricksSQLException {
  public DatabricksTemporaryRedirectException(DatabricksDriverErrorCode internalError) {
    super("Redirect to Thrift Client", internalError);
  }
}
