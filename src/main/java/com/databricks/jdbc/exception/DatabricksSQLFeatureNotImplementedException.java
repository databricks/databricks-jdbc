package com.databricks.jdbc.exception;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;

public class DatabricksSQLFeatureNotImplementedException extends DatabricksSQLException {

  public DatabricksSQLFeatureNotImplementedException(
      String reason, IDatabricksConnectionContext connectionContext) {
    super(reason, DatabricksDriverErrorCode.NOT_IMPLEMENTED_OPERATION, connectionContext);
  }
}
