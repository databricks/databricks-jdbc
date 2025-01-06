package com.databricks.jdbc.exception;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;

import java.sql.SQLFeatureNotSupportedException;

public class DatabricksSQLFeatureNotSupportedException extends SQLFeatureNotSupportedException {

  public DatabricksSQLFeatureNotSupportedException(String reason, IDatabricksConnectionContext connectionContext) {
    super(reason, DatabricksDriverErrorCode.UNSUPPORTED_OPERATION.toString());
  }
}
