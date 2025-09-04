package com.databricks.jdbc.exception;

import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.sql.SQLFeatureNotSupportedException;

public class DatabricksSQLFeatureNotSupportedException extends SQLFeatureNotSupportedException {

  public DatabricksSQLFeatureNotSupportedException(String reason) {
    super(reason, DatabricksDriverErrorCode.UNSUPPORTED_OPERATION.toString());
  }
}
