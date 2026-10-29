package com.databricks.jdbc.exception;

import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.sql.SQLFeatureNotSupportedException;

public class DatabricksSQLFeatureNotSupportedException extends SQLFeatureNotSupportedException {

  public DatabricksSQLFeatureNotSupportedException(String reason) {
    super(reason, DatabricksDriverErrorCode.UNSUPPORTED_OPERATION.toString());
  }

  /**
   * Creates an exception whose SQLState is the error code's name and whose vendor error code is the
   * error code's numeric value.
   */
  public DatabricksSQLFeatureNotSupportedException(
      String reason, DatabricksDriverErrorCode errorCode) {
    super(reason, errorCode.name(), errorCode.getCode());
  }
}
