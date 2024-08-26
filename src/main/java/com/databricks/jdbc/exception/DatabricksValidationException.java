package com.databricks.jdbc.exception;

/** Top level exception for Databricks driver */
public class DatabricksValidationException extends DatabricksSQLException {

  public DatabricksValidationException(String reason, String sqlState, int vendorCode) {
    super(reason, sqlState, vendorCode);
  }

  public DatabricksValidationException(String reason) {
    // TODO: Add proper error code
    super(reason, null, 0);
  }

  public DatabricksValidationException(String reason, Throwable cause) {
    super(reason, cause);
  }
}
