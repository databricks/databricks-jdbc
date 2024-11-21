package com.databricks.jdbc.exception;

/** Top level exception for Databricks driver */
public class DatabricksValidationException extends DatabricksSQLException {

  public DatabricksValidationException(String reason) {
    // TODO: Add vendor-specific (databricks) exception code
    super(reason, 0);
  }
}
