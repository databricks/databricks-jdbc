package com.databricks.jdbc.exception;

public class DatabricksSQLFeatureNotImplementedException extends DatabricksSQLException {

  public DatabricksSQLFeatureNotImplementedException(String reason) {
    super(reason);
  }
}
