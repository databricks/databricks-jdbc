package com.databricks.jdbc.exception;

public class DatabricksSQLFeatureNotImplementedException extends DatabricksSQLException {
  String featureName;

  public DatabricksSQLFeatureNotImplementedException(String reason) {
    // TODO: Add proper error code
    super(reason);
    this.featureName = "";
  }

  public DatabricksSQLFeatureNotImplementedException(String reason, String featureName) {
    // TODO: Add proper error code
    super(reason);
    this.featureName = featureName;
  }
}
