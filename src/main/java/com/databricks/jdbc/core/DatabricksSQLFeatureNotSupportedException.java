package com.databricks.jdbc.core;

public class DatabricksSQLFeatureNotSupportedException extends DatabricksSQLException {
  String featureName;

  public DatabricksSQLFeatureNotSupportedException(String reason) {
    // TODO: Add proper error code
    super(reason);
    this.featureName = "";
  }

  public DatabricksSQLFeatureNotSupportedException(String reason, String featureName) {
    // TODO: Add proper error code
    super(reason);
    this.featureName = featureName;
  }
}
