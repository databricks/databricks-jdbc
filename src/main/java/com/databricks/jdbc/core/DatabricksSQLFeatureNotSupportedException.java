package com.databricks.jdbc.core;

import java.sql.SQLFeatureNotSupportedException;

public class DatabricksSQLFeatureNotSupportedException extends SQLFeatureNotSupportedException {
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
