package com.databricks.jdbc.core;

import com.databricks.jdbc.commons.ErrorTypes;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;

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

  public DatabricksSQLFeatureNotSupportedException(
      String reason,
      IDatabricksConnectionContext connectionContext,
      String sqlQueryId,
      int errorCode) {
    super(reason, null, errorCode);
    connectionContext
        .getMetricsExporter()
        .exportError(connectionContext, ErrorTypes.FEATURE_NOT_SUPPORTED, sqlQueryId, errorCode);
  }

  public DatabricksSQLFeatureNotSupportedException(
      String reason,
      String featureName,
      IDatabricksConnectionContext connectionContext,
      String sqlQueryId,
      int errorCode) {
    super(reason, null, errorCode);
    this.featureName = featureName;
    connectionContext
        .getMetricsExporter()
        .exportError(connectionContext, ErrorTypes.FEATURE_NOT_SUPPORTED, sqlQueryId, errorCode);
  }
}
