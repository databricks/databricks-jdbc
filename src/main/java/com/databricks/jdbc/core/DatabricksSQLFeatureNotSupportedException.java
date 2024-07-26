package com.databricks.jdbc.core;

import com.databricks.jdbc.commons.ErrorTypes;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.jdbc.telemetry.DatabricksMetrics;

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
    DatabricksMetrics metricsExporter = connectionContext.getMetricsExporter();
    if (metricsExporter != null) {
      metricsExporter.exportError(ErrorTypes.FEATURE_NOT_SUPPORTED, sqlQueryId, errorCode);
    }
  }

  public DatabricksSQLFeatureNotSupportedException(
      String reason,
      String featureName,
      IDatabricksConnectionContext connectionContext,
      String sqlQueryId,
      int errorCode) {
    super(reason, null, errorCode);
    this.featureName = featureName;
    DatabricksMetrics metricsExporter = connectionContext.getMetricsExporter();
    if (metricsExporter != null) {
      metricsExporter.exportError(ErrorTypes.FEATURE_NOT_SUPPORTED, sqlQueryId, errorCode);
    }
  }
}
