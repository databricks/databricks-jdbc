package com.databricks.jdbc.exception;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.ErrorTypes;
import com.databricks.jdbc.telemetry.DatabricksMetrics;

public class DatabricksSQLFeatureNotSupportedException extends DatabricksSQLException {
  String featureName;

  private void exportError(
      IDatabricksConnectionContext connectionContext,
      String errorName,
      String sqlQueryId,
      int errorCode) {
    DatabricksMetrics metricsExporter = connectionContext.getMetricsExporter();
    if (metricsExporter != null) {
      metricsExporter.exportError(errorName, sqlQueryId, errorCode);
    }
  }

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
    exportError(connectionContext, ErrorTypes.FEATURE_NOT_SUPPORTED, sqlQueryId, errorCode);
  }

  public DatabricksSQLFeatureNotSupportedException(
      String reason,
      String featureName,
      IDatabricksConnectionContext connectionContext,
      String sqlQueryId,
      int errorCode) {
    this(reason, connectionContext, sqlQueryId, errorCode);
    this.featureName = featureName;
  }
}
