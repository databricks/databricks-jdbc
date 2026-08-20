package com.databricks.jdbc.exception;

import com.databricks.jdbc.common.TelemetryLogLevel;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.jdbc.telemetry.TelemetryHelper;
import java.sql.SQLFeatureNotSupportedException;

public class DatabricksSQLFeatureNotImplementedException extends SQLFeatureNotSupportedException {

  public DatabricksSQLFeatureNotImplementedException(String reason) {
    super(reason, DatabricksDriverErrorCode.NOT_IMPLEMENTED_OPERATION.name());
    TelemetryHelper.exportFailureLog(
        DatabricksThreadContextHolder.getConnectionContext(),
        DatabricksDriverErrorCode.NOT_IMPLEMENTED_OPERATION.name(),
        reason,
        TelemetryLogLevel.ERROR);
  }
}
