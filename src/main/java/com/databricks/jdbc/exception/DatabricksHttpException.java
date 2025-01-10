package com.databricks.jdbc.exception;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import org.apache.thrift.annotation.Nullable;

/** Exception class to handle http errors while downloading chunk data from external links. */
public class DatabricksHttpException extends DatabricksSQLException {

  public DatabricksHttpException(
      String message,
      Throwable cause,
      DatabricksDriverErrorCode sqlCode,
      IDatabricksConnectionContext connectionContext) {
    super(message, cause, sqlCode, connectionContext);
  }

  public DatabricksHttpException(
      String message,
      DatabricksDriverErrorCode internalCode,
      @Nullable IDatabricksConnectionContext connectionContext) {
    super(message, null, internalCode.toString(), connectionContext);
  }

  public DatabricksHttpException(
      String message, String sqlState, IDatabricksConnectionContext connectionContext) {
    super(message, null, sqlState, connectionContext);
  }

  public DatabricksHttpException(
      String message,
      Throwable throwable,
      String sqlState,
      IDatabricksConnectionContext connectionContext) {
    super(message, throwable, sqlState, connectionContext);
  }
}
