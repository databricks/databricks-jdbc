package com.databricks.jdbc.exception;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.sql.SQLException;

/** Top level exception for Databricks driver */
public class DatabricksSQLException extends SQLException {
  public DatabricksSQLException(String reason, DatabricksDriverErrorCode internalError, IDatabricksConnectionContext connectionContext) {
    this(reason, null, internalError, connectionContext);
  }

  public DatabricksSQLException(
      String reason, Throwable cause, DatabricksDriverErrorCode internalError, IDatabricksConnectionContext connectionContext) {
    this(reason, cause, internalError.toString(), connectionContext);
  }

  public DatabricksSQLException(String reason, Throwable cause, String errorCode, IDatabricksConnectionContext connectionContext) {
    super(reason, errorCode, cause);
  }
}
