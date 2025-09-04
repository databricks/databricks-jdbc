package com.databricks.jdbc.exception;

import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.sql.SQLTimeoutException;

/** Top level exception for Databricks driver */
public class DatabricksTimeoutException extends SQLTimeoutException {
  public DatabricksTimeoutException(String message) {
    super(message);
  }

  public DatabricksTimeoutException(
      String reason, Throwable cause, DatabricksDriverErrorCode internalError) {
    super(reason, internalError.toString(), cause);
  }
}
