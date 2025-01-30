package com.databricks.jdbc.exception;

import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.sql.ClientInfoStatus;
import java.sql.SQLClientInfoException;
import java.util.Map;

public class DatabricksSQLClientInfoException extends SQLClientInfoException {
  public DatabricksSQLClientInfoException(
      String message,
      Map<String, ClientInfoStatus> failedProperties,
      DatabricksDriverErrorCode internalErrorCode) {
    super(message, internalErrorCode.toString(), failedProperties);
  }
}
