package com.databricks.jdbc.exception;

import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;

/** Exception class to handle SSL/TLS configuration and handshake errors. */
public class DatabricksSSLException extends DatabricksSQLException {

  public DatabricksSSLException(
      String message, Throwable cause, DatabricksDriverErrorCode sqlCode) {
    super(message, cause, sqlCode);
  }

  public DatabricksSSLException(String message, DatabricksDriverErrorCode internalCode) {
    super(message, null, internalCode.toString());
  }

  public DatabricksSSLException(String message, String sqlState) {
    super(message, null, sqlState);
  }

  public DatabricksSSLException(String message, Throwable throwable, String sqlState) {
    super(message, throwable, sqlState);
  }
}
