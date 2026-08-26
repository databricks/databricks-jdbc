package com.databricks.jdbc.exception;

import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;

/** Exception class to handle http errors while downloading chunk data from external links. */
public class DatabricksHttpException extends DatabricksSQLException {

  /**
   * HTTP status code that produced this exception, or {@code 0} when no HTTP response was received
   * (e.g. a connection-level failure) or the status is otherwise unknown.
   */
  private final int statusCode;

  public DatabricksHttpException(
      String message, Throwable cause, DatabricksDriverErrorCode sqlCode) {
    super(message, cause, sqlCode);
    this.statusCode = 0;
  }

  public DatabricksHttpException(String message, DatabricksDriverErrorCode internalCode) {
    super(message, null, internalCode.toString());
    this.statusCode = 0;
  }

  public DatabricksHttpException(String message, String sqlState) {
    super(message, null, sqlState);
    this.statusCode = 0;
  }

  public DatabricksHttpException(String message, Throwable throwable, String sqlState) {
    super(message, throwable, sqlState);
    this.statusCode = 0;
  }

  public DatabricksHttpException(String message, String sqlState, int statusCode) {
    super(message, null, sqlState);
    this.statusCode = statusCode;
  }

  /**
   * Returns the HTTP status code associated with this exception, or {@code 0} when no HTTP response
   * was received (connection-level failure) or the status is unknown.
   */
  public int getStatusCode() {
    return statusCode;
  }
}
