package com.databricks.jdbc.exception;

import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;

/** Exception class to handle http errors while downloading chunk data from external links. */
public class DatabricksHttpException extends DatabricksSQLException {

  /** HTTP response status code; 0 when no HTTP response was received (e.g. network error). */
  private final int httpStatusCode;

  public DatabricksHttpException(
      String message, Throwable cause, DatabricksDriverErrorCode sqlCode) {
    super(message, cause, sqlCode);
    this.httpStatusCode = 0;
  }

  public DatabricksHttpException(String message, DatabricksDriverErrorCode internalCode) {
    super(message, null, internalCode.toString());
    this.httpStatusCode = 0;
  }

  public DatabricksHttpException(String message, String sqlState) {
    super(message, null, sqlState);
    this.httpStatusCode = 0;
  }

  /**
   * Creates an HTTP exception carrying the response status code for programmatic differentiation of
   * transient vs. permanent failures.
   */
  public DatabricksHttpException(String message, int httpStatusCode, String sqlState) {
    super(message, null, sqlState);
    this.httpStatusCode = httpStatusCode;
  }

  public DatabricksHttpException(String message, Throwable throwable, String sqlState) {
    super(message, throwable, sqlState);
    this.httpStatusCode = 0;
  }

  /**
   * Returns the HTTP response status code associated with this exception, or 0 if no HTTP response
   * was received (e.g. connection reset before a response arrived).
   */
  public int getHttpStatusCode() {
    return httpStatusCode;
  }
}
