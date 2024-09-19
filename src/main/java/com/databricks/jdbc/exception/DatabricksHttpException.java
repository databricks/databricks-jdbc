package com.databricks.jdbc.exception;

/** Exception class to handle http errors while downloading chunk data from external links. */
public class DatabricksHttpException extends DatabricksSQLException {

  private final Throwable cause;

  public DatabricksHttpException(String message) {
    super(message);
    this.cause = null;
  }

  public DatabricksHttpException(String message, Throwable cause) {
    super(message);
    this.cause = cause;
  }
}
