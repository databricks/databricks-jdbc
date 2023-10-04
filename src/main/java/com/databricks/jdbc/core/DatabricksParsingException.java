package com.databricks.jdbc.core;

public class DatabricksParsingException extends Exception {
  private final Throwable cause;

  public DatabricksParsingException(String message) {
    super(message);
    this.cause = null;
  }

  public DatabricksParsingException(String message, Throwable cause) {
    super(message);
    this.cause = cause;
  }
}
