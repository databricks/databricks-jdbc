package com.databricks.jdbc.exception;

/** Exception class to handle volume operation errors. */
public class DatabricksVolumeOperationException extends DatabricksSQLException {

  public DatabricksVolumeOperationException(String message, Throwable cause) {
    super(message, cause);
  }

  public DatabricksVolumeOperationException(String message) {
    super(message);
  }
}
