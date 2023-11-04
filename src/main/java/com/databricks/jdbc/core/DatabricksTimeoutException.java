package com.databricks.jdbc.core;

import java.sql.SQLTimeoutException;

/** Top level exception for Databricks driver */
public class DatabricksTimeoutException extends SQLTimeoutException {
  public DatabricksTimeoutException(String message) {
    super(message);
  }

  public DatabricksTimeoutException(String message, Throwable exception) {
    super(message, exception);
  }
}
