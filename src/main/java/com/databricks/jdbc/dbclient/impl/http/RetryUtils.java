package com.databricks.jdbc.dbclient.impl.http;

import com.databricks.jdbc.exception.DatabricksRetryHandlerException;

/**
 * Utility class containing common retry handling helper functions used across different retry
 * strategies and handlers.
 */
public class RetryUtils {
  public static final long REQUEST_TIMEOUT_SECONDS = 10;
  public static final long REQUEST_EXCEPTION_TIMEOUT_SECONDS = 10;

  /**
   * Extracts DatabricksRetryHandlerException from the exception cause chain.
   *
   * @param e the exception to search through
   * @return the DatabricksRetryHandlerException if found, null otherwise
   */
  public static DatabricksRetryHandlerException extractRetryException(Throwable e) {
    Throwable cause = e.getCause();
    while (cause != null) {
      if (cause instanceof DatabricksRetryHandlerException) {
        return (DatabricksRetryHandlerException) cause;
      }
      cause = cause.getCause();
    }
    return null;
  }
}
