package com.databricks.jdbc.exception;

import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import org.apache.http.HttpStatus;

/**
 * Exception thrown when a Databricks API call fails due to rate limiting (HTTP 429).
 *
 * <p>This exception is used to distinguish rate limit errors from other HTTP errors, enabling
 * special handling such as circuit breaker activation for session creation failures.
 */
public class DatabricksRateLimitException extends DatabricksSQLException {

  /** HTTP status code for rate limiting */
  public static final int HTTP_TOO_MANY_REQUESTS = HttpStatus.SC_TOO_MANY_REQUESTS; // 429

  private final int statusCode;

  /**
   * Constructs a new rate limit exception.
   *
   * @param message the detail message
   * @param statusCode the HTTP status code (should be 429)
   */
  public DatabricksRateLimitException(String message, int statusCode) {
    super(message, DatabricksDriverErrorCode.RATE_LIMIT_EXCEEDED);
    this.statusCode = statusCode;
  }

  /**
   * Constructs a new rate limit exception with a cause.
   *
   * @param message the detail message
   * @param cause the cause of this exception
   * @param statusCode the HTTP status code (should be 429)
   */
  public DatabricksRateLimitException(String message, Throwable cause, int statusCode) {
    super(message, cause, DatabricksDriverErrorCode.RATE_LIMIT_EXCEEDED);
    this.statusCode = statusCode;
  }

  /**
   * Gets the HTTP status code.
   *
   * @return the status code
   */
  public int getStatusCode() {
    return statusCode;
  }
}
