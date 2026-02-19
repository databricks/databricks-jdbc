package com.databricks.jdbc.dbclient.impl.http;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import org.apache.http.HttpStatus;

/**
 * Manages retry decisions and timeout updates based on HTTP responses and exceptions. Coordinates
 * with retry strategies to determine whether requests should be retried and updates request
 * timeouts accordingly.
 */
public class RetryTimeoutManager {
  private long tempUnavailableTimeoutMillis;
  private long rateLimitTimeoutMillis;
  private long otherErrorCodesTimeoutMillis;
  private long exceptionTimeoutMillis;

  /**
   * Creates a new RetryTimeoutManager with connection context.
   *
   * @param connectionContext the connection context for timeout configurations
   */
  public RetryTimeoutManager(IDatabricksConnectionContext connectionContext) {
    // Initialize timeouts
    this.tempUnavailableTimeoutMillis =
        connectionContext.getTemporarilyUnavailableRetryTimeout() * 1000L;
    this.rateLimitTimeoutMillis = connectionContext.getRateLimitRetryTimeout() * 1000L;
    this.otherErrorCodesTimeoutMillis = RetryUtils.REQUEST_TIMEOUT_SECONDS * 1000L;
    this.exceptionTimeoutMillis = RetryUtils.REQUEST_EXCEPTION_TIMEOUT_SECONDS * 1000L;
  }

  /**
   * Evaluates retry decision based on HTTP status code and updates timeout accordingly. Uses the
   * Retry-After header value when provided by the strategy.
   *
   * @param statusCode the HTTP status code from the response
   * @param retryDelayMillis the retry delay in milliseconds to subtract from timeout
   * @return true if the request should be retried, false otherwise
   */
  public boolean evaluateRetryTimeoutForResponse(int statusCode, int retryDelayMillis) {
    // Update the appropriate timeout based on status code
    switch (statusCode) {
      case HttpStatus.SC_SERVICE_UNAVAILABLE:
        tempUnavailableTimeoutMillis -= retryDelayMillis;
        break;
      case HttpStatus.SC_TOO_MANY_REQUESTS:
        rateLimitTimeoutMillis -= retryDelayMillis;
        break;
      default:
        otherErrorCodesTimeoutMillis -= retryDelayMillis;
        break;
    }

    // Check if any timeout has been exceeded
    return tempUnavailableTimeoutMillis > 0
        && rateLimitTimeoutMillis > 0
        && otherErrorCodesTimeoutMillis > 0;
  }

  /**
   * Evaluates retry decision based on an exception and updates timeout accordingly.
   *
   * @param exception the exception that occurred during request execution
   * @param retryDelayMillis the retry delay in milliseconds to subtract from timeout
   * @return true if the request should be retried, false otherwise
   */
  public boolean evaluateRetryTimeoutForException(int retryDelayMillis) {
    // Update exception timeout by subtracting the retry delay
    exceptionTimeoutMillis -= retryDelayMillis;

    // Check if exception timeout has been exceeded
    return exceptionTimeoutMillis > 0;
  }
}
