package com.databricks.jdbc.dbclient.impl.http;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import java.util.Optional;
import org.apache.http.HttpStatus;

/**
 * Manages retry decisions and timeout updates based on HTTP responses and exceptions. Coordinates
 * with retry strategies to determine whether requests should be retried and updates request
 * timeouts accordingly.
 */
public class RetryTimeoutManager {
  private long tempUnavailableTimeout;
  private long rateLimitTimeout;
  private long otherErrorCodesTimeout;
  private long exceptionTimeout;

  /**
   * Creates a new RetryTimeoutManager with connection context.
   *
   * @param connectionContext the connection context for timeout configurations
   */
  public RetryTimeoutManager(IDatabricksConnectionContext connectionContext) {
    // Initialize timeouts
    this.tempUnavailableTimeout = connectionContext.getTemporarilyUnavailableRetryTimeout() * 1000L;
    this.rateLimitTimeout = connectionContext.getRateLimitRetryTimeout() * 1000L;
    this.otherErrorCodesTimeout = RetryUtils.REQUEST_TIMEOUT * 1000L;
    this.exceptionTimeout = RetryUtils.REQUEST_EXCEPTION_TIMEOUT * 1000L;
  }

  /**
   * Evaluates retry decision based on HTTP status code and updates timeout accordingly. Uses the
   * Retry-After header value when provided by the strategy.
   *
   * @param statusCode the HTTP status code from the response
   * @param retryDelayMillis the retry delay in milliseconds to subtract from timeout
   * @return true if the request should be retried, false otherwise
   */
  public boolean evaluateRetryDecisionForResponse(
      int statusCode, Optional<Integer> retryDelayMillis) {
    if (retryDelayMillis.isEmpty()) {
      return false;
    }
    // Update the appropriate timeout based on status code, following executeWithRetry logic
    switch (statusCode) {
      case HttpStatus.SC_SERVICE_UNAVAILABLE:
        tempUnavailableTimeout -= retryDelayMillis.get();
        break;
      case HttpStatus.SC_TOO_MANY_REQUESTS:
        rateLimitTimeout -= retryDelayMillis.get();
        break;
      default:
        otherErrorCodesTimeout -= retryDelayMillis.get();
        break;
    }

    // Check if any timeout has been exceeded
    return tempUnavailableTimeout > 0 && rateLimitTimeout > 0 && otherErrorCodesTimeout > 0;
  }

  /**
   * Evaluates retry decision based on an exception and updates timeout accordingly.
   *
   * @param exception the exception that occurred during request execution
   * @param retryDelayMillis the retry delay in milliseconds to subtract from timeout
   * @return true if the request should be retried, false otherwise
   */
  public boolean evaluateRetryDecisionForException(
      IRetryStrategy strategy, Exception exception, int retryDelayMillis) {
    if (!strategy.isExceptionRetryable(exception)) {
      return false;
    }

    // Update exception timeout by subtracting the retry delay
    exceptionTimeout -= retryDelayMillis;

    // Check if exception timeout has been exceeded
    return exceptionTimeout > 0;
  }
}
