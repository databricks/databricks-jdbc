package com.databricks.jdbc.dbclient.impl.http;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import org.apache.http.HttpStatus;

/**
 * Manages retry decisions and timeout updates based on HTTP responses and exceptions. Coordinates
 * with retry strategies to determine whether requests should be retried and updates request
 * timeouts accordingly.
 */
public class RetryTimeoutManager {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(RetryTimeoutManager.class);
  private long tempUnavailableTimeoutMillis;
  private long rateLimitTimeoutMillis;
  private long otherErrorCodesTimeoutMillis;
  private long exceptionTimeoutMillis;
  private long apiRetriableCodesTimeoutMillis;

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
    this.otherErrorCodesTimeoutMillis = RetryUtils.DEFAULT_REQUEST_TIMEOUT_SECONDS * 1000L;
    this.exceptionTimeoutMillis = RetryUtils.DEFAULT_REQUEST_EXCEPTION_TIMEOUT_SECONDS * 1000L;
    this.apiRetriableCodesTimeoutMillis = connectionContext.getApiRetryTimeout() * 1000L;
  }

  /**
   * Evaluates retry decision based on HTTP status code and updates timeout accordingly. Uses the
   * Retry-After header value when provided by the strategy.
   *
   * @param statusCode the HTTP status code from the response
   * @param retryDelayMillis the retry delay in milliseconds to subtract from timeout
   * @param isApiRetriableCode true if this is a custom API retriable code, false otherwise
   * @return true if the request should be retried, false otherwise
   */
  public boolean evaluateRetryTimeoutForResponse(
      int statusCode, int retryDelayMillis, boolean isApiRetriableCode) {
    // If this is a custom API retriable code, only deduct from API codes timeout
    if (isApiRetriableCode) {
      apiRetriableCodesTimeoutMillis -= retryDelayMillis;
      if (apiRetriableCodesTimeoutMillis <= 0) {
        LOGGER.debug(
            "Retry stopped: API retriable codes timeout exhausted. Remaining: {}ms",
            apiRetriableCodesTimeoutMillis);
        return false;
      }
      return true;
    }

    // Otherwise, update the appropriate timeout based on status code
    switch (statusCode) {
      case HttpStatus.SC_SERVICE_UNAVAILABLE:
        tempUnavailableTimeoutMillis -= retryDelayMillis;
        if (tempUnavailableTimeoutMillis <= 0) {
          LOGGER.debug(
              "Retry stopped: Service unavailable (503) timeout exhausted. Remaining: {}ms",
              tempUnavailableTimeoutMillis);
          return false;
        }
        break;
      case HttpStatus.SC_TOO_MANY_REQUESTS:
        rateLimitTimeoutMillis -= retryDelayMillis;
        if (rateLimitTimeoutMillis <= 0) {
          LOGGER.debug(
              "Retry stopped: Rate limit (429) timeout exhausted. Remaining: {}ms",
              rateLimitTimeoutMillis);
          return false;
        }
        break;
      default:
        otherErrorCodesTimeoutMillis -= retryDelayMillis;
        if (otherErrorCodesTimeoutMillis <= 0) {
          LOGGER.debug(
              "Retry stopped: Other error codes timeout exhausted for status {}. Remaining: {}ms",
              statusCode,
              otherErrorCodesTimeoutMillis);
          return false;
        }
        break;
    }

    return true;
  }

  /**
   * Evaluates retry decision based on an exception and updates timeout accordingly.
   *
   * @param retryDelayMillis the retry delay in milliseconds to subtract from timeout
   * @return true if the request should be retried, false otherwise
   */
  public boolean evaluateRetryTimeoutForException(int retryDelayMillis) {
    // Update exception timeout by subtracting the retry delay
    exceptionTimeoutMillis -= retryDelayMillis;

    // Check if exception timeout has been exceeded
    if (exceptionTimeoutMillis <= 0) {
      LOGGER.debug(
          "Retry stopped: Exception timeout exhausted. Remaining: {}ms", exceptionTimeoutMillis);
      return false;
    }
    return true;
  }
}
