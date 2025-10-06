package com.databricks.jdbc.dbclient.impl.http;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.net.*;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.http.HttpStatus;

/**
 * Retry strategy for non-idempotent requests - only retries 503/429 and respects Retry-After
 * header. Does not retry if Retry-After header is missing.
 */
public class NonIdempotentRetryStrategy implements IRetryStrategy {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(NonIdempotentRetryStrategy.class);

  private static final List<Class<? extends Throwable>> RETRIABLE_EXCEPTIONS =
      Arrays.asList(
          ConnectException.class,
          UnknownHostException.class,
          NoRouteToHostException.class,
          PortUnreachableException.class);

  /**
   * Determines if an HTTP response should be retried for non-idempotent requests and calculates the
   * retry delay.
   *
   * <p>This method is more conservative than idempotent retry strategy. It only retries:
   *
   * <ul>
   *   <li>503 Service Unavailable (if enabled in connection context)
   *   <li>429 Too Many Requests (if enabled in connection context)
   *   <li>ONLY when a Retry-After header is present - will not retry without it
   * </ul>
   *
   * <p>For retriable responses with Retry-After header:
   *
   * <ul>
   *   <li>Uses the Retry-After header value as the retry delay
   *   <li>Validates the retry delay against configured timeouts using RetryTimeoutManager
   * </ul>
   *
   * <p>Timeout determination: The RetryTimeoutManager maintains separate timeout budgets for
   * different status codes (503, 429, and other errors). For each retry attempt:
   *
   * <ol>
   *   <li>The retry delay (from Retry-After header) is subtracted from the appropriate timeout
   *       budget
   *   <li>If the remaining budget is positive, retry is allowed
   *   <li>If the remaining budget becomes zero or negative, all retries are exhausted and the
   *       request fails
   * </ol>
   *
   * <p>Initial timeout values are configured via connection context (e.g.,
   * TemporarilyUnavailableRetryTimeout for 503, RateLimitRetryTimeout for 429).
   *
   * @param statusCode the HTTP status code from the response
   * @param retryAfterHeader optional Retry-After header value in milliseconds
   * @param executionAttempt the current execution attempt number (0-based)
   * @param connectionContext the connection context with retry configuration
   * @param retryTimeoutManager manages timeout tracking across retries
   * @return Optional containing retry delay in milliseconds if retry should occur, empty otherwise
   */
  @Override
  public Optional<Integer> shouldRetryAfter(
      int statusCode,
      Optional<Integer> retryAfterHeader,
      int executionAttempt,
      IDatabricksConnectionContext connectionContext,
      RetryTimeoutManager retryTimeoutManager) {

    LOGGER.debug(
        "Received HTTP response. Status code: {}, Retry-After header: {}, attempt: {}",
        statusCode,
        retryAfterHeader.isPresent() ? retryAfterHeader.get() + "ms" : "not present",
        executionAttempt);

    if (!isStatusCodeRetriable(statusCode, connectionContext)) {
      return Optional.empty();
    } else if (retryAfterHeader.isEmpty()) {
      LOGGER.error(
          "Retry-After header not present for status code {} in non-idempotent request",
          statusCode);
      return Optional.empty();
    }

    int retryAfter = retryAfterHeader.get();
    if (!retryTimeoutManager.evaluateRetryTimeoutForResponse(statusCode, retryAfter)) {
      LOGGER.error(
          "Retry timeout reached for HTTP response. Status code: {}, retry after: {} seconds",
          statusCode,
          retryAfter);
      return Optional.empty();
    }

    return Optional.of(retryAfter);
  }

  /**
   * Determines if a request should be retried after an exception for non-idempotent requests and
   * calculates the retry delay.
   *
   * <p>This method only retries specific network-related exceptions that indicate connectivity
   * issues:
   *
   * <ul>
   *   <li>ConnectException - connection refused
   *   <li>UnknownHostException - DNS resolution failure
   *   <li>NoRouteToHostException - network routing issue
   *   <li>PortUnreachableException - port not accessible
   * </ul>
   *
   * <p>For retriable exceptions:
   *
   * <ul>
   *   <li>Calculates exponential backoff delay based on execution attempt
   *   <li>Validates the retry delay against configured exception timeout using RetryTimeoutManager
   * </ul>
   *
   * <p>Timeout determination: The RetryTimeoutManager maintains a separate timeout budget for
   * exceptions. For each retry attempt:
   *
   * <ol>
   *   <li>The retry delay (calculated via exponential backoff) is subtracted from the exception
   *       timeout budget
   *   <li>If the remaining budget is positive, retry is allowed
   *   <li>If the remaining budget becomes zero or negative, all retries are exhausted and the
   *       exception is thrown
   * </ol>
   *
   * <p>The initial exception timeout value is configured via connection context.
   *
   * @param e the exception that occurred during request execution
   * @param executionAttempt the current execution attempt number (0-based)
   * @param retryTimeoutManager manages timeout tracking across retries
   * @return Optional containing retry delay in milliseconds if retry should occur, empty otherwise
   */
  @Override
  public Optional<Integer> shouldRetryAfter(
      Exception e, int executionAttempt, RetryTimeoutManager retryTimeoutManager) {
    LOGGER.debug(
        "Received exception. Exception type: {}, attempt: {}",
        e.getClass().getSimpleName(),
        executionAttempt);

    if (!isExceptionRetrieable(e)) {
      LOGGER.debug(
          "Exception {} is not retriable for non-idempotent request", e.getClass().getSimpleName());
      return Optional.empty();
    }

    int retryAfter = RetryUtils.calculateExponentialBackoff(executionAttempt);
    if (!retryTimeoutManager.evaluateRetryTimeoutForException(retryAfter)) {
      LOGGER.error(
          "Retry timeout reached for exception. Exception: {}, retry after: {} seconds",
          e.getClass().getSimpleName(),
          retryAfter);
      return Optional.empty();
    }

    return Optional.of(retryAfter);
  }

  private boolean isStatusCodeRetriable(
      int statusCode, IDatabricksConnectionContext connectionContext) {
    if (statusCode >= 200 && statusCode < 300) {
      return false;
    }

    boolean isRetriable;
    switch (statusCode) {
      case HttpStatus.SC_SERVICE_UNAVAILABLE:
        isRetriable = connectionContext.shouldRetryTemporarilyUnavailableError();
        break;
      case HttpStatus.SC_TOO_MANY_REQUESTS:
        isRetriable = connectionContext.shouldRetryRateLimitError();
        break;
      default:
        isRetriable = false;
        break;
    }

    if (!isRetriable) {
      LOGGER.error("Status code {} is not retriable for non-idempotent request", statusCode);
    }
    return isRetriable;
  }

  private boolean isExceptionRetrieable(Exception e) {
    return RETRIABLE_EXCEPTIONS.contains(e.getClass());
  }
}
