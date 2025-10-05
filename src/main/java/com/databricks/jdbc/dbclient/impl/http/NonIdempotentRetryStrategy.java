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
