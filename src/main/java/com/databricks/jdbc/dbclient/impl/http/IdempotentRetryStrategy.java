package com.databricks.jdbc.dbclient.impl.http;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.http.HttpStatus;

/**
 * Retry strategy for idempotent requests - retries all codes except specific client errors. Always
 * uses exponential backoff and ignores Retry-After headers.
 */
public class IdempotentRetryStrategy implements IRetryStrategy {

  private static final Set<Class<? extends RuntimeException>> NON_RETRIABLE_EXCEPTIONS =
      new HashSet<>(
          Arrays.asList(
              IllegalArgumentException.class,
              IllegalStateException.class,
              UnsupportedOperationException.class,
              IndexOutOfBoundsException.class,
              NullPointerException.class,
              ClassCastException.class,
              NumberFormatException.class,
              ArrayIndexOutOfBoundsException.class,
              ArrayStoreException.class,
              ArithmeticException.class,
              NegativeArraySizeException.class));

  private static final Set<Integer> NON_RETRIABLE_HTTP_CODES =
      new HashSet<>(Arrays.asList(400, 401, 403, 404, 405, 409, 410, 411, 412, 413, 414, 415, 416));

  @Override
  public boolean isStatusCodeRetriable(
      int statusCode, IDatabricksConnectionContext connectionContext) {

    if (statusCode >= 200 && statusCode < 300) {
      return false;
    }

    switch (statusCode) {
      case HttpStatus.SC_SERVICE_UNAVAILABLE:
        return connectionContext.shouldRetryTemporarilyUnavailableError();
      case HttpStatus.SC_TOO_MANY_REQUESTS:
        return connectionContext.shouldRetryRateLimitError();
      default:
        return !NON_RETRIABLE_HTTP_CODES.contains(statusCode);
    }
  }

  @Override
  public Optional<Integer> retryRequestAfter(
      int statusCode,
      Optional<Integer> retryAfterHeader,
      int executionAttempt,
      IDatabricksConnectionContext connectionContext) {

    if (!isStatusCodeRetriable(statusCode, connectionContext)) {
      return Optional.empty();
    }

    if (retryAfterHeader.isPresent()) {
      // Use Retry-After header if it is present in response
      return retryAfterHeader;
    }

    // Use exponential backoff if Retry-After header is not present in response
    int delay = RetryUtils.calculateExponentialBackoff(executionAttempt);
    return Optional.of(delay);
  }

  @Override
  public boolean isExceptionRetryable(Exception e) {
    return !NON_RETRIABLE_EXCEPTIONS.contains(e.getClass());
  }
}
