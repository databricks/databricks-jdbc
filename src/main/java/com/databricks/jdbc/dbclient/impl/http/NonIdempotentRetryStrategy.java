package com.databricks.jdbc.dbclient.impl.http;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
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

  private static final List<Class<? extends Throwable>> RETRIABLE_EXCEPTIONS =
      Arrays.asList(
          ConnectException.class,
          UnknownHostException.class,
          NoRouteToHostException.class,
          PortUnreachableException.class);

  @Override
  public boolean isStatusCodeRetriable(
      int statusCode, IDatabricksConnectionContext connectionContext) {
    switch (statusCode) {
      case HttpStatus.SC_SERVICE_UNAVAILABLE:
        return connectionContext.shouldRetryTemporarilyUnavailableError();
      case HttpStatus.SC_TOO_MANY_REQUESTS:
        return connectionContext.shouldRetryRateLimitError();
      default:
        return false;
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

    return retryAfterHeader; // Should not reach here based on isStatusCodeRetriable logic
  }

  @Override
  public boolean isExceptionRetryable(Exception e) {
    return RETRIABLE_EXCEPTIONS.contains(e.getClass());
  }
}
