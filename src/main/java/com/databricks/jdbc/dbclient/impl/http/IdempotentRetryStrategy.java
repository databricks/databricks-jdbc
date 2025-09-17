package com.databricks.jdbc.dbclient.impl.http;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;

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
    if (connectionContext == null) {
      return false; // Default to no retry if connection context is null
    }

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
  public int retryRequestAfter(
      CloseableHttpResponse response,
      int executionAttempt,
      IDatabricksConnectionContext connectionContext) {
    int statusCode = response.getStatusLine().getStatusCode();

    if (!isStatusCodeRetriable(statusCode, connectionContext)) {
      return -1;
    }

    int retryInterval = RetryHandlingHelperFunctions.extractRetryInterval(response);

    if (retryInterval != -1) {
      // Use Retry-After header if it is present in response
      return retryInterval;
    }

    // Use exponential backoff if Retry-After header is not present in response
    return RetryHandlingHelperFunctions.calculateExponentialBackoff(executionAttempt);
  }

  @Override
  public boolean isExceptionRetryable(Exception e) {
    return !NON_RETRIABLE_EXCEPTIONS.contains(e.getClass());
  }
}
