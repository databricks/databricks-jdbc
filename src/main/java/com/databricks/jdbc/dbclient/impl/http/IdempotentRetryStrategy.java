package com.databricks.jdbc.dbclient.impl.http;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import java.util.Set;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;

/**
 * Retry strategy for idempotent requests - retries all codes except specific client errors. Always
 * uses exponential backoff and ignores Retry-After headers.
 */
public class IdempotentRetryStrategy implements IRetryStrategy {

  // Non-retryable codes for idempotent requests (client errors + success codes)
  private static final Set<Integer> NON_RETRYABLE_CLIENT_ERRORS =
      Set.of(
          // Client errors - should not retry
          HttpStatus.SC_BAD_REQUEST,
          HttpStatus.SC_UNAUTHORIZED,
          HttpStatus.SC_FORBIDDEN,
          HttpStatus.SC_NOT_FOUND,
          HttpStatus.SC_METHOD_NOT_ALLOWED,
          HttpStatus.SC_CONFLICT,
          HttpStatus.SC_GONE,
          HttpStatus.SC_LENGTH_REQUIRED,
          HttpStatus.SC_PRECONDITION_FAILED,
          HttpStatus.SC_REQUEST_TOO_LONG,
          HttpStatus.SC_REQUEST_URI_TOO_LONG,
          HttpStatus.SC_UNSUPPORTED_MEDIA_TYPE,
          HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE);

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
        return !NON_RETRYABLE_CLIENT_ERRORS.contains(statusCode);
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
}
