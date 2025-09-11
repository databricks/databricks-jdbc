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
          // Success codes - no need to retry
          HttpStatus.SC_OK,
          HttpStatus.SC_CREATED,
          HttpStatus.SC_ACCEPTED,
          HttpStatus.SC_NO_CONTENT,
          HttpStatus.SC_RESET_CONTENT,
          HttpStatus.SC_PARTIAL_CONTENT,
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

  private static final int DEFAULT_BACKOFF_FACTOR = 2;
  private static final int MIN_BACKOFF_INTERVAL = 1000; // 1s
  private static final int MAX_RETRY_INTERVAL = 10000; // 10s

  @Override
  public boolean isStatusCodeRetriable(
      int statusCode, IDatabricksConnectionContext connectionContext) {
    return !NON_RETRYABLE_CLIENT_ERRORS.contains(statusCode);
  }

  @Override
  public int retryRequestAfter(
      CloseableHttpResponse response,
      int executionAttempt,
      IDatabricksConnectionContext connectionContext) {
    if (executionAttempt <= 0) {
      return MIN_BACKOFF_INTERVAL;
    }
    // For idempotent requests: always use exponential backoff (ignore Retry-After header)
    return calculateExponentialBackoff(executionAttempt - 1);
  }

  private static int calculateExponentialBackoff(int executionCount) {
    return (int)
        Math.min(
            MIN_BACKOFF_INTERVAL * Math.pow(DEFAULT_BACKOFF_FACTOR, executionCount),
            MAX_RETRY_INTERVAL);
  }
}
