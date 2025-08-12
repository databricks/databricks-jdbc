package com.databricks.jdbc.dbclient.impl.http;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;

/**
 * Retry strategy for non-idempotent requests - only retries 503/429 and respects Retry-After
 * header. Does not retry if Retry-After header is missing.
 */
public class NonIdempotentRetryStrategy implements IRetryStrategy {

  private static final String RETRY_AFTER_HEADER = "Retry-After";

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
  public int retryRequestAfter(
      CloseableHttpResponse response,
      int executionAttempt,
      IDatabricksConnectionContext connectionContext) {
    int statusCode = response.getStatusLine().getStatusCode();

    // For non-idempotent requests: respect Retry-After header, return -1 if not present
    if (statusCode == HttpStatus.SC_SERVICE_UNAVAILABLE
        || statusCode == HttpStatus.SC_TOO_MANY_REQUESTS) {
      int retryIntervalSeconds = extractRetryInterval(response);
      return retryIntervalSeconds > 0 ? retryIntervalSeconds * 1000 : -1; // Convert to milliseconds
    }

    return -1; // Should not reach here based on isStatusCodeRetriable logic
  }

  private static int extractRetryInterval(CloseableHttpResponse response) {
    if (response.containsHeader(RETRY_AFTER_HEADER)) {
      try {
        return Integer.parseInt(response.getFirstHeader(RETRY_AFTER_HEADER).getValue());
      } catch (NumberFormatException e) {
        // Invalid header value
      }
    }
    return -1;
  }
}
