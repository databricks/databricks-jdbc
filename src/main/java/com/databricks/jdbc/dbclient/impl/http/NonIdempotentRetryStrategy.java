package com.databricks.jdbc.dbclient.impl.http;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import java.net.*;
import java.util.Arrays;
import java.util.List;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;

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
    if (connectionContext == null) {
      return false; // Default to no retry if connection context is null
    }

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

    if (!isStatusCodeRetriable(statusCode, connectionContext)) {
      return -1;
    }

    // For non-idempotent requests: respect Retry-After header, return -1 if not present
    if (statusCode == HttpStatus.SC_SERVICE_UNAVAILABLE
        || statusCode == HttpStatus.SC_TOO_MANY_REQUESTS) {
      int retryIntervalSeconds = RetryHandlingHelperFunctions.extractRetryInterval(response);
      return retryIntervalSeconds > 0 ? retryIntervalSeconds * 1000 : -1; // Convert to milliseconds
    }

    return -1; // Should not reach here based on isStatusCodeRetriable logic
  }

  @Override
  public boolean isExceptionRetryable(Exception e) {
    return RETRIABLE_EXCEPTIONS.contains(e.getClass());
  }
}
