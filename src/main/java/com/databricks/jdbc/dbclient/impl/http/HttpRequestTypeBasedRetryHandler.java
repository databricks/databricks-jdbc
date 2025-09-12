package com.databricks.jdbc.dbclient.impl.http;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.HTTPRequestType;
import com.databricks.jdbc.common.RequestRetryability;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.io.IOException;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;

/**
 * Static HTTP retry handler that implements retry logic based on HTTPRequestType using strategy
 * pattern.
 */
public class HttpRequestTypeBasedRetryHandler {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(HttpRequestTypeBasedRetryHandler.class);

  private static final int MAX_RETRIES = 5;
  private static final String RETRY_AFTER_HEADER = "Retry-After";

  private static final IRetryStrategy IDEMPOTENT_STRATEGY = new IdempotentRetryStrategy();
  private static final IRetryStrategy NON_IDEMPOTENT_STRATEGY = new NonIdempotentRetryStrategy();

  /** Main entry point - executes HTTP request with retry logic based on request type. */
  public static CloseableHttpResponse executeWithRetry(
      CloseableHttpClient httpClient,
      HttpUriRequest request,
      HTTPRequestType requestType,
      IDatabricksConnectionContext connectionContext)
      throws DatabricksHttpException {

    long[] accumulatedTimes = {0L, 0L}; // [tempUnavailable, rateLimit]

    IRetryStrategy strategy = getRetryStrategy(requestType);

    for (int attempt = 1; attempt <= MAX_RETRIES + 1; attempt++) {
      try {
        CloseableHttpResponse response = httpClient.execute(request);
        int statusCode = response.getStatusLine().getStatusCode();

        if (!strategy.isStatusCodeRetriable(statusCode, connectionContext)) {
          return response;
        }

        updateAccumulatedTime(statusCode, response, accumulatedTimes);

        if (!isRetryAllowedByTimeout(statusCode, accumulatedTimes, connectionContext)) {
          return response;
        }

        if (attempt > MAX_RETRIES) {
          return response;
        }

        int retryDelayMillis = strategy.retryRequestAfter(response, attempt, connectionContext);
        if (retryDelayMillis == -1) {
          return response;
        }
        response.close();

        try {
          Thread.sleep(retryDelayMillis);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Thread interrupted during retry", e);
        }
      } catch (RuntimeException e) {
        RetryHandlingHelperFunctions.throwHttpException(e, request);
      } catch (IOException e) {
        // Continue retry loop for IOException
        if (attempt > MAX_RETRIES) {
          break;
        }
        try {
          Thread.sleep(RetryHandlingHelperFunctions.calculateExponentialBackoff(attempt));
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Thread interrupted during retry", ie);
        }
      }
    }

    return null;
  }

  private static void updateAccumulatedTime(
      int statusCode, CloseableHttpResponse response, long[] accumulatedTimes) {
    int retryInterval = RetryHandlingHelperFunctions.extractRetryInterval(response);
    if (retryInterval <= 0) return;

    if (statusCode == HttpStatus.SC_SERVICE_UNAVAILABLE) {
      accumulatedTimes[0] += retryInterval; // tempUnavailable
    } else if (statusCode == HttpStatus.SC_TOO_MANY_REQUESTS) {
      accumulatedTimes[1] += retryInterval; // rateLimit
    }
  }

  private static boolean isRetryAllowedByTimeout(
      int statusCode, long[] accumulatedTimes, IDatabricksConnectionContext connectionContext) {
    if (connectionContext == null) {
      return true;
    }

    if (statusCode == HttpStatus.SC_SERVICE_UNAVAILABLE) {
      return accumulatedTimes[0] <= connectionContext.getTemporarilyUnavailableRetryTimeout();
    } else if (statusCode == HttpStatus.SC_TOO_MANY_REQUESTS) {
      return accumulatedTimes[1] <= connectionContext.getRateLimitRetryTimeout();
    }
    return true;
  }

  // Helper method to get retry strategy based on request type idempotency
  private static IRetryStrategy getRetryStrategy(HTTPRequestType requestType) {
    RequestRetryability retryability = requestType.getRequestRetryability();
    return (retryability == RequestRetryability.IDEMPOTENT)
        ? IDEMPOTENT_STRATEGY
        : NON_IDEMPOTENT_STRATEGY;
  }
}
