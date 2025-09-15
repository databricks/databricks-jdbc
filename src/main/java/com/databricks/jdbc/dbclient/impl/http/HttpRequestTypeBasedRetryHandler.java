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

    long accumulatedTimeTempUnavailable = connectionContext.getTemporarilyUnavailableRetryTimeout();
    long accumulatedTimeRateLimit = connectionContext.getRateLimitRetryTimeout();

    IRetryStrategy strategy = getRetryStrategy(requestType);

    for (int attempt = 1; attempt <= MAX_RETRIES + 1; attempt++) {
      // follow exponential backoff if executing the request throws IOException
      int retryDelayMillis = RetryHandlingHelperFunctions.calculateExponentialBackoff(attempt);
      try {
        CloseableHttpResponse response = httpClient.execute(request);
        int statusCode = response.getStatusLine().getStatusCode();

        // Get retry delay from strategy
        retryDelayMillis = strategy.retryRequestAfter(response, attempt, connectionContext);
        if (retryDelayMillis == -1) {
          return response; // Strategy says don't retry
        }

        switch (statusCode) {
          case HttpStatus.SC_SERVICE_UNAVAILABLE:
            accumulatedTimeTempUnavailable -= retryDelayMillis;
          case HttpStatus.SC_GATEWAY_TIMEOUT:
            accumulatedTimeRateLimit -= retryDelayMillis;
        }

        // Check whether the connection context allows to wait until next attempt
        if (accumulatedTimeTempUnavailable <= 0 || accumulatedTimeRateLimit <= 0) {
          return response;
        }

        // Last attempt check
        if (attempt > MAX_RETRIES) {
          return response;
        }

        response.close();
      } catch (RuntimeException e) {
        RetryHandlingHelperFunctions.throwHttpException(e, request);
      } catch (IOException e) {
        // Continue retry loop for IOException
      }

      try {
        Thread.sleep(retryDelayMillis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Thread interrupted during retry", e);
      }
    }

    return null;
  }

  // Helper method to get retry strategy based on request type idempotency
  private static IRetryStrategy getRetryStrategy(HTTPRequestType requestType) {
    RequestRetryability retryability = requestType.getRequestRetryability();
    return (retryability == RequestRetryability.IDEMPOTENT)
        ? IDEMPOTENT_STRATEGY
        : NON_IDEMPOTENT_STRATEGY;
  }
}
