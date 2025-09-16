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

  private static final IRetryStrategy IDEMPOTENT_STRATEGY = new IdempotentRetryStrategy();
  private static final IRetryStrategy NON_IDEMPOTENT_STRATEGY = new NonIdempotentRetryStrategy();

  /** Main entry point - executes HTTP request with retry logic based on request type. */
  public static CloseableHttpResponse executeWithRetry(
      CloseableHttpClient httpClient,
      HttpUriRequest request,
      HTTPRequestType requestType,
      IDatabricksConnectionContext connectionContext)
      throws DatabricksHttpException {

    long tempUnavailableTimeout =
        connectionContext.getTemporarilyUnavailableRetryTimeout(); // Default Value is 900
    long rateLimitTimeout = connectionContext.getRateLimitRetryTimeout(); // Default value is 120
    int maxRetries = connectionContext.getMaxRetries(); // Default value is 5

    IRetryStrategy strategy = getRetryStrategy(requestType);
    LOGGER.debug(
        "Starting retry handler for {} with {} strategy, maxRetries={}",
        requestType,
        strategy.getClass().getSimpleName(),
        maxRetries);

    for (int attempt = 1; attempt <= maxRetries + 1; attempt++) {
      // follow exponential backoff if executing the request throws IOException
      int retryDelayMillis = RetryHandlingHelperFunctions.calculateExponentialBackoff(attempt);
      retryDelayMillis = RetryHandlingHelperFunctions.addJitter(retryDelayMillis);
      try {
        CloseableHttpResponse response = httpClient.execute(request);
        int statusCode = response.getStatusLine().getStatusCode();

        // Get retry delay from strategy
        retryDelayMillis = strategy.retryRequestAfter(response, attempt, connectionContext);
        if (retryDelayMillis == -1) {
          return response; // Strategy says don't retry
        }
        // apply jitter if strategy says to retry
        retryDelayMillis = RetryHandlingHelperFunctions.addJitter(retryDelayMillis);

        switch (statusCode) {
          case HttpStatus.SC_SERVICE_UNAVAILABLE:
            tempUnavailableTimeout -= retryDelayMillis;
          case HttpStatus.SC_GATEWAY_TIMEOUT:
            rateLimitTimeout -= retryDelayMillis;
        }

        // Check whether the connection context allows to wait until next attempt
        if (tempUnavailableTimeout <= 0 || rateLimitTimeout <= 0) {
          LOGGER.debug(
              "Retry timeout exceeded for {} on attempt {}, received HTTP status code {}. Returning response",
              requestType,
              attempt,
              statusCode);
          return response;
        }

        // Last attempt check
        if (attempt > maxRetries) {
          LOGGER.debug(
              "Max retries ({}) reached for {} on attempt {}, returning response",
              maxRetries,
              requestType,
              attempt);
          return response;
        }

        String errorReason = response.getStatusLine().getReasonPhrase();
        String errorMessage =
            String.format(
                "Retry failure. HTTP response code: %s, Error Message: %s",
                statusCode, errorReason);
        LOGGER.debug(errorMessage);

        response.close();
      } catch (RuntimeException e) {
        /* These include
          IllegalArgumentException
          IllegalStateException
          UnsupportedOperationException
          IndexOutOfBoundsException
          NullPointerException
          ClassCastException
          NumberFormatException
          ArrayIndexOutOfBoundsException
          ArrayStoreException
          ArithmeticException
          NegativeArraySizeException
        */
        LOGGER.error(
            "Runtime exception on attempt {} for {}: error message {}",
            attempt,
            requestType,
            e.getMessage());
        RetryHandlingHelperFunctions.throwHttpException(e, request);
      } catch (IOException e) {
        /* Continue retry loop for IOException which include
           ConnectException
           UnknownHostException
           NoRouteToHostException
           PortUnreachableException
        */
        LOGGER.warn(
            "IOException on attempt {} for {}, error message: {}",
            attempt,
            requestType,
            e.getMessage());
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
