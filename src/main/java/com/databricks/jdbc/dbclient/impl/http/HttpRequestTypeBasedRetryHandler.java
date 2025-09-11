package com.databricks.jdbc.dbclient.impl.http;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.DEFAULT_HTTP_EXCEPTION_SQLSTATE;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.HTTPRequestType;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksRetryHandlerException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;
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

  public enum RequestIdempotency {
    IDEMPOTENT,
    NON_IDEMPOTENT
  }

  // Map defining which request types are idempotent vs non-idempotent
  private static final Map<HTTPRequestType, RequestIdempotency> REQUEST_IDEMPOTENCY_MAP;

  static {
    REQUEST_IDEMPOTENCY_MAP = new EnumMap<>(HTTPRequestType.class);
    // Idempotent operations
    REQUEST_IDEMPOTENCY_MAP.put(HTTPRequestType.THRIFT_OPEN_SESSION, RequestIdempotency.IDEMPOTENT);
    REQUEST_IDEMPOTENCY_MAP.put(
        HTTPRequestType.THRIFT_CLOSE_SESSION, RequestIdempotency.IDEMPOTENT);
    REQUEST_IDEMPOTENCY_MAP.put(HTTPRequestType.THRIFT_METADATA, RequestIdempotency.IDEMPOTENT);
    REQUEST_IDEMPOTENCY_MAP.put(
        HTTPRequestType.THRIFT_CLOSE_OPERATION, RequestIdempotency.IDEMPOTENT);
    REQUEST_IDEMPOTENCY_MAP.put(
        HTTPRequestType.THRIFT_CANCEL_OPERATION, RequestIdempotency.IDEMPOTENT);
    REQUEST_IDEMPOTENCY_MAP.put(
        HTTPRequestType.THRIFT_FETCH_RESULTS, RequestIdempotency.IDEMPOTENT);
    REQUEST_IDEMPOTENCY_MAP.put(HTTPRequestType.CLOUD_FETCH, RequestIdempotency.IDEMPOTENT);
    REQUEST_IDEMPOTENCY_MAP.put(HTTPRequestType.VOLUME_LIST, RequestIdempotency.IDEMPOTENT);
    REQUEST_IDEMPOTENCY_MAP.put(HTTPRequestType.VOLUME_SHOW_VOLUMES, RequestIdempotency.IDEMPOTENT);
    REQUEST_IDEMPOTENCY_MAP.put(HTTPRequestType.VOLUME_GET, RequestIdempotency.IDEMPOTENT);
    REQUEST_IDEMPOTENCY_MAP.put(HTTPRequestType.VOLUME_DELETE, RequestIdempotency.IDEMPOTENT);
    REQUEST_IDEMPOTENCY_MAP.put(HTTPRequestType.AUTH, RequestIdempotency.IDEMPOTENT);
    REQUEST_IDEMPOTENCY_MAP.put(HTTPRequestType.TELEMETRY_PUSH, RequestIdempotency.IDEMPOTENT);
    REQUEST_IDEMPOTENCY_MAP.put(
        HTTPRequestType.OTHER,
        RequestIdempotency.IDEMPOTENT); // some HTTP get requests written in test

    // Non-idempotent operations
    REQUEST_IDEMPOTENCY_MAP.put(
        HTTPRequestType.THRIFT_EXECUTE_STATEMENT, RequestIdempotency.NON_IDEMPOTENT);
    REQUEST_IDEMPOTENCY_MAP.put(HTTPRequestType.VOLUME_PUT, RequestIdempotency.NON_IDEMPOTENT);
    REQUEST_IDEMPOTENCY_MAP.put(HTTPRequestType.UNKNOWN, RequestIdempotency.NON_IDEMPOTENT);
  }

  private static final IRetryStrategy IDEMPOTENT_STRATEGY = new IdempotentRetryStrategy();
  private static final IRetryStrategy NON_IDEMPOTENT_STRATEGY = new NonIdempotentRetryStrategy();

  /** Main entry point - executes HTTP request with retry logic based on request type. */
  public static CloseableHttpResponse executeWithRetry(
      CloseableHttpClient httpClient,
      HttpUriRequest request,
      HTTPRequestType requestType,
      IDatabricksConnectionContext connectionContext)
      throws DatabricksHttpException {

    // Track retry state with array (mutable object) instead of HttpContext
    long[] accumulatedTimes = {0L, 0L}; // [tempUnavailable, rateLimit]

    RequestIdempotency idempotency = getIdempotency(requestType);
    IRetryStrategy strategy =
        (idempotency == RequestIdempotency.IDEMPOTENT)
            ? IDEMPOTENT_STRATEGY
            : NON_IDEMPOTENT_STRATEGY;

    for (int attempt = 1; attempt <= MAX_RETRIES + 1; attempt++) {
      // follow idempotent retry (exponential backoff) if executing the request throws an
      // IOException
      int retryDelayMillis =
          IDEMPOTENT_STRATEGY.retryRequestAfter(null, attempt, connectionContext);
      try {
        CloseableHttpResponse response = httpClient.execute(request);
        int statusCode = response.getStatusLine().getStatusCode();

        // Check if status code is retryable
        if (!strategy.isStatusCodeRetriable(statusCode, connectionContext)) {
          return response;
        }

        // Update accumulated time for 503/429 errors
        updateAccumulatedTime(statusCode, response, accumulatedTimes);

        // Check timeout constraints
        if (!isRetryAllowedByTimeout(statusCode, accumulatedTimes, connectionContext)) {
          return response;
        }

        // Last attempt check
        if (attempt > MAX_RETRIES) {
          return response;
        }

        // Get retry delay from strategy
        retryDelayMillis = strategy.retryRequestAfter(response, attempt, connectionContext);
        if (retryDelayMillis == -1) {
          return response; // Strategy says don't retry
        }
        response.close();
      } catch (RuntimeException e) {
        throwHttpException(e, request);
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

  // Helper methods
  private static RequestIdempotency getIdempotency(HTTPRequestType requestType) {
    return REQUEST_IDEMPOTENCY_MAP.getOrDefault(requestType, RequestIdempotency.NON_IDEMPOTENT);
  }

  private static void updateAccumulatedTime(
      int statusCode, CloseableHttpResponse response, long[] accumulatedTimes) {
    int retryInterval = extractRetryInterval(response);
    if (retryInterval <= 0) return;

    if (statusCode == HttpStatus.SC_SERVICE_UNAVAILABLE) {
      accumulatedTimes[0] += retryInterval; // tempUnavailable
    } else if (statusCode == HttpStatus.SC_TOO_MANY_REQUESTS) {
      accumulatedTimes[1] += retryInterval; // rateLimit
    }
  }

  private static boolean isRetryAllowedByTimeout(
      int statusCode, long[] accumulatedTimes, IDatabricksConnectionContext connectionContext) {
    // If no connection context, allow retry (handled by strategy)
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

  private static void throwHttpException(RuntimeException e, HttpUriRequest request)
      throws DatabricksHttpException {
    Throwable cause = e;
    while (cause != null) {
      if (cause instanceof DatabricksRetryHandlerException) {
        throw new DatabricksHttpException(
            cause.getMessage(), cause, DatabricksDriverErrorCode.INVALID_STATE);
      }
      cause = cause.getCause();
    }
    String errorMsg =
        String.format(
            "Caught error while executing http request: [%s]. Error Message: [%s]",
            RequestSanitizer.sanitizeRequest(request), e);
    LOGGER.error(e, errorMsg);
    throw new DatabricksHttpException(errorMsg, DEFAULT_HTTP_EXCEPTION_SQLSTATE);
  }
}
