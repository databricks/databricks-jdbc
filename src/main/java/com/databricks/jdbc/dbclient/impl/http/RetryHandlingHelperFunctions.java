package com.databricks.jdbc.dbclient.impl.http;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.DEFAULT_HTTP_EXCEPTION_SQLSTATE;

import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksRetryHandlerException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.util.Random;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;

/**
 * Utility class containing common retry handling helper functions used across different retry
 * strategies and handlers.
 */
public class RetryHandlingHelperFunctions {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(RetryHandlingHelperFunctions.class);

  private static final int DEFAULT_BACKOFF_FACTOR = 2;
  private static final int MIN_BACKOFF_INTERVAL = 1000; // 1s
  private static final int MAX_RETRY_INTERVAL = 10000; // 10s
  private static final String RETRY_AFTER_HEADER = "Retry-After";
  private static final Random RANDOM = new Random();

  /**
   * Calculates exponential backoff delay based on execution count.
   *
   * @param executionCount the number of retries that have been attempted (0-based)
   * @return the backoff delay in milliseconds, capped at MAX_RETRY_INTERVAL
   */
  public static int calculateExponentialBackoff(int executionCount) {
    return (int)
        Math.min(
            MIN_BACKOFF_INTERVAL * Math.pow(DEFAULT_BACKOFF_FACTOR, executionCount - 1),
            MAX_RETRY_INTERVAL);
  }

  /**
   * Extracts the retry interval from the Retry-After header in an HTTP response.
   *
   * @param response the HTTP response to extract the header from
   * @return the retry interval in seconds, or -1 if header is missing or invalid
   */
  public static int extractRetryInterval(CloseableHttpResponse response) {
    if (response.containsHeader(RETRY_AFTER_HEADER)) {
      try {
        return Integer.parseInt(response.getFirstHeader(RETRY_AFTER_HEADER).getValue().trim());
      } catch (NumberFormatException e) {
        // Invalid header value
      }
    }
    return -1;
  }

  /**
   * Gets the minimum backoff interval used in exponential backoff calculations.
   *
   * @return the minimum backoff interval in milliseconds
   */
  public static int getMinBackoffInterval() {
    return MIN_BACKOFF_INTERVAL;
  }

  /**
   * Gets the maximum retry interval cap used in exponential backoff calculations.
   *
   * @return the maximum retry interval in milliseconds
   */
  public static int getMaxRetryInterval() {
    return MAX_RETRY_INTERVAL;
  }

  /**
   * Gets the default backoff factor used in exponential backoff calculations.
   *
   * @return the backoff factor
   */
  public static int getDefaultBackoffFactor() {
    return DEFAULT_BACKOFF_FACTOR;
  }

  /**
   * Adds jitter to a delay value to avoid thundering herd problem. Returns a random value between
   * the original value and value * 1.2 (20% jitter).
   *
   * @param value the base delay value in milliseconds
   * @return a jittered delay value between value and value * 1.2
   */
  public static int addJitter(int value) {
    return (int) (value * (1.0 + (RANDOM.nextDouble() * 0.2)));
  }

  /**
   * Converts generic exceptions during HTTP request execution into standardized
   * DatabricksHttpException. Traverses exception cause chain to identify retry-specific errors and
   * logs sanitized request details.
   *
   * @param e the original exception that occurred during HTTP request execution
   * @param request the HTTP request that was being executed when the exception occurred
   * @throws DatabricksHttpException standardized exception with appropriate error code
   */
  static void throwHttpException(Exception e, HttpUriRequest request)
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
