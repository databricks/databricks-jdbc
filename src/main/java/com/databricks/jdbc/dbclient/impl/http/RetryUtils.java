package com.databricks.jdbc.dbclient.impl.http;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.DEFAULT_HTTP_EXCEPTION_SQLSTATE;

import com.databricks.jdbc.common.RequestType;
import com.databricks.jdbc.common.RetryPolicy;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksRetryHandlerException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.util.Optional;
import java.util.Random;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;

/**
 * Utility class containing common retry handling helper functions used across different retry
 * strategies and handlers.
 */
public class RetryUtils {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(RetryUtils.class);

  private static final int DEFAULT_BACKOFF_FACTOR = 2;
  private static final int MIN_BACKOFF_INTERVAL_MILLISECONDS = 1000; // 1s
  private static final int MAX_BACKOFF_INTERVAL_MILLISECONDS = 10000; // 10s
  private static final String RETRY_AFTER_HEADER = "Retry-After";
  private static final Random RANDOM = new Random();
  private static final IRetryStrategy IDEMPOTENT_STRATEGY = new IdempotentRetryStrategy();
  private static final IRetryStrategy NON_IDEMPOTENT_STRATEGY = new NonIdempotentRetryStrategy();
  public static final long REQUEST_TIMEOUT_SECONDS = 900;
  public static final long REQUEST_EXCEPTION_TIMEOUT_SECONDS = 900;

  /**
   * Calculates exponential backoff delay based on execution count with jitter to avoid thundering
   * herd problem.
   *
   * @param executionCount the number of retries that have been attempted (0-based)
   * @return the backoff delay with jitter in milliseconds, capped at MAX_RETRY_INTERVAL(without
   *     jitter)
   */
  public static int calculateExponentialBackoff(int executionCount) {
    int baseDelay =
        (int)
            Math.min(
                MIN_BACKOFF_INTERVAL_MILLISECONDS
                    * Math.pow(DEFAULT_BACKOFF_FACTOR, executionCount),
                MAX_BACKOFF_INTERVAL_MILLISECONDS);
    // Add 0-20% jitter to avoid thundering herd problem
    return (int) (baseDelay * (1.0 + (RANDOM.nextDouble() * 0.2)));
  }

  /**
   * Extracts the retry interval from the Retry-After header in an HTTP response.
   *
   * @param response the HTTP response to extract the header from
   * @return Optional containing the retry interval in milliseconds, or empty if header is missing
   *     or invalid
   */
  public static Optional<Integer> extractRetryAfterHeader(CloseableHttpResponse response) {
    if (response.containsHeader(RETRY_AFTER_HEADER)) {
      try {
        int retryAfterSeconds =
            Integer.parseInt(response.getFirstHeader(RETRY_AFTER_HEADER).getValue().trim());
        int retryAfterMillis = retryAfterSeconds * 1000;
        return Optional.of(retryAfterMillis);
      } catch (NumberFormatException e) {
        // Invalid header value
      }
    }
    return Optional.empty();
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
  static void throwDatabricksHttpException(Exception e, HttpUriRequest request)
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

  // Helper method to get retry strategy based on request type idempotency
  public static IRetryStrategy getRetryStrategy(RequestType requestType) {
    RetryPolicy retryPolicy = requestType.getRetryPolicy();
    return (retryPolicy == RetryPolicy.IDEMPOTENT) ? IDEMPOTENT_STRATEGY : NON_IDEMPOTENT_STRATEGY;
  }
}
