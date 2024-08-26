package com.databricks.jdbc.dbclient.impl.http;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.DEFAULT_RETRY_COUNT;
import static com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClient.*;

import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksRetryHandlerException;
import java.io.IOException;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.protocol.HttpContext;

public class RetryHandler {

  public static int getErrorCode(IOException exception) {
    if (exception instanceof DatabricksRetryHandlerException) {
      return ((DatabricksRetryHandlerException) exception).getErrCode();
    }
    return 0;
  }

  public static boolean isRetryAllowedHttp(
      int executionCount,
      HttpContext context,
      int errCode,
      boolean shouldRetryTemporarilyUnavailableError,
      long temporarilyUnavailableRetryTimeout,
      boolean shouldRetryRateLimitError,
      long rateLimitRetryTimeout,
      long temporarilyUnavailableRetryCount,
      long rateLimitRetryCount,
      long retryInterval,
      String originalErrorMessage)
      throws DatabricksHttpException {

    if (isImmediateRetryNotAllowed(
        errCode,
        shouldRetryRateLimitError,
        shouldRetryTemporarilyUnavailableError,
        retryInterval)) {
      throw new DatabricksHttpException(
          "HTTP retry after response received with no Retry-After header. " + originalErrorMessage);
    }

    if (isTemporarilyUnavailableRetryTimeoutExceeded(
        errCode,
        temporarilyUnavailableRetryCount,
        retryInterval,
        temporarilyUnavailableRetryTimeout)) {
      throw new DatabricksHttpException(
          String.format(
              "TemporarilyUnavailableRetry timeout of %s seconds has been hit. "
                  + originalErrorMessage,
              temporarilyUnavailableRetryTimeout));
    }

    if (isRateLimitRetryTimeoutExceeded(
        errCode, rateLimitRetryCount, retryInterval, rateLimitRetryTimeout)) {
      throw new DatabricksHttpException(
          String.format(
              "RateLimitRetry timeout of %s seconds has been hit. " + originalErrorMessage,
              rateLimitRetryTimeout));
    }

    if (isRetryDisabledButReceivedResponse(
        errCode,
        shouldRetryTemporarilyUnavailableError,
        shouldRetryRateLimitError,
        retryInterval)) {
      String retry = errCode == 503 ? "TemporarilyUnavailableRetry" : "RateLimitRetry";
      throw new DatabricksHttpException(
          retry
              + " is disabled, but received a HTTP retry after response. "
              + originalErrorMessage);
    }

    return isRetryAllowedBasedOnConditions(
        executionCount,
        context,
        errCode,
        shouldRetryTemporarilyUnavailableError,
        temporarilyUnavailableRetryTimeout,
        shouldRetryRateLimitError,
        rateLimitRetryTimeout,
        temporarilyUnavailableRetryCount,
        rateLimitRetryCount,
        retryInterval);
  }

  static boolean isImmediateRetryNotAllowed(
      int errCode,
      boolean shouldRetryRateLimitError,
      boolean shouldRetryTemporarilyUnavailableError,
      long retryInterval) {
    return retryInterval == -1
        && ((errCode == 429 && shouldRetryRateLimitError)
            || (errCode == 503 && shouldRetryTemporarilyUnavailableError));
  }

  static boolean isTemporarilyUnavailableRetryTimeoutExceeded(
      int errCode,
      long temporarilyUnavailableRetryCount,
      long retryInterval,
      long temporarilyUnavailableRetryTimeout) {
    return (errCode == 503)
        && temporarilyUnavailableRetryCount * retryInterval > temporarilyUnavailableRetryTimeout;
  }

  static boolean isRateLimitRetryTimeoutExceeded(
      int errCode, long rateLimitRetryCount, long retryInterval, long rateLimitRetryTimeout) {
    return (errCode == 429) && rateLimitRetryCount * retryInterval > rateLimitRetryTimeout;
  }

  static boolean isRetryDisabledButReceivedResponse(
      int errCode,
      boolean shouldRetryTemporarilyUnavailableError,
      boolean shouldRetryRateLimitError,
      long retryInterval) {
    return ((errCode == 503 && !shouldRetryTemporarilyUnavailableError && retryInterval != -1)
        || (errCode == 429 && !shouldRetryRateLimitError && retryInterval != -1));
  }

  static boolean isRetryAllowedBasedOnConditions(
      int executionCount,
      HttpContext context,
      int errCode,
      boolean shouldRetryTemporarilyUnavailableError,
      long temporarilyUnavailableRetryTimeout,
      boolean shouldRetryRateLimitError,
      long rateLimitRetryTimeout,
      long temporarilyUnavailableRetryCount,
      long rateLimitRetryCount,
      long retryInterval) {

    boolean isErrorCode503Or429 = (errCode == 503 || errCode == 429);
    boolean isExecutionCountExceeded = executionCount > DEFAULT_RETRY_COUNT;
    boolean isMethodRetryNotAllowed =
        !isRetryAllowed(((HttpClientContext) context).getRequest().getRequestLine().getMethod());

    boolean is503RetryTimeoutExceeded =
        (errCode == 503
            && shouldRetryTemporarilyUnavailableError
            && temporarilyUnavailableRetryTimeout > 0
            && temporarilyUnavailableRetryCount * retryInterval
                > temporarilyUnavailableRetryTimeout);

    boolean is429RetryTimeoutExceeded =
        (errCode == 429
            && shouldRetryRateLimitError
            && rateLimitRetryTimeout > 0
            && rateLimitRetryCount * retryInterval > rateLimitRetryTimeout);

    return !((!isErrorCode503Or429 && isExecutionCountExceeded)
        || isMethodRetryNotAllowed
        || is503RetryTimeoutExceeded
        || is429RetryTimeoutExceeded);
  }

  public static void sleepForDelay(long delay) {
    try {
      Thread.sleep(delay * 1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt(); // Restore the interrupt status
      throw new RuntimeException("Sleep interrupted", e);
    }
  }
}
