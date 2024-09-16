package com.databricks.jdbc.dbclient.impl.http;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.ErrorTypes;
import com.databricks.jdbc.exception.DatabricksRetryHandlerException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Objects;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.protocol.HttpContext;

public class DatabricksHttpRetryHandler
    implements HttpResponseInterceptor, HttpRequestRetryHandler {

  public static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksHttpRetryHandler.class);
  private static final String RETRY_INTERVAL_KEY = "retryInterval";
  private static final String TEMP_UNAVAILABLE_RETRY_COUNT_KEY = "tempUnavailableRetryCount";
  private static final String RATE_LIMIT_RETRY_COUNT_KEY = "rateLimitRetryCount";
  private static final String RETRY_AFTER_HEADER = "Retry-After";
  private static final String THRIFT_ERROR_MESSAGE_HEADER = "X-Thriftserver-Error-Message";
  private static final int DEFAULT_BACKOFF_FACTOR = 2; // Exponential factor
  private static final int MIN_BACKOFF_INTERVAL = 1000; // 1s
  private static final int MAX_RETRY_INTERVAL = 10 * 1000; // 10s
  public static final int DEFAULT_RETRY_COUNT = 5;
  private final IDatabricksConnectionContext connectionContext;

  public DatabricksHttpRetryHandler(IDatabricksConnectionContext connectionContext) {
    this.connectionContext = connectionContext;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Processing related to HTTP retry.
   */
  @Override
  public void process(HttpResponse httpResponse, HttpContext httpContext) throws IOException {
    int statusCode = httpResponse.getStatusLine().getStatusCode();
    if (!isStatusCodeRetryable(statusCode)) {
      // If the status code is not retryable, then no processing is needed for retry
      return;
    }

    // Extract the retry interval from the response. Relevant for 503 and 429 status codes
    int retryInterval = -1;
    if (httpResponse.containsHeader(RETRY_AFTER_HEADER)) {
      retryInterval = Integer.parseInt(httpResponse.getFirstHeader(RETRY_AFTER_HEADER).getValue());
    }

    // Set the context state
    httpContext.setAttribute(RETRY_INTERVAL_KEY, retryInterval);
    initializeRetryCountsIfNotExist(httpContext);

    // Throw an exception to trigger the retry mechanism
    if (httpResponse.containsHeader(THRIFT_ERROR_MESSAGE_HEADER)) {
      throw new DatabricksRetryHandlerException(
          "HTTP Response code: "
              + statusCode
              + ", Error message: "
              + httpResponse.getFirstHeader(THRIFT_ERROR_MESSAGE_HEADER).getValue(),
          statusCode,
          connectionContext,
          ErrorTypes.HTTP_RETRY_ERROR,
          "");
    } else {
      throw new DatabricksRetryHandlerException(
          "HTTP Response code: "
              + statusCode
              + ", Error Message: "
              + httpResponse.getStatusLine().getReasonPhrase(),
          statusCode,
          connectionContext,
          ErrorTypes.HTTP_RETRY_ERROR,
          "");
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
    // check if retrying this status code is supported
    int statusCode = getErrorCodeFromException(exception);
    if (!isStatusCodeRetryable(statusCode)) {
      return false;
    }

    // check if retry interval is valid for 503 and 429
    int retryInterval = (int) context.getAttribute(RETRY_INTERVAL_KEY);
    if ((statusCode == HttpStatus.SC_SERVICE_UNAVAILABLE
            || statusCode == HttpStatus.SC_TOO_MANY_REQUESTS)
        && retryInterval == -1) {
      throw new RuntimeException(
          "Invalid retry interval in the context "
              + context
              + " for the error: "
              + exception.getMessage());
    }

    int tempUnavailableRetryCount = (int) context.getAttribute(TEMP_UNAVAILABLE_RETRY_COUNT_KEY);
    int rateLimitRetryCount = (int) context.getAttribute(RATE_LIMIT_RETRY_COUNT_KEY);

    // check if retry timeout has been hit for error code 503
    if (statusCode == HttpStatus.SC_SERVICE_UNAVAILABLE
        && tempUnavailableRetryCount * retryInterval
            > connectionContext.getTemporarilyUnavailableRetryTimeout()) {
      LOGGER.warn(
          "TemporarilyUnavailableRetry timeout "
              + connectionContext.getTemporarilyUnavailableRetryTimeout()
              + " has been hit for the error: "
              + exception.getMessage());
      return false;
    }

    // check if retry timeout has been hit for error code 429
    if (statusCode == HttpStatus.SC_TOO_MANY_REQUESTS
        && rateLimitRetryCount * retryInterval > connectionContext.getRateLimitRetryTimeout()) {
      LOGGER.warn(
          "RateLimitRetry timeout "
              + connectionContext.getTemporarilyUnavailableRetryTimeout()
              + " has been hit for the error: "
              + exception.getMessage());
      return false;
    }

    // check the execution count and request method
    boolean isExecutionCountExceeded = executionCount > DEFAULT_RETRY_COUNT;
    boolean isRequestMethodRetryable =
        isRequestMethodRetryable(
            ((HttpClientContext) context).getRequest().getRequestLine().getMethod());
    if (isExecutionCountExceeded || !isRequestMethodRetryable) {
      return false;
    }

    // if the control has reached here, then we can retry the request
    // update the context state
    if (statusCode == HttpStatus.SC_SERVICE_UNAVAILABLE) {
      tempUnavailableRetryCount++;
      context.setAttribute(TEMP_UNAVAILABLE_RETRY_COUNT_KEY, tempUnavailableRetryCount);
    } else if (statusCode == HttpStatus.SC_TOO_MANY_REQUESTS) {
      rateLimitRetryCount++;
      context.setAttribute(RATE_LIMIT_RETRY_COUNT_KEY, rateLimitRetryCount);
    }

    // calculate the delay and sleep for that duration
    long delay = calculateDelay(statusCode, executionCount, retryInterval);
    sleepForDelay(delay);

    return true;
  }

  @VisibleForTesting
  static boolean isRequestMethodRetryable(String method) {
    return Objects.equals(HttpGet.METHOD_NAME, method)
        || Objects.equals(HttpPost.METHOD_NAME, method)
        || Objects.equals(HttpPut.METHOD_NAME, method);
  }

  @VisibleForTesting
  static long calculateDelay(int errorCode, int executionCount, int retryInterval) {
    switch (errorCode) {
      case HttpStatus.SC_SERVICE_UNAVAILABLE:
      case HttpStatus.SC_TOO_MANY_REQUESTS:
        return retryInterval;
      default:
        return calculateExponentialBackoff(executionCount);
    }
  }

  private static long calculateExponentialBackoff(int executionCount) {
    return Math.min(
        MIN_BACKOFF_INTERVAL * (long) Math.pow(DEFAULT_BACKOFF_FACTOR, executionCount),
        MAX_RETRY_INTERVAL);
  }

  private static int getErrorCodeFromException(IOException exception) {
    if (exception instanceof DatabricksRetryHandlerException) {
      return ((DatabricksRetryHandlerException) exception).getErrCode();
    }
    return 0;
  }

  private static void initializeRetryCountsIfNotExist(HttpContext httpContext) {
    if (httpContext.getAttribute(TEMP_UNAVAILABLE_RETRY_COUNT_KEY) == null) {
      httpContext.setAttribute(TEMP_UNAVAILABLE_RETRY_COUNT_KEY, 0);
    }
    if (httpContext.getAttribute(RATE_LIMIT_RETRY_COUNT_KEY) == null) {
      httpContext.setAttribute(RATE_LIMIT_RETRY_COUNT_KEY, 0);
    }
  }

  private static void sleepForDelay(long delay) {
    try {
      Thread.sleep(delay * 1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt(); // Restore the interrupt status
      throw new RuntimeException("Sleep interrupted", e);
    }
  }

  /** Check if the request is retryable based on the status code and any connection preferences. */
  private boolean isStatusCodeRetryable(int statusCode) {
    return (statusCode == HttpStatus.SC_SERVICE_UNAVAILABLE
            && connectionContext.shouldRetryTemporarilyUnavailableError())
        || (statusCode == HttpStatus.SC_TOO_MANY_REQUESTS
            && connectionContext.shouldRetryRateLimitError());
  }
}
