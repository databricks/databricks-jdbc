package com.databricks.jdbc.dbclient.impl.http;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.THRIFT_ERROR_MESSAGE_HEADER;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
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
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksHttpRetryHandler.class);
  private static final String RETRY_INTERVAL_KEY = "retryInterval";
  private static final String TEMP_UNAVAILABLE_RETRY_COUNT_KEY = "tempUnavailableRetryCount";
  private static final String RATE_LIMIT_RETRY_COUNT_KEY = "rateLimitRetryCount";
  private static final String RETRY_AFTER_HEADER = "Retry-After";
  private static final int DEFAULT_BACKOFF_FACTOR = 2; // Exponential factor
  private static final int MIN_BACKOFF_INTERVAL = 1000; // 1s
  private static final int MAX_RETRY_INTERVAL = 10 * 1000; // 10s
  public static final int DEFAULT_RETRY_COUNT = 5;
  private final IDatabricksConnectionContext connectionContext;

  public DatabricksHttpRetryHandler(IDatabricksConnectionContext connectionContext) {
    this.connectionContext = connectionContext;
  }

  /**
   * Processes an HTTP response to handle retryable status codes and set up retry logic.
   *
   * <p>This method is responsible for examining the HTTP response, determining if it's retryable,
   * and setting up the necessary context for potential retry attempts.
   *
   * @param httpResponse The HTTP response to be processed.
   * @param httpContext The HTTP context associated with the request and response.
   * @throws IOException If there's an issue processing the response.
   * @throws DatabricksRetryHandlerException If the status code is retryable, triggering the retry
   *     mechanism.
   * @implNote The method performs the following steps:
   *     <ul>
   *       <li>Checks if the status code is retryable.
   *       <li>Extracts the retry interval from the response for status codes 503 and 429.
   *       <li>Sets up the context state for retry logic.
   *       <li>Throws a {@code DatabricksRetryHandlerException} to trigger the retry mechanism,
   *           including relevant error information from the response.
   *     </ul>
   *
   * @implSpec This method adheres to the contract specified by its parent interface or class. It's
   *     designed to be called as part of the HTTP response handling pipeline.
   * @see #isStatusCodeRetryable(int)
   * @see #initializeRetryCountsIfNotExist(HttpContext)
   * @see DatabricksRetryHandlerException
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
    String errorReason;
    if (httpResponse.containsHeader(THRIFT_ERROR_MESSAGE_HEADER)) {
      errorReason = httpResponse.getFirstHeader(THRIFT_ERROR_MESSAGE_HEADER).getValue();
    } else {
      errorReason = httpResponse.getStatusLine().getReasonPhrase();
    }
    String errorMessage =
        String.format(
            "Retry failure. HTTP response code: %s, Error Message: %s", statusCode, errorReason);
    LOGGER.debug(errorMessage);
    throw new DatabricksRetryHandlerException(errorMessage, statusCode);
  }

  /**
   * Determines whether a request should be retried after encountering an IOException.
   *
   * <p>This method implements a comprehensive retry strategy for HTTP requests, considering various
   * factors such as status codes, retry intervals, and execution counts.
   *
   * @param exception The IOException encountered during the request execution.
   * @param executionCount The number of times this request has been executed.
   * @param context The HttpContext containing attributes related to the request and retry logic.
   * @return boolean True if the request should be retried, false otherwise.
   * @throws RuntimeException If an invalid retry interval is found in the context for status codes
   *     503 (Service Unavailable) or 429 (Too Many Requests).
   * @implNote The method performs the following checks:
   *     <ul>
   *       <li>Verifies if the status code is retryable.
   *       <li>Checks the validity of retry intervals for specific status codes.
   *       <li>Ensures retry timeouts haven't been exceeded for temporary unavailability (503) and
   *           rate limiting (429).
   *       <li>Verifies that the execution count hasn't exceeded the maximum allowed retries.
   *       <li>Confirms that the HTTP method of the request is retryable.
   *     </ul>
   *     If all checks pass, the method updates retry counters, calculates a delay, and sleeps for
   *     the calculated duration before allowing a retry.
   * @see #isStatusCodeRetryable(int)
   * @see #isRequestMethodRetryable(String)
   * @see #calculateDelay(int, int, int)
   * @see #sleepForDelay(long)
   */
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

  private static void sleepForDelay(long delayInSeconds) {
    try {
      Thread.sleep(delayInSeconds * 1000);
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
