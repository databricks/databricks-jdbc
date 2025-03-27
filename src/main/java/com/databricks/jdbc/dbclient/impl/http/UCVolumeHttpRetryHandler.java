package com.databricks.jdbc.dbclient.impl.http;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.exception.DatabricksRetryHandlerException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.protocol.HttpContext;

public class UCVolumeHttpRetryHandler extends DatabricksHttpRetryHandler {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(UCVolumeHttpRetryHandler.class);
  static final String RETRY_START_TIME = "retry-start-time";

  private final IDatabricksConnectionContext connectionContext;

  public UCVolumeHttpRetryHandler(IDatabricksConnectionContext connectionContext) {
    super(connectionContext);
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
   * @see DatabricksRetryHandlerException
   */
  @Override
  public void process(HttpResponse httpResponse, HttpContext httpContext) throws IOException {
    int statusCode = httpResponse.getStatusLine().getStatusCode();
    if (!isStatusCodeRetryable(statusCode)) {
      // If the status code is not retryable, then no processing is needed for retry
      return;
    }

    Instant startTime = (Instant) httpContext.getAttribute(RETRY_START_TIME);
    if (startTime == null) {
      startTime = Instant.now();
      httpContext.setAttribute(RETRY_START_TIME, startTime);
    }

    // Extract the retry interval from the response if server supports retry after header
    int retryInterval = -1;
    if (httpResponse.containsHeader(RETRY_AFTER_HEADER)) {
      retryInterval = Integer.parseInt(httpResponse.getFirstHeader(RETRY_AFTER_HEADER).getValue());
    }

    // Set the context state
    httpContext.setAttribute(RETRY_INTERVAL_KEY, retryInterval);
  }

  /**
   * Determines whether a request should be retried after encountering an IOException.
   *
   * <p>This method implements retry strategy for HTTP requests for UC Volume operations,
   * considering various factors such as status codes, retry intervals, and execution counts.
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
   *     </ul>
   *     If all checks pass, the method updates retry counters, calculates a delay, and sleeps for
   *     the calculated duration before allowing a retry.
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

    Instant startTime = (Instant) context.getAttribute(RETRY_START_TIME);
    if (startTime == null) {
      startTime = Instant.now();
    }

    // check if retry interval is valid for 503 and 429
    int retryInterval = (int) context.getAttribute(RETRY_INTERVAL_KEY);
    long delay = calculateDelay(statusCode, executionCount, retryInterval);
    doSleepForDelay(delay);

    long elapsedTime = Duration.between(startTime, Instant.now()).toMillis();
    return elapsedTime <= connectionContext.getUCIngestionRetryTimeoutMinutes() * 60 * 1000L;
  }

  static long calculateDelay(int errorCode, int executionCount, int retryInterval) {
    switch (errorCode) {
      case HttpStatus.SC_SERVICE_UNAVAILABLE:
      case HttpStatus.SC_TOO_MANY_REQUESTS:
        if (retryInterval > 0) {
          return retryInterval;
        }
      default:
        return calculateExponentialBackoff(executionCount);
    }
  }

  /** Check if the request is retryable based on the status code and any connection preferences. */
  private boolean isStatusCodeRetryable(int statusCode) {
    return connectionContext.getUCIngestionRetriableHttpCodes().contains(statusCode);
  }
}
