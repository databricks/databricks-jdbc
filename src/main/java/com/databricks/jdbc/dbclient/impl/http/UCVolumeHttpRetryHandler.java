package com.databricks.jdbc.dbclient.impl.http;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
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
  static final String RETRY_START_TIME_KEY = "retry-start-time";

  private final IDatabricksConnectionContext connectionContext;

  public UCVolumeHttpRetryHandler(IDatabricksConnectionContext connectionContext) {
    super(connectionContext);
    this.connectionContext = connectionContext;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Specifically this handles retryable http codes and setting of retry start time for UC Volume
   * operations
   */
  @Override
  public void process(HttpResponse httpResponse, HttpContext httpContext) throws IOException {
    int statusCode = httpResponse.getStatusLine().getStatusCode();
    if (!isStatusCodeRetryable(statusCode)) {
      // If the status code is not retryable, then no processing is needed for retry
      return;
    }

    Instant startTime = (Instant) httpContext.getAttribute(RETRY_START_TIME_KEY);
    if (startTime == null) {
      startTime = Instant.now();
      httpContext.setAttribute(RETRY_START_TIME_KEY, startTime);
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
   * {@inheritDoc}
   *
   * <p>Specifically, this method implements retry strategy for HTTP requests for UC Volume
   * operations
   */
  @Override
  public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
    // check if retrying this status code is supported
    int statusCode = getErrorCodeFromException(exception);
    if (!isStatusCodeRetryable(statusCode)) {
      return false;
    }

    Instant startTime = (Instant) context.getAttribute(RETRY_START_TIME_KEY);
    if (startTime == null) {
      startTime = Instant.now();
    }

    // check if we have a retry interval set from retry-after header
    int retryInterval = (int) context.getAttribute(RETRY_INTERVAL_KEY);
    long delay = calculateDelay(statusCode, executionCount, retryInterval);
    doSleepForDelay(delay);

    // Check if we are still good for retry
    long elapsedTime = Duration.between(startTime, Instant.now()).toMillis();
    return elapsedTime <= connectionContext.getUCIngestionRetryTimeoutSeconds() * 1000L;
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
