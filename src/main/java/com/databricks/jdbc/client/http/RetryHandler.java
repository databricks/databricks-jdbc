package com.databricks.jdbc.client.http;

import static com.databricks.jdbc.client.http.DatabricksHttpClient.*;

import com.databricks.jdbc.client.DatabricksRetryHandlerException;
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

  public static Boolean isRetryAllowedHttp(
      int executionCount,
      HttpContext context,
      int errCode,
      long temporarilyUnavailableRetryInterval,
      long temporarilyUnavailableRetryTimeout,
      long rateLimitRetryInterval,
      long rateLimitRetryTimeout,
      long temporarilyUnavailableRetryCount,
      long rateLimitRetryCount) {

    boolean isErrorCode503Or429 = (errCode == 503 || errCode == 429);
    boolean isExecutionCountExceeded = executionCount > DEFAULT_RETRY_COUNT;
    boolean isMethodRetryNotAllowed =
        !isRetryAllowed(((HttpClientContext) context).getRequest().getRequestLine().getMethod());

    boolean is503RetryTimeoutExceeded =
        (errCode == 503
            && temporarilyUnavailableRetryTimeout > 0
            && temporarilyUnavailableRetryCount * temporarilyUnavailableRetryInterval
                > temporarilyUnavailableRetryTimeout);

    boolean is429RetryTimeoutExceeded =
        (errCode == 429
            && rateLimitRetryTimeout > 0
            && rateLimitRetryCount * rateLimitRetryInterval > rateLimitRetryTimeout);

    boolean shouldNotRetry =
        (!isErrorCode503Or429 && isExecutionCountExceeded)
            || isMethodRetryNotAllowed
            || is503RetryTimeoutExceeded
            || is429RetryTimeoutExceeded;

    return !shouldNotRetry;
  }

  public static void sleepForDelay(long delay) {
    try {
      Thread.sleep(delay);
    } catch (InterruptedException e) {
      // Do nothing
    }
  }
}
