package com.databricks.jdbc.client.http;

import com.databricks.jdbc.client.DatabricksHttpException;
import com.databricks.jdbc.client.IDatabricksHttpClient;
import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Http client implementation to be used for executing http requests. */
public class DatabricksHttpClient implements IDatabricksHttpClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksHttpClient.class);

  // TODO(PECO-1373): Revisit number of connections and connections per route.
  private static final int DEFAULT_MAX_HTTP_CONNECTIONS = 1000;
  private static final int DEFAULT_MAX_HTTP_CONNECTIONS_PER_ROUTE = 1000;
  private static final int DEFAULT_HTTP_CONNECTION_TIMEOUT = 60 * 1000; // ms
  private static final int DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT = 300 * 1000; // ms
  private static final int DEFAULT_BACKOFF_FACTOR = 2; // Exponential factor
  private static final int MIN_BACKOFF_INTERVAL = 1000; // 1s
  private static final int MAX_RETRY_INTERVAL = 10 * 1000; // 10s
  private static final int DEFAULT_RETRY_COUNT = 5;
  private static final String HTTP_GET = "GET";
  private static final Set<Integer> RETRYABLE_HTTP_CODES = getRetryableHttpCodes();
  private static final long DEFAULT_IDLE_CONNECTION_TIMEOUT = 5;

  private static DatabricksHttpClient instance = new DatabricksHttpClient();

  private static PoolingHttpClientConnectionManager connectionManager;

  private final CloseableHttpClient httpClient;

  private DatabricksHttpClient() {
    connectionManager = new PoolingHttpClientConnectionManager();
    connectionManager.setMaxTotal(DEFAULT_MAX_HTTP_CONNECTIONS);
    connectionManager.setDefaultMaxPerRoute(DEFAULT_MAX_HTTP_CONNECTIONS_PER_ROUTE);
    httpClient = makeClosableHttpClient();
  }

  private RequestConfig makeRequestConfig() {
    return RequestConfig.custom()
        .setConnectionRequestTimeout(DEFAULT_HTTP_CONNECTION_TIMEOUT)
        .setConnectTimeout(DEFAULT_HTTP_CONNECTION_TIMEOUT)
        .setSocketTimeout(DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT)
        .build();
  }

  private static Set<Integer> getRetryableHttpCodes() {
    Set<Integer> retryableCodes = new HashSet<>();
    retryableCodes.add(408); // request timeout
    retryableCodes.add(425); // too early response
    retryableCodes.add(429); // too many requests
    retryableCodes.add(500); // internal server error
    retryableCodes.add(502); // bad gateway (should this be retried?)
    retryableCodes.add(503); // service unavailable
    retryableCodes.add(504); // gateway timeout
    return retryableCodes;
  }

  private CloseableHttpClient makeClosableHttpClient() {
    return HttpClientBuilder.create()
        .setConnectionManager(connectionManager)
        // TODO: set appropriate user agent
        .setUserAgent("jdbc/databricks")
        .setDefaultRequestConfig(makeRequestConfig())
        .setRetryHandler(
            (exception, executionCount, context) -> {
              if (executionCount > DEFAULT_RETRY_COUNT
                  || !isRetryAllowed(
                      ((HttpClientContext) context).getRequest().getRequestLine().getMethod())) {
                return false;
              }
              long nextBackOffDelay =
                  MIN_BACKOFF_INTERVAL
                      * (long) Math.pow(DEFAULT_BACKOFF_FACTOR, executionCount - 1);
              long delay = Math.min(MAX_RETRY_INTERVAL, nextBackOffDelay);
              try {
                Thread.sleep(delay);
              } catch (InterruptedException e) {
                // Do nothing
              }
              return true;
            })
        .addInterceptorFirst(
            new HttpResponseInterceptor() {
              // Handling 500 and 503 explicitly for retry
              @Override
              public void process(HttpResponse httpResponse, HttpContext httpContext)
                  throws HttpException, IOException {
                if (isErrorCodeRetryable(httpResponse.getStatusLine().getStatusCode())) {
                  throw new IOException("Retry http request");
                }
              }
            })
        .build();
  }

  private static boolean isRetryAllowed(String method) {
    // For now, allowing retry only for GET which is idempotent
    return Objects.equals(HTTP_GET, method);
  }

  private static boolean isErrorCodeRetryable(int errCode) {
    return RETRYABLE_HTTP_CODES.contains(errCode);
  }

  public static synchronized DatabricksHttpClient getInstance() {
    return instance;
  }

  @Override
  public HttpResponse execute(HttpUriRequest request) throws DatabricksHttpException {
    LOGGER.debug("Executing HTTP request [{}]", RequestSanitizer.sanitizeRequest(request));
    // TODO: add retries and error handling
    try {
      return httpClient.execute(request);
    } catch (IOException e) {
      String errorMsg =
          String.format(
              "Caught error while executing http request: [%s]",
              RequestSanitizer.sanitizeRequest(request));
      LOGGER.error(errorMsg, e);
      throw new DatabricksHttpException(errorMsg, e);
    }
  }

  @Override
  public void closeExpiredAndIdleConnections() {
    if (connectionManager != null) {
      synchronized (connectionManager) {
        LOGGER.debug("connection pool stats: {}", connectionManager.getTotalStats());
        connectionManager.closeExpiredConnections();
        connectionManager.closeIdleConnections(DEFAULT_IDLE_CONNECTION_TIMEOUT, TimeUnit.SECONDS);
      }
    }
  }
}
