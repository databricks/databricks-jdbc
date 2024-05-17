package com.databricks.jdbc.client.http;

import static com.databricks.jdbc.driver.DatabricksJdbcConstants.FAKE_SERVICE_URI_PROP_SUFFIX;
import static com.databricks.jdbc.driver.DatabricksJdbcConstants.IS_FAKE_SERVICE_TEST_PROP;

import com.databricks.jdbc.client.DatabricksHttpException;
import com.databricks.jdbc.client.IDatabricksHttpClient;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.core.UserAgent;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.UnsupportedSchemeException;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.ProxyAuthenticationStrategy;
import org.apache.http.impl.conn.DefaultSchemePortResolver;
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
  private static final String SDK_USER_AGENT = "databricks-sdk-java";
  private static final String JDBC_HTTP_USER_AGENT = "databricks-jdbc-http";
  private static final Set<Integer> RETRYABLE_HTTP_CODES = getRetryableHttpCodes();
  protected static final long DEFAULT_IDLE_CONNECTION_TIMEOUT = 5;

  private static DatabricksHttpClient instance = null;

  private static PoolingHttpClientConnectionManager connectionManager;

  private final CloseableHttpClient httpClient;

  private DatabricksHttpClient(IDatabricksConnectionContext connectionContext) {
    connectionManager = new PoolingHttpClientConnectionManager();
    connectionManager.setMaxTotal(DEFAULT_MAX_HTTP_CONNECTIONS);
    connectionManager.setDefaultMaxPerRoute(DEFAULT_MAX_HTTP_CONNECTIONS_PER_ROUTE);
    httpClient = makeClosableHttpClient(connectionContext);
  }

  @VisibleForTesting
  DatabricksHttpClient(
      CloseableHttpClient closeableHttpClient,
      PoolingHttpClientConnectionManager connectionManager) {
    DatabricksHttpClient.connectionManager = connectionManager;
    if (connectionManager != null) {
      connectionManager.setMaxTotal(DEFAULT_MAX_HTTP_CONNECTIONS);
      connectionManager.setDefaultMaxPerRoute(DEFAULT_MAX_HTTP_CONNECTIONS_PER_ROUTE);
    }
    httpClient = closeableHttpClient;
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

  private CloseableHttpClient makeClosableHttpClient(
      IDatabricksConnectionContext connectionContext) {
    HttpClientBuilder builder =
        HttpClientBuilder.create()
            .setConnectionManager(connectionManager)
            .setUserAgent(getUserAgent())
            .setDefaultRequestConfig(makeRequestConfig())
            .setRetryHandler(
                (exception, executionCount, context) -> {
                  if (executionCount > DEFAULT_RETRY_COUNT
                      || !isRetryAllowed(
                          ((HttpClientContext) context)
                              .getRequest()
                              .getRequestLine()
                              .getMethod())) {
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
                });
    if (connectionContext.getUseSystemProxy()) {
      builder.useSystemProperties();
    }
    // Override system proxy if proxy details are explicitly provided
    // If cloud fetch proxy is provided use that, else use the regular proxy
    if (connectionContext.getUseCloudFetchProxy()) {
      setProxyDetailsInHttpClient(
          builder,
          connectionContext.getCloudFetchProxyHost(),
          connectionContext.getCloudFetchProxyPort(),
          connectionContext.getUseCloudFetchProxyAuth(),
          connectionContext.getCloudFetchProxyUser(),
          connectionContext.getCloudFetchProxyPassword());
    } else if (connectionContext.getUseProxy()) {
      setProxyDetailsInHttpClient(
          builder,
          connectionContext.getProxyHost(),
          connectionContext.getProxyPort(),
          connectionContext.getUseProxyAuth(),
          connectionContext.getProxyUser(),
          connectionContext.getProxyPassword());
    } else if (Boolean.parseBoolean(System.getProperty(IS_FAKE_SERVICE_TEST_PROP))) {
      setFakeServiceRouteInHttpClient(builder);
    }

    return builder.build();
  }

  @VisibleForTesting
  public static void setProxyDetailsInHttpClient(
      HttpClientBuilder builder,
      String proxyHost,
      int proxyPort,
      Boolean useProxyAuth,
      String proxyUser,
      String proxyPassword) {
    builder.setProxy(new HttpHost(proxyHost, proxyPort));
    if (useProxyAuth) {
      CredentialsProvider credsProvider = new BasicCredentialsProvider();
      credsProvider.setCredentials(
          new AuthScope(proxyHost, proxyPort),
          new UsernamePasswordCredentials(proxyUser, proxyPassword));
      builder
          .setDefaultCredentialsProvider(credsProvider)
          .setProxyAuthenticationStrategy(new ProxyAuthenticationStrategy());
    }
  }

  @VisibleForTesting
  static void setFakeServiceRouteInHttpClient(HttpClientBuilder builder) {
    builder.setRoutePlanner(
        (host, request, context) -> {
          // Get the fake service URI for the target URI and set it as proxy
          final HttpHost proxy =
              HttpHost.create(System.getProperty(host.toURI() + FAKE_SERVICE_URI_PROP_SUFFIX));
          final HttpHost target;
          try {
            target =
                new HttpHost(
                    host.getHostName(),
                    DefaultSchemePortResolver.INSTANCE.resolve(host),
                    host.getSchemeName());
          } catch (UnsupportedSchemeException e) {
            throw new HttpException(e.getMessage());
          }
          return new HttpRoute(target, null, proxy, false);
        });
  }

  @VisibleForTesting
  static boolean isRetryAllowed(String method) {
    // For now, allowing retry only for GET which is idempotent
    return Objects.equals(HTTP_GET, method);
  }

  @VisibleForTesting
  static boolean isErrorCodeRetryable(int errCode) {
    return RETRYABLE_HTTP_CODES.contains(errCode);
  }

  public static synchronized DatabricksHttpClient getInstance(
      IDatabricksConnectionContext context) {
    if (instance == null) {
      instance = new DatabricksHttpClient(context);
    }
    return instance;
  }

  @Override
  public CloseableHttpResponse execute(HttpUriRequest request) throws DatabricksHttpException {
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

  static String getUserAgent() {
    String sdkUserAgent = UserAgent.asString();
    // Split the string into parts
    String[] parts = sdkUserAgent.split("\\s+");

    // User Agent is in format:
    // product/product-version databricks-sdk-java/sdk-version jvm/jvm-version other-info
    // Remove the SDK part from user agent
    StringBuilder mergedString = new StringBuilder();
    for (int i = 0; i < parts.length; i++) {
      if (parts[i].startsWith(SDK_USER_AGENT)) {
        mergedString.append(JDBC_HTTP_USER_AGENT);
      } else {
        mergedString.append(parts[i]);
      }
      if (i != parts.length - 1) {
        mergedString.append(" "); // Add space between parts
      }
    }
    return mergedString.toString();
  }

  /** Reset the instance of the http client. This is used for testing purposes only. */
  @VisibleForTesting
  public static synchronized void resetInstance() {
    if (instance != null) {
      try {
        instance.httpClient.close();
      } catch (IOException e) {
        LOGGER.error("Caught error while closing http client", e);
      }
      instance = null;
    }
  }
}
