package com.databricks.jdbc.client.http;

import static com.databricks.jdbc.client.http.RetryHandler.*;
import static com.databricks.jdbc.driver.DatabricksJdbcConstants.*;
import static io.netty.util.NetUtil.LOCALHOST;

import com.databricks.jdbc.client.DatabricksHttpException;
import com.databricks.jdbc.client.DatabricksRetryHandlerException;
import com.databricks.jdbc.client.IDatabricksHttpClient;
import com.databricks.jdbc.commons.LogLevel;
import com.databricks.jdbc.commons.util.LoggingUtil;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.ProxyConfig;
import com.databricks.sdk.core.UserAgent;
import com.databricks.sdk.core.utils.ProxyUtils;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.UnsupportedSchemeException;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.DefaultSchemePortResolver;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLContextBuilder;

/** Http client implementation to be used for executing http requests. */
public class DatabricksHttpClient implements IDatabricksHttpClient {

  // Context attribute keys
  private static final String RETRY_INTERVAL_KEY = "retryInterval";
  private static final String TEMP_UNAVAILABLE_RETRY_COUNT_KEY = "tempUnavailableRetryCount";
  private static final String RATE_LIMIT_RETRY_COUNT_KEY = "rateLimitRetryCount";

  private static final int DEFAULT_MAX_HTTP_CONNECTIONS = 1000;
  private static final int DEFAULT_MAX_HTTP_CONNECTIONS_PER_ROUTE = 1000;
  private static final int DEFAULT_HTTP_CONNECTION_TIMEOUT = 60 * 1000; // ms
  private static final int DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT = 300 * 1000; // ms
  public static final int DEFAULT_BACKOFF_FACTOR = 2; // Exponential factor
  public static final int MIN_BACKOFF_INTERVAL = 1000; // 1s
  public static final int MAX_RETRY_INTERVAL = 10 * 1000; // 10s
  private static final String HTTP_GET = "GET";
  private static final String HTTP_POST = "POST";
  private static final String HTTP_PUT = "PUT";
  private static final String SDK_USER_AGENT = "databricks-sdk-java";
  private static final String JDBC_HTTP_USER_AGENT = "databricks-jdbc-http";

  // TODO: Consider other HTTP codes for retries (408, 425, 500, 502, 504)
  private static final Set<Integer> RETRYABLE_HTTP_CODES = Set.of(503, 429);

  private static final ConcurrentHashMap<String, DatabricksHttpClient> instances =
      new ConcurrentHashMap<>();
  private static final String RETRY_AFTER_HEADER = "Retry-After";
  private static final String THRIFT_ERROR_MESSAGE_HEADER = "X-Thriftserver-Error-Message";

  private static PoolingHttpClientConnectionManager connectionManager;

  private final CloseableHttpClient httpClient;

  private static boolean shouldRetryTemporarilyUnavailableError;
  private static int temporarilyUnavailableRetryTimeout;
  private static boolean shouldRetryRateLimitError;
  private static int rateLimitRetryTimeout;
  protected static int idleHttpConnectionExpiry;
  private CloseableHttpClient httpDisabledSSLClient;

  private DatabricksHttpClient(IDatabricksConnectionContext connectionContext) {
    initializeConnectionManager();
    shouldRetryTemporarilyUnavailableError =
        connectionContext.shouldRetryTemporarilyUnavailableError();
    temporarilyUnavailableRetryTimeout = connectionContext.getTemporarilyUnavailableRetryTimeout();
    shouldRetryRateLimitError = connectionContext.shouldRetryRateLimitError();
    rateLimitRetryTimeout = connectionContext.getRateLimitRetryTimeout();
    httpClient = makeClosableHttpClient(connectionContext);
    httpDisabledSSLClient = makeClosableDisabledSslHttpClient();
    idleHttpConnectionExpiry = connectionContext.getIdleHttpConnectionExpiry();
  }

  @VisibleForTesting
  DatabricksHttpClient(
      CloseableHttpClient closeableHttpClient,
      PoolingHttpClientConnectionManager connectionManager) {
    DatabricksHttpClient.connectionManager = connectionManager;
    initializeConnectionManager();
    this.httpClient = closeableHttpClient;
  }

  private static void initializeConnectionManager() {
    if (connectionManager == null) {
      connectionManager = new PoolingHttpClientConnectionManager();
    }
    connectionManager.setMaxTotal(DEFAULT_MAX_HTTP_CONNECTIONS);
    connectionManager.setDefaultMaxPerRoute(DEFAULT_MAX_HTTP_CONNECTIONS_PER_ROUTE);
  }

  private RequestConfig makeRequestConfig() {
    return RequestConfig.custom()
        .setConnectionRequestTimeout(DEFAULT_HTTP_CONNECTION_TIMEOUT)
        .setConnectTimeout(DEFAULT_HTTP_CONNECTION_TIMEOUT)
        .setSocketTimeout(DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT)
        .build();
  }

  @VisibleForTesting
  long calculateDelay(int errCode, int executionCount, int retryInterval) {
    long delay;
    switch (errCode) {
      case 503:
      case 429:
        delay = retryInterval;
        break;
      default:
        delay =
            Math.min(
                MIN_BACKOFF_INTERVAL * (long) Math.pow(DEFAULT_BACKOFF_FACTOR, executionCount),
                MAX_RETRY_INTERVAL);
        break;
    }
    return delay;
  }

  private CloseableHttpClient makeClosableHttpClient(
      IDatabricksConnectionContext connectionContext) {
    HttpClientBuilder builder =
        HttpClientBuilder.create()
            .setConnectionManager(connectionManager)
            .setUserAgent(getUserAgent())
            .setDefaultRequestConfig(makeRequestConfig())
            .setRetryHandler(this::handleRetry)
            .addInterceptorFirst(this::handleResponseInterceptor);
    setupProxy(connectionContext, builder);
    if (Boolean.parseBoolean(System.getProperty(IS_FAKE_SERVICE_TEST_PROP))) {
      setFakeServiceRouteInHttpClient(builder);
    }
    return builder.build();
  }

  private CloseableHttpClient makeClosableDisabledSslHttpClient() {
    try {
      // Create SSL context that trusts all certificates
      SSLContext sslContext =
          new SSLContextBuilder().loadTrustMaterial(null, (chain, authType) -> true).build();

      // Create HttpClient with the SSL context
      return HttpClientBuilder.create()
          .setSSLContext(sslContext)
          .setSSLHostnameVerifier(new NoopHostnameVerifier())
          .build();
    } catch (NoSuchAlgorithmException | KeyManagementException | KeyStoreException e) {
      LoggingUtil.log(
          LogLevel.DEBUG,
          String.format(
              "Error in creating HttpClient with the SSL context [{%s}]", e.getMessage()));
    }
    return null;
  }

  private boolean handleRetry(IOException exception, int executionCount, HttpContext context) {
    int errCode = getErrorCode(exception);
    if (!isErrorCodeRetryable(errCode)) {
      return false;
    }
    int retryInterval = (int) context.getAttribute(RETRY_INTERVAL_KEY);

    long tempUnavailableRetryCount = (long) context.getAttribute(TEMP_UNAVAILABLE_RETRY_COUNT_KEY);
    long rateLimitRetryCount = (long) context.getAttribute(RATE_LIMIT_RETRY_COUNT_KEY);

    try {
      if (!isRetryAllowedHttp(
          executionCount,
          context,
          errCode,
          shouldRetryTemporarilyUnavailableError,
          temporarilyUnavailableRetryTimeout,
          shouldRetryRateLimitError,
          rateLimitRetryTimeout,
          tempUnavailableRetryCount,
          rateLimitRetryCount,
          retryInterval,
          exception.getMessage())) {
        return false;
      }
    } catch (DatabricksHttpException e) {
      throw new RuntimeException(e);
    }

    if (errCode == 503) {
      tempUnavailableRetryCount++;
      context.setAttribute(TEMP_UNAVAILABLE_RETRY_COUNT_KEY, tempUnavailableRetryCount);
    } else if (errCode == 429) {
      rateLimitRetryCount++;
      context.setAttribute(RATE_LIMIT_RETRY_COUNT_KEY, rateLimitRetryCount);
    }

    long delay = calculateDelay(errCode, executionCount, retryInterval);
    sleepForDelay(delay);
    return true;
  }

  private void handleResponseInterceptor(HttpResponse httpResponse, HttpContext httpContext)
      throws IOException {
    int errCode = httpResponse.getStatusLine().getStatusCode();
    if (isErrorCodeRetryable(errCode)) {
      int retryInterval = -1;
      if (httpResponse.containsHeader(RETRY_AFTER_HEADER)) {
        retryInterval =
            Integer.parseInt(httpResponse.getFirstHeader(RETRY_AFTER_HEADER).getValue());
        httpContext.setAttribute(RETRY_INTERVAL_KEY, retryInterval);
      } else {
        httpContext.setAttribute(RETRY_INTERVAL_KEY, -1);
      }

      if (retryInterval == -1 && !isRetryableError(errCode)) {
        return;
      }

      initializeRetryCounts(httpContext);

      if (httpResponse.containsHeader(THRIFT_ERROR_MESSAGE_HEADER)) {
        String errorMessage = httpResponse.getFirstHeader(THRIFT_ERROR_MESSAGE_HEADER).getValue();
        throw new DatabricksRetryHandlerException(
            "HTTP Response code: " + errCode + ", Error message: " + errorMessage, errCode);
      }
      throw new DatabricksRetryHandlerException(
          "HTTP Response code: "
              + errCode
              + ", Error Message: "
              + httpResponse.getStatusLine().getReasonPhrase(),
          errCode);
    }
  }

  private boolean isRetryableError(int errCode) {
    return (errCode == 503 && shouldRetryTemporarilyUnavailableError)
        || (errCode == 429 && shouldRetryRateLimitError);
  }

  private void initializeRetryCounts(HttpContext httpContext) {
    if (httpContext.getAttribute(TEMP_UNAVAILABLE_RETRY_COUNT_KEY) == null) {
      httpContext.setAttribute(TEMP_UNAVAILABLE_RETRY_COUNT_KEY, 0L);
    }
    if (httpContext.getAttribute(RATE_LIMIT_RETRY_COUNT_KEY) == null) {
      httpContext.setAttribute(RATE_LIMIT_RETRY_COUNT_KEY, 0L);
    }
  }

  @VisibleForTesting
  public static void setupProxy(
      IDatabricksConnectionContext connectionContext, HttpClientBuilder builder) {
    String proxyHost = null;
    Integer proxyPort = null;
    String proxyUser = null;
    String proxyPassword = null;
    ProxyConfig.ProxyAuthType proxyAuth = connectionContext.getProxyAuthType();
    // System proxy is handled by the SDK.
    // If proxy details are explicitly provided use those for the connection.
    if (connectionContext.getUseCloudFetchProxy()) {
      proxyHost = connectionContext.getCloudFetchProxyHost();
      proxyPort = connectionContext.getCloudFetchProxyPort();
      proxyUser = connectionContext.getCloudFetchProxyUser();
      proxyPassword = connectionContext.getCloudFetchProxyPassword();
      proxyAuth = connectionContext.getCloudFetchProxyAuthType();
    } else if (connectionContext.getUseProxy()) {
      proxyHost = connectionContext.getProxyHost();
      proxyPort = connectionContext.getProxyPort();
      proxyUser = connectionContext.getProxyUser();
      proxyPassword = connectionContext.getProxyPassword();
      proxyAuth = connectionContext.getProxyAuthType();
    }
    if (proxyHost != null || connectionContext.getUseSystemProxy()) {
      ProxyConfig proxyConfig =
          new ProxyConfig(new DatabricksConfig())
              .setUseSystemProperties(connectionContext.getUseSystemProxy())
              .setHost(proxyHost)
              .setPort(proxyPort)
              .setUsername(proxyUser)
              .setPassword(proxyPassword)
              .setProxyAuthType(proxyAuth);
      ProxyUtils.setupProxy(proxyConfig, builder);
    }
  }

  @VisibleForTesting
  static void setFakeServiceRouteInHttpClient(HttpClientBuilder builder) {
    builder.setRoutePlanner(
        (host, request, context) -> {
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

          if (LOCALHOST.getHostName().equalsIgnoreCase(host.getHostName())) {
            // If the target host is localhost, then no need to set proxy
            return new HttpRoute(target, null, false);
          }

          // Get the fake service URI for the target URI and set it as proxy
          final HttpHost proxy =
              HttpHost.create(System.getProperty(host.toURI() + FAKE_SERVICE_URI_PROP_SUFFIX));

          return new HttpRoute(target, null, proxy, false);
        });
  }

  @VisibleForTesting
  static boolean isRetryAllowed(String method) {
    return Objects.equals(HTTP_GET, method)
        || Objects.equals(HTTP_POST, method)
        || Objects.equals(HTTP_PUT, method);
  }

  @VisibleForTesting
  static boolean isErrorCodeRetryable(int errCode) {
    return RETRYABLE_HTTP_CODES.contains(errCode);
  }

  public static synchronized DatabricksHttpClient getInstance(
      IDatabricksConnectionContext context) {
    String contextKey = Integer.toString(context.hashCode());
    return instances.computeIfAbsent(contextKey, k -> new DatabricksHttpClient(context));
  }

  @Override
  public CloseableHttpResponse execute(HttpUriRequest request) throws DatabricksHttpException {
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format("Executing HTTP request [{%s}]", RequestSanitizer.sanitizeRequest(request)));
    try {
      return httpClient.execute(request);
    } catch (IOException e) {
      throwHttpException(e, request, LogLevel.ERROR);
    }
    return null;
  }

  public CloseableHttpResponse executeWithoutSSL(HttpUriRequest request)
      throws DatabricksHttpException {
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format("Executing HTTP request [{%s}]", RequestSanitizer.sanitizeRequest(request)));
    try {
      return httpDisabledSSLClient.execute(request);
    } catch (Exception e) {
      throwHttpException(e, request, LogLevel.DEBUG);
    }
    return null;
  }

  @Override
  public void closeExpiredAndIdleConnections() {
    if (connectionManager != null) {
      synchronized (connectionManager) {
        LoggingUtil.log(
            LogLevel.DEBUG,
            String.format("connection pool stats: {%s}", connectionManager.getTotalStats()));
        connectionManager.closeExpiredConnections();
        connectionManager.closeIdleConnections(idleHttpConnectionExpiry, TimeUnit.SECONDS);
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

  public static synchronized void removeInstance(IDatabricksConnectionContext context) {
    String contextKey = Integer.toString(context.hashCode());
    DatabricksHttpClient instance = instances.remove(contextKey);
    if (instance != null) {
      try {
        instance.httpClient.close();
      } catch (IOException e) {
        LoggingUtil.log(
            LogLevel.DEBUG, String.format("Caught error while closing http client. Error %s", e));
      }
    }
  }

  private static void throwHttpException(Exception e, HttpUriRequest request, LogLevel logLevel)
      throws DatabricksHttpException {
    Throwable cause = e;
    while (cause != null) {
      if (cause instanceof DatabricksRetryHandlerException) {
        throw new DatabricksHttpException(cause.getMessage(), cause);
      }
      cause = cause.getCause();
    }
    String errorMsg =
        String.format(
            "Caught error while executing http request: [%s]. Error Message: [%s]",
            RequestSanitizer.sanitizeRequest(request), e);
    LoggingUtil.log(logLevel, errorMsg);
    throw new DatabricksHttpException(errorMsg, e);
  }
}
