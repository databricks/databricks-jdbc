package com.databricks.jdbc.dbclient.impl.http;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.*;
import static com.databricks.jdbc.common.util.WildcardUtil.isNullOrEmpty;
import static com.databricks.jdbc.dbclient.impl.common.ClientConfigurator.convertNonProxyHostConfigToBeSystemPropertyCompliant;
import static io.netty.util.NetUtil.LOCALHOST;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.HttpClientType;
import com.databricks.jdbc.common.RequestType;
import com.databricks.jdbc.common.util.DriverUtil;
import com.databricks.jdbc.common.util.UserAgentManager;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.ConfiguratorUtils;
import com.databricks.jdbc.exception.DatabricksDriverException;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksSSLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.core.ProxyConfig;
import com.databricks.sdk.core.utils.ProxyUtils;
import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.nio.AsyncRequestProducer;
import org.apache.hc.core5.http.nio.AsyncResponseConsumer;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.UnsupportedSchemeException;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.IdleConnectionEvictor;
import org.apache.http.impl.conn.DefaultSchemePortResolver;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

/** Http client implementation to be used for executing http requests. */
public class DatabricksHttpClient implements IDatabricksHttpClient, Closeable {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DatabricksHttpClient.class);
  private static final int DEFAULT_MAX_HTTP_CONNECTIONS = 1000;
  private final PoolingHttpClientConnectionManager connectionManager;
  private final CloseableHttpClient httpClient;
  private IdleConnectionEvictor idleConnectionEvictor;
  private CloseableHttpAsyncClient asyncClient;
  private IDatabricksConnectionContext connectionContext;

  DatabricksHttpClient(IDatabricksConnectionContext connectionContext, HttpClientType type) {
    connectionManager = initializeConnectionManager(connectionContext);
    httpClient = makeClosableHttpClient(connectionContext, type);
    idleConnectionEvictor =
        new IdleConnectionEvictor(
            connectionManager, connectionContext.getIdleHttpConnectionExpiry(), TimeUnit.SECONDS);
    idleConnectionEvictor.start();
    asyncClient = GlobalAsyncHttpClient.getClient();
    this.connectionContext = connectionContext;
  }

  @VisibleForTesting
  DatabricksHttpClient(
      CloseableHttpClient testCloseableHttpClient,
      PoolingHttpClientConnectionManager testConnectionManager) {
    httpClient = testCloseableHttpClient;
    connectionManager = testConnectionManager;
  }

  @Override
  public CloseableHttpResponse execute(HttpUriRequest request) throws DatabricksHttpException {
    return execute(request, false);
  }

  @Override
  public CloseableHttpResponse execute(HttpUriRequest request, boolean supportGzipEncoding)
      throws DatabricksHttpException {
    LOGGER.debug("Executing HTTP request {}", RequestSanitizer.sanitizeRequest(request));
    prepareRequestHeaders(request, supportGzipEncoding);
    try {
      return httpClient.execute(request);
    } catch (IOException e) {
      RetryUtils.throwDatabricksHttpException(e, request);
    }
    return null;
  }

  @Override
  public CloseableHttpResponse executeWithRetry(HttpUriRequest request, RequestType requestType)
      throws DatabricksHttpException {
    return executeWithRetry(request, requestType, false);
  }

  @Override
  public CloseableHttpResponse executeWithRetry(
      HttpUriRequest request, RequestType requestType, boolean supportGzipEncoding)
      throws DatabricksHttpException {
    prepareRequestHeaders(request, supportGzipEncoding);

    IRetryStrategy strategy = RetryUtils.getRetryStrategy(requestType);
    LOGGER.debug(
        "Executing HTTP request : {}, request type : {},  retryStrategy : {}",
        RequestSanitizer.sanitizeRequest(request),
        requestType,
        strategy.getClass().getSimpleName());

    RetryTimeoutManager retryTimeoutManager = new RetryTimeoutManager(connectionContext);

    int retryAttempt = 0;

    while (true) {
      Optional<Integer> shouldRetryAfter;
      try {
        CloseableHttpResponse response = httpClient.execute(request);
        int statusCode = response.getStatusLine().getStatusCode();
        Optional<Integer> retryAfterHeader = RetryUtils.extractRetryAfterHeader(response);
        shouldRetryAfter =
            strategy.shouldRetryAfter(
                statusCode, retryAfterHeader, retryAttempt, connectionContext, retryTimeoutManager);
        if (shouldRetryAfter.isEmpty()) {
          return response;
        }
        response.close();
      } catch (Exception e) {
        shouldRetryAfter = strategy.shouldRetryAfter(e, retryAttempt, retryTimeoutManager);
        if (shouldRetryAfter.isEmpty()) {
          RetryUtils.throwDatabricksHttpException(e, request);
        }
      }

      try {
        Thread.sleep(shouldRetryAfter.get());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Thread interrupted during retry", e);
      }
      retryAttempt++;
    }
  }

  private void prepareRequestHeaders(HttpUriRequest request, boolean supportGzipEncoding) {
    if (!DriverUtil.isRunningAgainstFake() && supportGzipEncoding) {
      // TODO : allow gzip in wiremock
      request.setHeader("Content-Encoding", "gzip");
    }

    String userAgentString = UserAgentManager.getUserAgentString();
    if (!isNullOrEmpty(userAgentString) && !request.containsHeader("User-Agent")) {
      request.setHeader("User-Agent", userAgentString);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>This method leverages the Apache Async HTTP client which uses non-blocking I/O, allowing for
   * higher throughput and better resource utilization compared to blocking I/O. Instead of
   * dedicating one thread per connection, it can handle multiple connections with a smaller thread
   * pool, significantly reducing memory overhead and thread context switching.
   */
  @Override
  public <T> Future<T> executeAsync(
      AsyncRequestProducer requestProducer,
      AsyncResponseConsumer<T> responseConsumer,
      FutureCallback<T> callback) {
    return asyncClient.execute(requestProducer, responseConsumer, callback);
  }

  @Override
  public void close() throws IOException {
    if (idleConnectionEvictor != null) {
      idleConnectionEvictor.shutdown();
    }
    if (httpClient != null) {
      httpClient.close();
    }
    if (connectionManager != null) {
      connectionManager.shutdown();
    }
    if (asyncClient != null) {
      GlobalAsyncHttpClient.releaseClient();
      asyncClient = null;
    }
  }

  private PoolingHttpClientConnectionManager initializeConnectionManager(
      IDatabricksConnectionContext connectionContext) {
    try {
      PoolingHttpClientConnectionManager connectionManager =
          ConfiguratorUtils.getBaseConnectionManager(connectionContext);
      connectionManager.setMaxTotal(DEFAULT_MAX_HTTP_CONNECTIONS);
      connectionManager.setDefaultMaxPerRoute(connectionContext.getHttpMaxConnectionsPerRoute());
      return connectionManager;
    } catch (DatabricksSSLException e) {
      LOGGER.error("Failed to initialize HTTP connection manager", e);
      // Currently only SSL Handshake failure causes this exception.
      throw new DatabricksDriverException(
          "Failed to initialize HTTP connection manager",
          DatabricksDriverErrorCode.SSL_HANDSHAKE_ERROR);
    }
  }

  private RequestConfig makeRequestConfig(IDatabricksConnectionContext connectionContext) {
    int timeoutMillis = connectionContext.getSocketTimeout() * 1000;
    int requestTimeout =
        connectionContext.getHttpConnectionRequestTimeout() != null
            ? connectionContext.getHttpConnectionRequestTimeout() * 1000
            : timeoutMillis;
    return RequestConfig.custom()
        .setConnectionRequestTimeout(requestTimeout)
        .setConnectTimeout(timeoutMillis)
        .setSocketTimeout(timeoutMillis)
        .build();
  }

  private CloseableHttpClient makeClosableHttpClient(
      IDatabricksConnectionContext connectionContext, HttpClientType type) {
    DatabricksHttpRetryHandler retryHandler =
        type.equals(HttpClientType.COMMON)
            ? new DatabricksHttpRetryHandler(connectionContext)
            : new UCVolumeHttpRetryHandler(connectionContext);
    HttpClientBuilder builder =
        HttpClientBuilder.create()
            .setConnectionManager(connectionManager)
            .setUserAgent(UserAgentManager.getUserAgentString())
            .setDefaultRequestConfig(makeRequestConfig(connectionContext))
            .setRetryHandler(retryHandler)
            .addInterceptorFirst(retryHandler);
    setupProxy(connectionContext, builder);
    if (DriverUtil.isRunningAgainstFake()) {
      setFakeServiceRouteInHttpClient(builder);
    }
    return builder.build();
  }

  @VisibleForTesting
  void setupProxy(IDatabricksConnectionContext connectionContext, HttpClientBuilder builder) {
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
      String nonProxyHosts =
          convertNonProxyHostConfigToBeSystemPropertyCompliant(
              connectionContext.getNonProxyHosts());
      ProxyConfig proxyConfig =
          new ProxyConfig()
              .setUseSystemProperties(connectionContext.getUseSystemProxy())
              .setHost(proxyHost)
              .setPort(proxyPort)
              .setUsername(proxyUser)
              .setPassword(proxyPassword)
              .setProxyAuthType(proxyAuth)
              .setNonProxyHosts(nonProxyHosts);
      ProxyUtils.setupProxy(proxyConfig, builder);
    }
  }

  @VisibleForTesting
  void setFakeServiceRouteInHttpClient(HttpClientBuilder builder) {
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
            throw new DatabricksDriverException(
                e.getMessage(), DatabricksDriverErrorCode.INTEGRATION_TEST_ERROR);
          }

          if (host.getHostName().equalsIgnoreCase(LOCALHOST.getHostName())
              || host.getHostName().equalsIgnoreCase("127.0.0.1")) {
            // If the target host is localhost, then no need to set proxy
            return new HttpRoute(target, null, false);
          }

          // Get the fake service URI for the target URI and set it as proxy
          final HttpHost proxy =
              HttpHost.create(System.getProperty(host.toURI() + FAKE_SERVICE_URI_PROP_SUFFIX));

          return new HttpRoute(target, null, proxy, false);
        });
  }
}
