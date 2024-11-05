package com.databricks.jdbc.dbclient.impl.http;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.*;
import static com.databricks.jdbc.dbclient.impl.common.ClientConfigurator.convertNonProxyHostConfigToBeSystemPropertyCompliant;
import static io.netty.util.NetUtil.LOCALHOST;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.ConfiguratorUtils;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksRetryHandlerException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.sdk.core.ProxyConfig;
import com.databricks.sdk.core.UserAgent;
import com.databricks.sdk.core.utils.ProxyUtils;
import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpException;
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
  private static final int DEFAULT_MAX_HTTP_CONNECTIONS_PER_ROUTE = 1000;
  private static final int DEFAULT_HTTP_CONNECTION_TIMEOUT = 60 * 1000; // ms
  private static final int DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT = 300 * 1000; // ms
  private static final String SDK_USER_AGENT = "databricks-sdk-java";
  private static final String JDBC_HTTP_USER_AGENT = "databricks-jdbc-http";
  private final PoolingHttpClientConnectionManager connectionManager;
  private final CloseableHttpClient httpClient;
  private DatabricksHttpRetryHandler retryHandler;
  private IdleConnectionEvictor idleConnectionEvictor;

  DatabricksHttpClient(IDatabricksConnectionContext connectionContext) {
    connectionManager = initializeConnectionManager(connectionContext);
    httpClient = makeClosableHttpClient(connectionContext);
    retryHandler = new DatabricksHttpRetryHandler(connectionContext);
    idleConnectionEvictor =
        new IdleConnectionEvictor(
            connectionManager, connectionContext.getIdleHttpConnectionExpiry(), TimeUnit.SECONDS);
    idleConnectionEvictor.start();
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
    LOGGER.debug(
        String.format("Executing HTTP request [{%s}]", RequestSanitizer.sanitizeRequest(request)));
    if (!Boolean.parseBoolean(System.getProperty(IS_FAKE_SERVICE_TEST_PROP))) {
      // TODO : allow gzip in wiremock
      request.setHeader("Content-Encoding", "gzip");
    }
    try {
      return httpClient.execute(request);
    } catch (IOException e) {
      throwHttpException(e, request);
    }
    return null;
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
  }

  private PoolingHttpClientConnectionManager initializeConnectionManager(
      IDatabricksConnectionContext connectionContext) {
    PoolingHttpClientConnectionManager connectionManager =
        ConfiguratorUtils.getBaseConnectionManager(connectionContext);
    connectionManager.setMaxTotal(DEFAULT_MAX_HTTP_CONNECTIONS);
    connectionManager.setDefaultMaxPerRoute(DEFAULT_MAX_HTTP_CONNECTIONS_PER_ROUTE);
    return connectionManager;
  }

  private RequestConfig makeRequestConfig() {
    return RequestConfig.custom()
        .setConnectionRequestTimeout(DEFAULT_HTTP_CONNECTION_TIMEOUT)
        .setConnectTimeout(DEFAULT_HTTP_CONNECTION_TIMEOUT)
        .setSocketTimeout(DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT)
        .build();
  }

  private CloseableHttpClient makeClosableHttpClient(
      IDatabricksConnectionContext connectionContext) {
    HttpClientBuilder builder =
        HttpClientBuilder.create()
            .setConnectionManager(connectionManager)
            .setUserAgent(getUserAgent())
            .setDefaultRequestConfig(makeRequestConfig())
            .setRetryHandler(retryHandler)
            .addInterceptorFirst(retryHandler);
    setupProxy(connectionContext, builder);
    if (Boolean.parseBoolean(System.getProperty(IS_FAKE_SERVICE_TEST_PROP))) {
      setFakeServiceRouteInHttpClient(builder);
    }
    return builder.build();
  }

  private static void throwHttpException(Exception e, HttpUriRequest request)
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
    LOGGER.error(e, errorMsg);
    throw new DatabricksHttpException(errorMsg, e);
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

  String getUserAgent() {
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
}
