package com.databricks.jdbc.dbclient.impl.http;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.*;
import static com.databricks.jdbc.dbclient.impl.common.ClientConfigurator.convertNonProxyHostConfigToBeSystemPropertyCompliant;
import static io.netty.util.NetUtil.LOCALHOST;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.LogLevel;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.ClientConfigurator;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksRetryHandlerException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.sdk.core.ProxyConfig;
import com.databricks.sdk.core.UserAgent;
import com.databricks.sdk.core.utils.ProxyUtils;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
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
import org.apache.http.ssl.SSLContextBuilder;

/** Http client implementation to be used for executing http requests. */
public class DatabricksHttpClient implements IDatabricksHttpClient {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DatabricksHttpClient.class);
  private static final int DEFAULT_MAX_HTTP_CONNECTIONS = 1000;
  private static final int DEFAULT_MAX_HTTP_CONNECTIONS_PER_ROUTE = 1000;
  private static final int DEFAULT_HTTP_CONNECTION_TIMEOUT = 60 * 1000; // ms
  private static final int DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT = 300 * 1000; // ms
  private static final String SDK_USER_AGENT = "databricks-sdk-java";
  private static final String JDBC_HTTP_USER_AGENT = "databricks-jdbc-http";
  private static final ConcurrentHashMap<String, DatabricksHttpClient> instances =
      new ConcurrentHashMap<>();
  private static PoolingHttpClientConnectionManager connectionManager;
  private final CloseableHttpClient httpClient;
  protected static int idleHttpConnectionExpiry;
  private CloseableHttpClient httpDisabledSSLClient;
  private DatabricksHttpRetryHandler retryHandler;

  private DatabricksHttpClient(IDatabricksConnectionContext connectionContext) {
    initializeConnectionManager(connectionContext);
    httpClient = makeClosableHttpClient(connectionContext);
    httpDisabledSSLClient = makeClosableDisabledSslHttpClient();
    idleHttpConnectionExpiry = connectionContext.getIdleHttpConnectionExpiry();
    retryHandler = new DatabricksHttpRetryHandler(connectionContext);
  }

  @VisibleForTesting
  DatabricksHttpClient(
      CloseableHttpClient closeableHttpClient,
      PoolingHttpClientConnectionManager connectionManager) {
    DatabricksHttpClient.connectionManager = connectionManager;
    initializeConnectionManager(null);
    this.httpClient = closeableHttpClient;
  }

  private static void initializeConnectionManager(IDatabricksConnectionContext connectionContext) {
    if (connectionManager == null) {
      if (connectionContext != null) {
        connectionManager =
            new PoolingHttpClientConnectionManager(
                ClientConfigurator.getConnectionSocketFactoryRegistry(connectionContext));
      } else {
        connectionManager = new PoolingHttpClientConnectionManager();
      }
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
      LOGGER.debug(
          String.format(
              "Error in creating HttpClient with the SSL context [{%s}]", e.getMessage()));
    }
    return null;
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

  public static synchronized DatabricksHttpClient getInstance(
      IDatabricksConnectionContext context) {
    String contextKey = Integer.toString(context.hashCode());
    return instances.computeIfAbsent(contextKey, k -> new DatabricksHttpClient(context));
  }

  @Override
  public CloseableHttpResponse execute(HttpUriRequest request) throws DatabricksHttpException {
    LOGGER.debug(
        String.format("Executing HTTP request [{%s}]", RequestSanitizer.sanitizeRequest(request)));
    try {
      return httpClient.execute(request);
    } catch (IOException e) {
      throwHttpException(e, request, LogLevel.ERROR);
    }
    return null;
  }

  public CloseableHttpResponse executeWithoutCertVerification(HttpUriRequest request)
      throws DatabricksHttpException {
    LOGGER.debug(
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
        LOGGER.debug(
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
        LOGGER.debug(String.format("Caught error while closing http client. Error %s", e));
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
    if (logLevel == LogLevel.DEBUG) {
      LOGGER.debug(errorMsg);
    } else {
      LOGGER.error(e, errorMsg);
    }
    throw new DatabricksHttpException(errorMsg, e);
  }
}
