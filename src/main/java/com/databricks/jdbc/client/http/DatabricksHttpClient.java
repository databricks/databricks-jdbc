package com.databricks.jdbc.client.http;

import com.databricks.jdbc.client.DatabricksHttpException;
import com.databricks.jdbc.client.IDatabricksHttpClient;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Http client implementation to be used for executing http requests.
 */
public class DatabricksHttpClient implements IDatabricksHttpClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksHttpClient.class);

  private static final int DEFAULT_MAX_HTTP_CONNECTIONS = 300;
  private static final int DEFAULT_MAX_HTTP_CONNECTIONS_PER_ROUTE = 300;
  private static final int DEFAULT_HTTP_CONNECTION_TIMEOUT = 60 * 1000; // ms
  private static final int DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT = 300 * 1000; // ms

  private static DatabricksHttpClient instance = new DatabricksHttpClient();

  private final PoolingHttpClientConnectionManager connectionManager;

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

  private CloseableHttpClient makeClosableHttpClient() {
    return HttpClientBuilder.create()
        .setConnectionManager(connectionManager)
        // TODO: set appropriate user agent
        .setUserAgent("jdbc/databricks")
        .setDefaultRequestConfig(makeRequestConfig())
        .build();
  }

  public static synchronized DatabricksHttpClient getInstance() {
    return instance;
  }


  @Override
  public HttpResponse execute(HttpUriRequest request) throws DatabricksHttpException {
    LOGGER.atDebug().log("Executing HTTP request [%s]", RequestSanitizer.sanitizeRequest(request));
    // TODO: add retries and error handling
    try {
      return httpClient.execute(request);
    } catch (IOException e) {
      String errorMsg = String.format("Caught error while executing http request: [%s]",
          RequestSanitizer.sanitizeRequest(request));
      LOGGER.atError().setCause(e).log(errorMsg);
      throw new DatabricksHttpException(errorMsg, e);
    }
  }
}
