package com.databricks.jdbc.dbclient.impl.common;

import com.databricks.sdk.core.ProxyConfig;
import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.http.Response;
import com.databricks.sdk.core.utils.ProxyUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

/** Preserves a declared DELETE body, which the SDK CommonsHttpClient silently drops. */
public final class DeleteBodyHttpClient implements HttpClient {
  private final HttpClient delegate;
  private final CloseableHttpClient deleteClient;
  private final PoolingHttpClientConnectionManager connectionManager;

  public DeleteBodyHttpClient(
      HttpClient delegate,
      PoolingHttpClientConnectionManager connectionManager,
      ProxyConfig proxyConfig,
      int timeoutSeconds) {
    this.delegate = delegate;
    this.connectionManager = connectionManager;
    int timeout = timeoutSeconds * 1000;
    HttpClientBuilder builder =
        HttpClientBuilder.create()
            .setConnectionManager(connectionManager)
            .setConnectionManagerShared(true)
            .setDefaultRequestConfig(
                RequestConfig.custom()
                    .setConnectTimeout(timeout)
                    .setSocketTimeout(timeout)
                    .setConnectionRequestTimeout(timeout)
                    .build());
    ProxyUtils.setupProxy(proxyConfig, builder);
    this.deleteClient = builder.build();
  }

  @Override
  public Response execute(Request request) throws IOException {
    if (!Request.DELETE.equals(request.getMethod())
        || !request.isBodyString()
        || !request.getBodyString().contains("\"backend_compute\"")) {
      return delegate.execute(request);
    }
    HttpEntityEnclosingRequestBase delete =
        new HttpEntityEnclosingRequestBase() {
          @Override
          public String getMethod() {
            return Request.DELETE;
          }
        };
    delete.setURI(request.getUri());
    delete.setEntity(new StringEntity(request.getBodyString(), StandardCharsets.UTF_8));
    request.getHeaders().forEach(delete::setHeader);
    // The SDK's discovery request can leave a stale idle socket in this shared pool.
    connectionManager.closeIdleConnections(0, TimeUnit.MILLISECONDS);
    try (CloseableHttpResponse response = deleteClient.execute(delete)) {
      Map<String, List<String>> headers =
          Arrays.stream(response.getAllHeaders())
              .collect(
                  Collectors.groupingBy(
                      NameValuePair::getName,
                      Collectors.mapping(NameValuePair::getValue, Collectors.toList())));
      int status = response.getStatusLine().getStatusCode();
      String reason = response.getStatusLine().getReasonPhrase();
      String body =
          response.getEntity() == null
              ? null
              : EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
      return new Response(request, request.getUri().toURL(), status, reason, headers, body);
    }
  }
}
