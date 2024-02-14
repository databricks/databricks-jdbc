package com.databricks.jdbc.driver;

import static org.apache.http.entity.ContentType.APPLICATION_JSON;

import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.http.Response;
import com.databricks.sdk.core.utils.CustomCloseInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.*;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabricksCommonHttpClient implements HttpClient {
  private static final Logger LOG =
      LoggerFactory.getLogger(com.databricks.jdbc.driver.DatabricksCommonHttpClient.class);
  private PoolingHttpClientConnectionManager connectionManager = null;
  private final CloseableHttpClient hc;
  private int timeout;

  private SSLContext sslContext;

  private SSLConnectionSocketFactory sslFactory;

  public DatabricksCommonHttpClient(int timeoutSeconds) {
    timeout = timeoutSeconds * 1000;
    this.connectionManager = new PoolingHttpClientConnectionManager();
    connectionManager.setMaxTotal(100);
    hc = makeClosableHttpClient();
  }

  public DatabricksCommonHttpClient(int timeoutSeconds, SSLContext sslContext) {
    this.timeout = timeoutSeconds * 1000;
    this.sslContext = sslContext;
    this.sslFactory = new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE);
    Registry<ConnectionSocketFactory> socketFactoryRegistry =
        RegistryBuilder.<ConnectionSocketFactory>create()
            .register("https", this.sslFactory)
            .register("http", new PlainConnectionSocketFactory())
            .build();
    this.connectionManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
    this.connectionManager.setMaxTotal(100);
    this.hc = this.makeClosableHttpClient();
  }

  private RequestConfig makeRequestConfig() {
    return RequestConfig.custom()
        .setConnectionRequestTimeout(timeout)
        .setConnectTimeout(timeout)
        .setSocketTimeout(timeout)
        .build();
  }

  private CloseableHttpClient makeClosableHttpClient() {
    return HttpClientBuilder.create()
        .setConnectionManager(connectionManager)
        .setDefaultRequestConfig(makeRequestConfig())
        .build();
  }

  @Override
  public Response execute(Request in) throws IOException {
    HttpUriRequest request = transformRequest(in);
    boolean handleRedirects = in.getRedirectionBehavior().orElse(true);
    if (!handleRedirects) {
      request.getParams().setParameter("http.protocol.handle-redirects", false);
    }
    in.getHeaders().forEach(request::setHeader);
    CloseableHttpResponse response = hc.execute(request);
    return computeResponse(in, response);
  }

  private Response computeResponse(Request in, CloseableHttpResponse response) throws IOException {
    HttpEntity entity = response.getEntity();
    StatusLine statusLine = response.getStatusLine();
    Map<String, List<String>> hs =
        Arrays.stream(response.getAllHeaders())
            .collect(
                Collectors.groupingBy(
                    NameValuePair::getName,
                    Collectors.mapping(NameValuePair::getValue, Collectors.toList())));
    if (entity == null) {
      response.close();
      return new Response(in, statusLine.getStatusCode(), statusLine.getReasonPhrase(), hs);
    }

    boolean streamResponse =
        in.getHeaders().containsKey("Accept")
            && !APPLICATION_JSON.getMimeType().equals(in.getHeaders().get("Accept"))
            && !APPLICATION_JSON
                .getMimeType()
                .equals(response.getFirstHeader("Content-Type").getValue());
    if (streamResponse) {
      CustomCloseInputStream inputStream =
          new CustomCloseInputStream(
              entity.getContent(),
              () -> {
                try {
                  response.close();
                } catch (Exception e) {
                  throw new DatabricksException("Unable to close connection", e);
                }
              });
      return new Response(
          in, statusLine.getStatusCode(), statusLine.getReasonPhrase(), hs, inputStream);
    }

    try (InputStream inputStream = entity.getContent()) {
      String body = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
      return new Response(in, statusLine.getStatusCode(), statusLine.getReasonPhrase(), hs, body);
    } finally {
      response.close();
    }
  }

  private HttpUriRequest transformRequest(Request in) {
    switch (in.getMethod()) {
      case Request.GET:
        return new HttpGet(in.getUri());
      case Request.DELETE:
        return new HttpDelete(in.getUri());
      case Request.POST:
        return withEntity(new HttpPost(in.getUri()), in);
      case Request.PUT:
        return withEntity(new HttpPut(in.getUri()), in);
      case Request.PATCH:
        return withEntity(new HttpPatch(in.getUri()), in);
      default:
        throw new IllegalArgumentException("Unknown method: " + in.getMethod());
    }
  }

  private HttpRequestBase withEntity(HttpEntityEnclosingRequestBase request, Request in) {
    if (in.isBodyString()) {
      request.setEntity(new StringEntity(in.getBodyString(), StandardCharsets.UTF_8));
    } else if (in.isBodyStreaming()) {
      request.setEntity(new InputStreamEntity(in.getBodyStream()));
    } else {
      LOG.warn(
          "withEntity called with a request with no body, so no request entity will be set. URI: {}",
          in.getUri());
    }
    return request;
  }
}
