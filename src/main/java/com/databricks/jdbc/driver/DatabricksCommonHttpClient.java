package com.databricks.jdbc.driver;

import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.commons.CommonsHttpClient;
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
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabricksCommonHttpClient implements HttpClient {
  private static final Logger LOG = LoggerFactory.getLogger(CommonsHttpClient.class);
  private PoolingHttpClientConnectionManager connectionManager = null;
  private final CloseableHttpClient hc;
  private int timeout;

  private SSLContext sslContext;

  private SSLConnectionSocketFactory sslFactory;

  public DatabricksCommonHttpClient(int timeoutSeconds) {
    this.timeout = timeoutSeconds * 1000;
    this.connectionManager = new PoolingHttpClientConnectionManager();
    this.connectionManager.setMaxTotal(100);
    this.hc = this.makeClosableHttpClient();
  }

  public DatabricksCommonHttpClient(int timeoutSeconds, SSLContext sslContext) {
    System.out.println("Madhav's logs - Gets to CommonHTTPClient");
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
        .setConnectionRequestTimeout(this.timeout)
        .setConnectTimeout(this.timeout)
        .setSocketTimeout(this.timeout)
        .build();
  }

  private CloseableHttpClient makeClosableHttpClient() {
    return HttpClientBuilder.create()
        .setConnectionManager(this.connectionManager)
        .setSSLSocketFactory(this.sslFactory)
        .setDefaultRequestConfig(this.makeRequestConfig())
        .build();
  }

  public Response execute(Request in) throws IOException {
    HttpUriRequest request = this.transformRequest(in);
    in.getHeaders().forEach(request::setHeader);
    CloseableHttpResponse response = this.hc.execute(request);
    return this.computeResponse(in, response);
  }

  private Response computeResponse(Request in, CloseableHttpResponse response) throws IOException {
    HttpEntity entity = response.getEntity();
    StatusLine statusLine = response.getStatusLine();
    Map<String, List<String>> hs =
        (Map)
            Arrays.stream(response.getAllHeaders())
                .collect(
                    Collectors.groupingBy(
                        NameValuePair::getName,
                        Collectors.mapping(NameValuePair::getValue, Collectors.toList())));
    if (entity == null) {
      response.close();
      return new Response(in, statusLine.getStatusCode(), statusLine.getReasonPhrase(), hs);
    } else {
      boolean streamResponse =
          in.getHeaders().containsKey("Accept")
              && !ContentType.APPLICATION_JSON.getMimeType().equals(in.getHeaders().get("Accept"))
              && !ContentType.APPLICATION_JSON
                  .getMimeType()
                  .equals(response.getFirstHeader("Content-Type").getValue());
      if (streamResponse) {
        CustomCloseInputStream inputStream =
            new CustomCloseInputStream(
                entity.getContent(),
                () -> {
                  try {
                    response.close();
                  } catch (Exception var2) {
                    throw new DatabricksException("Unable to close connection", var2);
                  }
                });
        return new Response(
            in, statusLine.getStatusCode(), statusLine.getReasonPhrase(), hs, inputStream);
      } else {
        Response var10;
        try {
          InputStream inputStream = entity.getContent();
          Throwable var8 = null;

          try {
            String body = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
            var10 =
                new Response(
                    in, statusLine.getStatusCode(), statusLine.getReasonPhrase(), hs, body);
          } catch (Throwable var26) {
            var8 = var26;
            throw var26;
          } finally {
            if (inputStream != null) {
              if (var8 != null) {
                try {
                  inputStream.close();
                } catch (Throwable var25) {
                  var8.addSuppressed(var25);
                }
              } else {
                inputStream.close();
              }
            }
          }
        } finally {
          response.close();
        }

        return var10;
      }
    }
  }

  private HttpUriRequest transformRequest(Request in) {
    switch (in.getMethod()) {
      case "GET":
        return new HttpGet(in.getUri());
      case "DELETE":
        return new HttpDelete(in.getUri());
      case "POST":
        return this.withEntity(new HttpPost(in.getUri()), in);
      case "PUT":
        return this.withEntity(new HttpPut(in.getUri()), in);
      case "PATCH":
        return this.withEntity(new HttpPatch(in.getUri()), in);
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
