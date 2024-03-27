package com.databricks.jdbc.client.http;

import static com.databricks.jdbc.client.http.DatabricksHttpClient.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.client.DatabricksHttpException;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksHttpClientTest {
  @Mock CloseableHttpClient mockHttpClient;

  @Mock HttpUriRequest request;

  @Mock PoolingHttpClientConnectionManager connectionManager;

  @Mock CloseableHttpResponse closeableHttpResponse;

  @Test
  public void testSetProxyDetailsIntoHttpClient() {
    HttpClientBuilder builder = HttpClientBuilder.create();
    assertDoesNotThrow(
        () ->
            DatabricksHttpClient.setProxyDetailsInHttpClient(
                builder, "proxyHost", 8080, true, "proxyUser", "proxyPassword"));
    assertDoesNotThrow(
        () ->
            DatabricksHttpClient.setProxyDetailsInHttpClient(
                builder, "proxyHost", 8080, false, "proxyUser", "proxyPassword"));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DatabricksHttpClient.setProxyDetailsInHttpClient(
                builder, "proxyHost", 8080, true, null, "proxyPassword"));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DatabricksHttpClient.setProxyDetailsInHttpClient(
                builder, null, 8080, true, "user", "proxyPassword"));
  }

  @Test
  void testExecuteThrowsError() throws IOException {
    DatabricksHttpClient databricksHttpClient =
        new DatabricksHttpClient(mockHttpClient, connectionManager);
    when(request.getURI()).thenReturn(URI.create("https://databricks.com"));
    when(mockHttpClient.execute(request)).thenThrow(new IOException());
    assertThrows(DatabricksHttpException.class, () -> databricksHttpClient.execute(request));
  }

  @Test
  void testExecute() throws IOException, DatabricksHttpException {
    DatabricksHttpClient databricksHttpClient =
        new DatabricksHttpClient(mockHttpClient, connectionManager);
    when(request.getURI()).thenReturn(URI.create("TestURI"));
    when(mockHttpClient.execute(request)).thenReturn(closeableHttpResponse);
    assertEquals(closeableHttpResponse, databricksHttpClient.execute(request));
  }

  @Test
  void TestCloseExpiredAndIdleConnections() {
    DatabricksHttpClient databricksHttpClient =
        new DatabricksHttpClient(mockHttpClient, connectionManager);
    databricksHttpClient.closeExpiredAndIdleConnections();
    verify(connectionManager).closeExpiredConnections();
    verify(connectionManager)
        .closeIdleConnections(
            DatabricksHttpClient.DEFAULT_IDLE_CONNECTION_TIMEOUT, TimeUnit.SECONDS);
  }

  @Test
  void TestCloseExpiredAndIdleConnectionsForNull() {
    DatabricksHttpClient databricksHttpClient = new DatabricksHttpClient(mockHttpClient, null);
    assertDoesNotThrow(databricksHttpClient::closeExpiredAndIdleConnections);
  }

  @Test
  void testIsRetryAllowed() {
    assertTrue(isRetryAllowed("GET"), "GET requests should be allowed for retry");
    assertFalse(isRetryAllowed("POST"), "POST requests should not be allowed for retry");
  }

  @Test
  void testIsErrorCodeRetryable() {
    assertTrue(isErrorCodeRetryable(408), "HTTP 408 Request Timeout should be retryable");
    assertTrue(isErrorCodeRetryable(503), "HTTP 503 Service Unavailable should be retryable");
    assertFalse(isErrorCodeRetryable(401), "HTTP 401 Unauthorized should not be retryable");
  }
}
