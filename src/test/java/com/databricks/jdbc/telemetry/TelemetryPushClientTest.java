package com.databricks.jdbc.telemetry;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.HttpClientType;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.exception.DatabricksTelemetryException;
import com.databricks.jdbc.model.telemetry.TelemetryRequest;
import java.util.Arrays;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

public class TelemetryPushClientTest {

  @Test
  public void pushEvent_throwsTelemetryException_on429_whenCBEnabled() throws Exception {
    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        org.mockito.Mockito.mockStatic(DatabricksHttpClientFactory.class)) {
      DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
      factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);

      IDatabricksHttpClient mockHttpClient = mock(IDatabricksHttpClient.class);
      when(mockFactory.getClient(any(), any())).thenReturn(mockHttpClient);

      CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
      StatusLine mockStatusLine = mock(StatusLine.class);
      when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
      when(mockStatusLine.getStatusCode()).thenReturn(429);
      when(mockHttpClient.execute(any())).thenReturn(mockResponse);

      IDatabricksConnectionContext mockContext = mock(IDatabricksConnectionContext.class);
      when(mockContext.getHostUrl()).thenReturn("https://example.com");
      when(mockContext.isTelemetryCircuitBreakerEnabled()).thenReturn(true);

      TelemetryPushClient client =
          new TelemetryPushClient(false /* isAuthenticated */, mockContext, null);

      assertThrows(
          DatabricksTelemetryException.class, () -> client.pushEvent(new TelemetryRequest()));
    }
  }

  @Test
  public void pushEvent_doesNotThrow_on429_whenCBDisabled() throws Exception {
    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        org.mockito.Mockito.mockStatic(DatabricksHttpClientFactory.class)) {
      DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
      factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);

      IDatabricksHttpClient mockHttpClient = mock(IDatabricksHttpClient.class);
      when(mockFactory.getClient(any(), any())).thenReturn(mockHttpClient);

      CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
      StatusLine mockStatusLine = mock(StatusLine.class);
      when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
      when(mockStatusLine.getStatusCode()).thenReturn(429);
      when(mockHttpClient.execute(any())).thenReturn(mockResponse);

      IDatabricksConnectionContext mockContext = mock(IDatabricksConnectionContext.class);
      when(mockContext.getHostUrl()).thenReturn("https://example.com");
      when(mockContext.isTelemetryCircuitBreakerEnabled()).thenReturn(false);

      TelemetryPushClient client =
          new TelemetryPushClient(false /* isAuthenticated */, mockContext, null);

      assertDoesNotThrow(() -> client.pushEvent(new TelemetryRequest()));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {400, 500, 502, 503, 504})
  public void pushEvent_throwsTelemetryException_onErrorCodes_whenCBEnabled(int statusCode)
      throws Exception {
    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        org.mockito.Mockito.mockStatic(DatabricksHttpClientFactory.class)) {
      DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
      factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);

      IDatabricksHttpClient mockHttpClient = mock(IDatabricksHttpClient.class);
      when(mockFactory.getClient(any(), any())).thenReturn(mockHttpClient);

      CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
      StatusLine mockStatusLine = mock(StatusLine.class);
      when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
      when(mockStatusLine.getStatusCode()).thenReturn(statusCode);
      when(mockHttpClient.execute(any())).thenReturn(mockResponse);

      IDatabricksConnectionContext mockContext = mock(IDatabricksConnectionContext.class);
      when(mockContext.getHostUrl()).thenReturn("https://example.com");
      when(mockContext.isTelemetryCircuitBreakerEnabled()).thenReturn(true);

      TelemetryPushClient client =
          new TelemetryPushClient(false /* isAuthenticated */, mockContext, null);

      assertThrows(
          DatabricksTelemetryException.class, () -> client.pushEvent(new TelemetryRequest()));
    }
  }

  @Test
  public void pushEvent_usesTelemetryHttpClientType() throws Exception {
    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        org.mockito.Mockito.mockStatic(DatabricksHttpClientFactory.class)) {
      DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
      factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);

      IDatabricksHttpClient mockHttpClient = mock(IDatabricksHttpClient.class);
      when(mockFactory.getClient(any(), any())).thenReturn(mockHttpClient);

      CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
      StatusLine mockStatusLine = mock(StatusLine.class);
      when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
      when(mockStatusLine.getStatusCode()).thenReturn(200);
      when(mockHttpClient.execute(any())).thenReturn(mockResponse);

      IDatabricksConnectionContext mockContext = mock(IDatabricksConnectionContext.class);
      when(mockContext.getHostUrl()).thenReturn("https://example.com");
      when(mockContext.isTelemetryCircuitBreakerEnabled()).thenReturn(false);

      TelemetryPushClient client =
          new TelemetryPushClient(false /* isAuthenticated */, mockContext, null);
      client.pushEvent(new TelemetryRequest());

      // Verify the TELEMETRY client type is used, not the default COMMON type
      verify(mockFactory).getClient(eq(mockContext), eq(HttpClientType.TELEMETRY));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {400, 500, 502, 503, 504})
  public void pushEvent_doesNotThrow_onErrorCodes_whenCBDisabled(int statusCode) throws Exception {
    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        org.mockito.Mockito.mockStatic(DatabricksHttpClientFactory.class)) {
      DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
      factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);

      IDatabricksHttpClient mockHttpClient = mock(IDatabricksHttpClient.class);
      when(mockFactory.getClient(any(), any())).thenReturn(mockHttpClient);

      CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
      StatusLine mockStatusLine = mock(StatusLine.class);
      when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
      when(mockStatusLine.getStatusCode()).thenReturn(statusCode);
      when(mockHttpClient.execute(any())).thenReturn(mockResponse);

      IDatabricksConnectionContext mockContext = mock(IDatabricksConnectionContext.class);
      when(mockContext.getHostUrl()).thenReturn("https://example.com");
      when(mockContext.isTelemetryCircuitBreakerEnabled()).thenReturn(false);

      TelemetryPushClient client =
          new TelemetryPushClient(false /* isAuthenticated */, mockContext, null);

      assertDoesNotThrow(() -> client.pushEvent(new TelemetryRequest()));
    }
  }

  @Test
  public void pushEvent_noRetryOnFullSuccess() throws Exception {
    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        org.mockito.Mockito.mockStatic(DatabricksHttpClientFactory.class)) {
      DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
      factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);

      IDatabricksHttpClient mockHttpClient = mock(IDatabricksHttpClient.class);
      when(mockFactory.getClient(any(), any())).thenReturn(mockHttpClient);

      TelemetryRequest request = new TelemetryRequest();
      request.setProtoLogs(Arrays.asList("log1", "log2"));

      CloseableHttpResponse successResponse = mock(CloseableHttpResponse.class);
      StatusLine successStatusLine = mock(StatusLine.class);
      when(successResponse.getStatusLine()).thenReturn(successStatusLine);
      when(successStatusLine.getStatusCode()).thenReturn(200);

      HttpEntity successEntity = mock(HttpEntity.class);
      when(successResponse.getEntity()).thenReturn(successEntity);
      when(successEntity.getContent())
          .thenReturn(
              new java.io.ByteArrayInputStream("{\"numProtoSuccess\":2,\"errors\":[]}".getBytes()));

      when(mockHttpClient.execute(any(HttpUriRequest.class))).thenReturn(successResponse);

      IDatabricksConnectionContext mockContext = mock(IDatabricksConnectionContext.class);
      when(mockContext.getHostUrl()).thenReturn("https://example.com");
      when(mockContext.isTelemetryCircuitBreakerEnabled()).thenReturn(false);

      TelemetryPushClient client =
          new TelemetryPushClient(false /* isAuthenticated */, mockContext, null);

      assertDoesNotThrow(() -> client.pushEvent(request));

      verify(mockHttpClient, times(1)).execute(any(HttpUriRequest.class));
    }
  }

  @Test
  public void pushEvent_retriesOnPartialSuccess() throws Exception {
    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        org.mockito.Mockito.mockStatic(DatabricksHttpClientFactory.class)) {
      DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
      factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);

      IDatabricksHttpClient mockHttpClient = mock(IDatabricksHttpClient.class);
      when(mockFactory.getClient(any(), any())).thenReturn(mockHttpClient);

      TelemetryRequest request = new TelemetryRequest();
      request.setProtoLogs(Arrays.asList("log1", "log2", "log3"));

      CloseableHttpResponse response1 = mock(CloseableHttpResponse.class);
      StatusLine statusLine1 = mock(StatusLine.class);
      when(response1.getStatusLine()).thenReturn(statusLine1);
      when(statusLine1.getStatusCode()).thenReturn(200);

      HttpEntity entity1 = mock(HttpEntity.class);
      when(response1.getEntity()).thenReturn(entity1);
      when(entity1.getContent())
          .thenReturn(
              new java.io.ByteArrayInputStream("{\"numProtoSuccess\":1,\"errors\":[]}".getBytes()));

      CloseableHttpResponse response2 = mock(CloseableHttpResponse.class);
      StatusLine statusLine2 = mock(StatusLine.class);
      when(response2.getStatusLine()).thenReturn(statusLine2);
      when(statusLine2.getStatusCode()).thenReturn(200);

      HttpEntity entity2 = mock(HttpEntity.class);
      when(response2.getEntity()).thenReturn(entity2);
      when(entity2.getContent())
          .thenReturn(
              new java.io.ByteArrayInputStream("{\"numProtoSuccess\":3,\"errors\":[]}".getBytes()));

      when(mockHttpClient.execute(any(HttpUriRequest.class)))
          .thenReturn(response1)
          .thenReturn(response2);

      IDatabricksConnectionContext mockContext = mock(IDatabricksConnectionContext.class);
      when(mockContext.getHostUrl()).thenReturn("https://example.com");
      when(mockContext.isTelemetryCircuitBreakerEnabled()).thenReturn(false);

      TelemetryPushClient client =
          new TelemetryPushClient(
              false /* isAuthenticated */, mockContext, null, 3 /* maxRetries */);

      assertDoesNotThrow(() -> client.pushEvent(request));

      verify(mockHttpClient, times(2)).execute(any(HttpUriRequest.class));
    }
  }
}
