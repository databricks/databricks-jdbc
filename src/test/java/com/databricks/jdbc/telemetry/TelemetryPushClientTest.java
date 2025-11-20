package com.databricks.jdbc.telemetry;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.exception.DatabricksTelemetryException;
import com.databricks.jdbc.model.telemetry.TelemetryRequest;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
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
      when(mockFactory.getClient(any())).thenReturn(mockHttpClient);

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
      when(mockFactory.getClient(any())).thenReturn(mockHttpClient);

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
      when(mockFactory.getClient(any())).thenReturn(mockHttpClient);

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

  @ParameterizedTest
  @ValueSource(ints = {400, 500, 502, 503, 504})
  public void pushEvent_doesNotThrow_onErrorCodes_whenCBDisabled(int statusCode) throws Exception {
    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        org.mockito.Mockito.mockStatic(DatabricksHttpClientFactory.class)) {
      DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
      factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);

      IDatabricksHttpClient mockHttpClient = mock(IDatabricksHttpClient.class);
      when(mockFactory.getClient(any())).thenReturn(mockHttpClient);

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
}
