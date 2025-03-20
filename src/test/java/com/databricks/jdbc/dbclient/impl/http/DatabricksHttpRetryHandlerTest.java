package com.databricks.jdbc.dbclient.impl.http;

import static com.databricks.jdbc.dbclient.impl.http.DatabricksHttpRetryHandler.isRequestMethodRetryable;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.exception.DatabricksRetryHandlerException;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicRequestLine;
import org.apache.http.message.BasicStatusLine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksHttpRetryHandlerTest {

  @Mock private IDatabricksConnectionContext mockConnectionContext;
  @Mock private HttpClientContext mockHttpContext;
  private DatabricksHttpRetryHandler retryHandler;

  @BeforeEach
  public void setUp() {
    retryHandler = new DatabricksHttpRetryHandler(mockConnectionContext, HttpClientType.COMMON);
  }

  @Test
  void processWithNonRetryableStatusCode() throws IOException {
    HttpResponse response = createMockResponse(HttpStatus.SC_OK);
    retryHandler.process(response, mockHttpContext);
    // No exception should be thrown, and no attributes should be set
    verify(mockHttpContext, never()).setAttribute(anyString(), any());
  }

  @Test
  void processUCVolumeRequestWithNonRetryableStatusCode() throws IOException {
    when(mockConnectionContext.getUCIngestionRetriableHttpCodes())
        .thenReturn(List.of(HttpStatus.SC_SERVICE_UNAVAILABLE, HttpStatus.SC_BAD_REQUEST));

    retryHandler = new DatabricksHttpRetryHandler(mockConnectionContext, HttpClientType.VOLUME);
    HttpResponse response = createMockResponse(HttpStatus.SC_OK);
    retryHandler.process(response, mockHttpContext);
    // No exception should be thrown, and no attributes should be set
    verify(mockHttpContext, never()).setAttribute(anyString(), any());
  }

  @Test
  void processUCVolumeRequestWithRetryableStatusCode() throws IOException {
    when(mockConnectionContext.getUCIngestionRetriableHttpCodes())
        .thenReturn(List.of(HttpStatus.SC_SERVICE_UNAVAILABLE, HttpStatus.SC_BAD_REQUEST));

    retryHandler = new DatabricksHttpRetryHandler(mockConnectionContext, HttpClientType.VOLUME);
    HttpResponse response = createMockResponse(HttpStatus.SC_BAD_REQUEST);
    retryHandler.process(response, mockHttpContext);
    // No exception should be thrown, and retry start time attributes should be set
    verify(mockHttpContext).setAttribute(eq(DatabricksHttpRetryHandler.RETRY_START_TIME), any());
  }

  @Test
  void processWithRetryableStatusCodeAndRetryAfterHeader() {
    when(mockConnectionContext.shouldRetryTemporarilyUnavailableError()).thenReturn(true);
    HttpResponse response = createMockResponse(HttpStatus.SC_SERVICE_UNAVAILABLE);
    response.setHeader("Retry-After", "5");

    assertThrows(
        DatabricksRetryHandlerException.class,
        () -> retryHandler.process(response, mockHttpContext));
  }

  @Test
  void retryRequestWithNonRetryableStatusCode() {
    IOException exception = new DatabricksRetryHandlerException("Test", HttpStatus.SC_BAD_REQUEST);
    assertFalse(retryHandler.retryRequest(exception, 1, mockHttpContext));
  }

  @Test
  void retryUCVolumeRequestWithNonRetryableStatusCode() throws Exception {
    when(mockConnectionContext.getUCIngestionRetriableHttpCodes())
        .thenReturn(List.of(HttpStatus.SC_SERVICE_UNAVAILABLE, HttpStatus.SC_BAD_REQUEST));

    retryHandler = new DatabricksHttpRetryHandler(mockConnectionContext, HttpClientType.VOLUME);

    IOException exception = new DatabricksRetryHandlerException("Test", HttpStatus.SC_BAD_GATEWAY);
    assertFalse(retryHandler.retryRequest(exception, 1, mockHttpContext));
  }

  @Test
  void retryUCVolumeRequestWithRetryableStatusCodeTimeout() throws Exception {
    when(mockConnectionContext.getUCIngestionRetriableHttpCodes())
        .thenReturn(List.of(HttpStatus.SC_SERVICE_UNAVAILABLE, HttpStatus.SC_BAD_REQUEST));
    when(mockConnectionContext.getUCIngestionRetryTimeoutMinutes()).thenReturn(1);
    when(mockHttpContext.getAttribute(DatabricksHttpRetryHandler.RETRY_START_TIME))
        .thenReturn(Instant.now().minusSeconds(100));

    retryHandler = new DatabricksHttpRetryHandler(mockConnectionContext, HttpClientType.VOLUME);

    IOException exception = new DatabricksRetryHandlerException("Test", HttpStatus.SC_BAD_REQUEST);
    assertFalse(retryHandler.retryRequest(exception, 1, mockHttpContext));
  }

  @Test
  void retryUCVolumeRequestWithRetryableStatusCode() throws Exception {
    when(mockConnectionContext.getUCIngestionRetriableHttpCodes())
        .thenReturn(List.of(HttpStatus.SC_SERVICE_UNAVAILABLE, HttpStatus.SC_BAD_REQUEST));
    when(mockConnectionContext.getUCIngestionRetryTimeoutMinutes()).thenReturn(2);
    when(mockHttpContext.getAttribute(DatabricksHttpRetryHandler.RETRY_START_TIME))
        .thenReturn(Instant.now().minusSeconds(100));

    retryHandler = new DatabricksHttpRetryHandler(mockConnectionContext, HttpClientType.VOLUME);

    IOException exception = new DatabricksRetryHandlerException("Test", HttpStatus.SC_BAD_REQUEST);
    assertTrue(retryHandler.retryRequest(exception, 1, mockHttpContext));
  }

  @Test
  void retryRequestWithRetryableStatusCodeAndValidRetryInterval() {
    when(mockConnectionContext.shouldRetryTemporarilyUnavailableError()).thenReturn(true);
    when(mockConnectionContext.getTemporarilyUnavailableRetryTimeout()).thenReturn(30000);
    when(mockHttpContext.getAttribute("retryInterval")).thenReturn(5);
    when(mockHttpContext.getAttribute("tempUnavailableRetryCount")).thenReturn(0);
    when(mockHttpContext.getAttribute("rateLimitRetryCount")).thenReturn(0);
    when(mockHttpContext.getRequest()).thenReturn(createMockRequest());

    IOException exception =
        new DatabricksRetryHandlerException("Test", HttpStatus.SC_SERVICE_UNAVAILABLE);
    assertTrue(retryHandler.retryRequest(exception, 1, mockHttpContext));
  }

  @Test
  void retryRequestExceedingMaxRetries() {
    when(mockConnectionContext.shouldRetryTemporarilyUnavailableError()).thenReturn(true);
    when(mockConnectionContext.getTemporarilyUnavailableRetryTimeout()).thenReturn(30000);
    when(mockHttpContext.getAttribute("retryInterval")).thenReturn(5);
    when(mockHttpContext.getAttribute("tempUnavailableRetryCount")).thenReturn(0);
    when(mockHttpContext.getAttribute("rateLimitRetryCount")).thenReturn(0);
    when(mockHttpContext.getRequest()).thenReturn(createMockRequest());

    IOException exception =
        new DatabricksRetryHandlerException("Test", HttpStatus.SC_SERVICE_UNAVAILABLE);
    assertFalse(retryHandler.retryRequest(exception, 6, mockHttpContext));
  }

  @Test
  void testIsRequestMethodRetryable() {
    assertTrue(isRequestMethodRetryable("GET"), "GET requests should be allowed for retry");
    assertTrue(isRequestMethodRetryable("POST"), "POST requests should be allowed for retry");
    assertTrue(isRequestMethodRetryable("PUT"), "PUT requests should be allowed for retry");
    assertFalse(
        isRequestMethodRetryable("DELETE"), "DELETE requests should not be allowed for retry");
  }

  @Test
  void calculateDelay() {
    assertEquals(
        5000,
        DatabricksHttpRetryHandler.calculateDelay(HttpStatus.SC_SERVICE_UNAVAILABLE, 1, 5000));
    assertEquals(
        5000, DatabricksHttpRetryHandler.calculateDelay(HttpStatus.SC_TOO_MANY_REQUESTS, 1, 5000));
    assertTrue(
        DatabricksHttpRetryHandler.calculateDelay(HttpStatus.SC_INTERNAL_SERVER_ERROR, 1, 0)
            >= 1000);
    assertTrue(
        DatabricksHttpRetryHandler.calculateDelay(HttpStatus.SC_INTERNAL_SERVER_ERROR, 5, 0)
            <= 10000);
  }

  private HttpResponse createMockResponse(int statusCode) {
    return new BasicHttpResponse(
        new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), statusCode, ""));
  }

  private HttpRequest createMockRequest() {
    return new BasicHttpRequest(
        new BasicRequestLine("GET", "http://databricks", new ProtocolVersion("HTTP", 1, 1)));
  }
}
