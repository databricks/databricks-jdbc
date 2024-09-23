package com.databricks.jdbc.dbclient.impl.http;

import static com.databricks.jdbc.dbclient.impl.http.DatabricksHttpRetryHandler.isRequestMethodRetryable;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.exception.DatabricksRetryHandlerException;
import java.io.IOException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.methods.HttpPost;
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
    retryHandler = new DatabricksHttpRetryHandler(mockConnectionContext, false);
  }

  @Test
  void processWithNonRetryableStatusCode() throws IOException {
    HttpResponse response = createMockResponse(HttpStatus.SC_OK);
    retryHandler.process(response, mockHttpContext);
    // No exception should be thrown, and no attributes should be set
    verify(mockHttpContext, never()).setAttribute(anyString(), any());
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
    IOException exception = getRetryHandlerException(HttpStatus.SC_BAD_REQUEST);
    assertFalse(retryHandler.retryRequest(exception, 1, mockHttpContext));
  }

  @Test
  void retryRequest_cloudFetch() {
    when(mockHttpContext.getAttribute(anyString())).thenReturn(0);
    when(mockHttpContext.getRequest()).thenReturn(new HttpPost());
    retryHandler = new DatabricksHttpRetryHandler(mockConnectionContext, true, 0, 0);
    IOException exception = getRetryHandlerException(HttpStatus.SC_BAD_REQUEST);
    assertFalse(retryHandler.retryRequest(exception, 1, mockHttpContext));

    exception = getRetryHandlerException(HttpStatus.SC_SERVICE_UNAVAILABLE);
    assertTrue(retryHandler.retryRequest(exception, 1, mockHttpContext));

    exception = getRetryHandlerException(HttpStatus.SC_REQUEST_TIMEOUT);
    assertTrue(retryHandler.retryRequest(exception, 1, mockHttpContext));

    exception = getRetryHandlerException(HttpStatus.SC_TOO_MANY_REQUESTS);
    assertTrue(retryHandler.retryRequest(exception, 1, mockHttpContext));

    exception = getRetryHandlerException(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    assertTrue(retryHandler.retryRequest(exception, 1, mockHttpContext));

    exception = getRetryHandlerException(HttpStatus.SC_GATEWAY_TIMEOUT);
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
        new DatabricksRetryHandlerException(
            "Test", HttpStatus.SC_SERVICE_UNAVAILABLE, mockConnectionContext, null, "");
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

    IOException exception = getRetryHandlerException(HttpStatus.SC_SERVICE_UNAVAILABLE);
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
    DatabricksHttpRetryHandler retryHandler =
        new DatabricksHttpRetryHandler(mockConnectionContext, false);
    assertEquals(5000, retryHandler.calculateDelay(HttpStatus.SC_SERVICE_UNAVAILABLE, 1, 5000));
    assertEquals(5000, retryHandler.calculateDelay(HttpStatus.SC_TOO_MANY_REQUESTS, 1, 5000));
    assertTrue(retryHandler.calculateDelay(HttpStatus.SC_INTERNAL_SERVER_ERROR, 1, 0) >= 1000);
    assertTrue(retryHandler.calculateDelay(HttpStatus.SC_INTERNAL_SERVER_ERROR, 5, 0) <= 10000);
  }

  private HttpResponse createMockResponse(int statusCode) {
    return new BasicHttpResponse(
        new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), statusCode, ""));
  }

  private HttpRequest createMockRequest() {
    return new BasicHttpRequest(
        new BasicRequestLine("GET", "http://databricks", new ProtocolVersion("HTTP", 1, 1)));
  }

  private DatabricksRetryHandlerException getRetryHandlerException(int httpErrorCode) {
    return new DatabricksRetryHandlerException(
        "Test", httpErrorCode, mockConnectionContext, null, "");
  }
}
