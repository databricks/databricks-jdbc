package com.databricks.jdbc.dbclient.impl.http;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.exception.DatabricksRetryHandlerException;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class UCVolumeHttpRetryHandlerTest {

  @Mock private IDatabricksConnectionContext mockConnectionContext;
  private HttpClientContext httpContext;
  private UCVolumeHttpRetryHandler retryHandler;

  @BeforeEach
  public void setUp() {
    retryHandler = new UCVolumeHttpRetryHandler(mockConnectionContext);
    httpContext = HttpClientContext.create();
  }

  @Test
  void processUCVolumeRequestWithNonRetryableStatusCode() throws IOException {
    when(mockConnectionContext.getUCIngestionRetriableHttpCodes())
        .thenReturn(List.of(HttpStatus.SC_SERVICE_UNAVAILABLE, HttpStatus.SC_BAD_REQUEST));

    HttpResponse response = createResponse(HttpStatus.SC_OK);
    retryHandler.process(response, httpContext);

    assertNull(httpContext.getAttribute(UCVolumeHttpRetryHandler.RETRY_START_TIME_KEY));
  }

  @Test
  void processUCVolumeRequestWithRetryableStatusCode() throws IOException {
    when(mockConnectionContext.getUCIngestionRetriableHttpCodes())
        .thenReturn(List.of(HttpStatus.SC_SERVICE_UNAVAILABLE, HttpStatus.SC_BAD_REQUEST));

    HttpResponse response = createResponse(HttpStatus.SC_BAD_REQUEST);
    retryHandler.process(response, httpContext);

    assertNotNull(httpContext.getAttribute(UCVolumeHttpRetryHandler.RETRY_START_TIME_KEY));
  }

  @Test
  void retryUCVolumeRequestWithNonRetryableStatusCode() throws Exception {
    when(mockConnectionContext.getUCIngestionRetriableHttpCodes())
        .thenReturn(List.of(HttpStatus.SC_SERVICE_UNAVAILABLE, HttpStatus.SC_BAD_REQUEST));

    IOException exception = new DatabricksRetryHandlerException("Test", HttpStatus.SC_BAD_GATEWAY);
    assertFalse(retryHandler.retryRequest(exception, 1, httpContext));
  }

  @Test
  void retryUCVolumeRequestWithRetryableStatusCodeTimeout() throws Exception {
    when(mockConnectionContext.getUCIngestionRetriableHttpCodes())
        .thenReturn(List.of(HttpStatus.SC_SERVICE_UNAVAILABLE, HttpStatus.SC_BAD_REQUEST));
    when(mockConnectionContext.getUCIngestionRetryTimeoutSeconds()).thenReturn(60);

    httpContext.setAttribute(
        UCVolumeHttpRetryHandler.RETRY_START_TIME_KEY, Instant.now().minusSeconds(100));
    httpContext.setAttribute(DatabricksHttpRetryHandler.RETRY_INTERVAL_KEY, -1);

    IOException exception = new DatabricksRetryHandlerException("Test", HttpStatus.SC_BAD_REQUEST);
    assertFalse(retryHandler.retryRequest(exception, 1, httpContext));
  }

  @Test
  void retryUCVolumeRequestWithRetryableStatusCode() throws Exception {
    when(mockConnectionContext.getUCIngestionRetriableHttpCodes())
        .thenReturn(List.of(HttpStatus.SC_SERVICE_UNAVAILABLE, HttpStatus.SC_BAD_REQUEST));
    when(mockConnectionContext.getUCIngestionRetryTimeoutSeconds()).thenReturn(120);

    httpContext.setAttribute(
        UCVolumeHttpRetryHandler.RETRY_START_TIME_KEY, Instant.now().minusSeconds(100));
    httpContext.setAttribute(DatabricksHttpRetryHandler.RETRY_INTERVAL_KEY, -1);

    IOException exception = new DatabricksRetryHandlerException("Test", HttpStatus.SC_BAD_REQUEST);
    assertTrue(retryHandler.retryRequest(exception, 1, httpContext));
  }

  @Test
  void testUCVolumeRetryTimeout() throws IOException {
    when(mockConnectionContext.getUCIngestionRetriableHttpCodes())
        .thenReturn(List.of(HttpStatus.SC_SERVICE_UNAVAILABLE, HttpStatus.SC_BAD_REQUEST));
    when(mockConnectionContext.getUCIngestionRetryTimeoutSeconds()).thenReturn(10);

    httpContext.setAttribute(UCVolumeHttpRetryHandler.RETRY_START_TIME_KEY, Instant.now());
    httpContext.setAttribute(DatabricksHttpRetryHandler.RETRY_INTERVAL_KEY, -1);

    // First attempt: should retry
    assertTrue(
        retryHandler.retryRequest(
            new DatabricksRetryHandlerException("Test", HttpStatus.SC_SERVICE_UNAVAILABLE),
            1,
            httpContext));

    // Second attempt: should retry
    assertTrue(
        retryHandler.retryRequest(
            new DatabricksRetryHandlerException("Test", HttpStatus.SC_SERVICE_UNAVAILABLE),
            2,
            httpContext));

    // Third attempt: should not retry
    assertFalse(
        retryHandler.retryRequest(
            new DatabricksRetryHandlerException("Test", HttpStatus.SC_SERVICE_UNAVAILABLE),
            3,
            httpContext));
  }

  private HttpResponse createResponse(int statusCode) {
    return new BasicHttpResponse(
        new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), statusCode, ""));
  }
}
