package com.databricks.jdbc.dbclient.impl.http;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.HTTPRequestType;
import com.databricks.jdbc.common.RequestRetryability;
import com.databricks.jdbc.exception.DatabricksHttpException;
import java.io.IOException;
import java.net.URI;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class HttpRequestTypeBasedRetryHandlerTest {

  @Mock private CloseableHttpClient mockHttpClient;
  @Mock private HttpUriRequest mockRequest;
  @Mock private CloseableHttpResponse mockResponse;
  @Mock private StatusLine mockStatusLine;
  @Mock private IDatabricksConnectionContext mockConnectionContext;

  // Helper method to set up basic request mocks (only when URI is needed for error handling)
  private void setupRequestURIForErrorHandling() {
    try {
      when(mockRequest.getURI()).thenReturn(new URI("TestURI"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void setupMockConnectionContextForErrorHandling() {
    // Mock connection context - needed for retry logic to work
    when(mockConnectionContext.getTemporarilyUnavailableRetryTimeout()).thenReturn(60000);
    when(mockConnectionContext.getRateLimitRetryTimeout()).thenReturn(60000);
    when(mockConnectionContext.getMaxRetries()).thenReturn(5);
  }

  @Test
  public void testSuccessfulRequestWithoutRetry() throws Exception {
    // Arrange
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockHttpClient.execute(eq(mockRequest))).thenReturn(mockResponse);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);

    // Act
    CloseableHttpResponse result =
        HttpRequestTypeBasedRetryHandler.executeWithRetry(
            mockHttpClient, mockRequest, HTTPRequestType.CLOUD_FETCH, mockConnectionContext);

    // Assert
    assertEquals(mockResponse, result);
    verify(mockHttpClient, times(1)).execute(eq(mockRequest));
  }

  @Test
  public void testIdempotentRequestRetryOnServerError() throws Exception {
    // Arrange
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    CloseableHttpResponse failureResponse = mock(CloseableHttpResponse.class);
    StatusLine failureStatusLine = mock(StatusLine.class);
    when(failureResponse.getStatusLine()).thenReturn(failureStatusLine);
    when(failureStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_INTERNAL_SERVER_ERROR);

    setupMockConnectionContextForErrorHandling();

    when(mockHttpClient.execute(eq(mockRequest)))
        .thenReturn(failureResponse)
        .thenReturn(failureResponse)
        .thenReturn(mockResponse);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);

    // Act
    CloseableHttpResponse result =
        HttpRequestTypeBasedRetryHandler.executeWithRetry(
            mockHttpClient, mockRequest, HTTPRequestType.CLOUD_FETCH, mockConnectionContext);

    // Assert
    assertSame(mockResponse, result);
    verify(mockHttpClient, times(3)).execute(eq(mockRequest));
    verify(failureResponse, times(2)).close();
  }

  @Test
  public void testSuccessfulIdempotentRequest() throws Exception {
    // Arrange
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockHttpClient.execute(eq(mockRequest))).thenReturn(mockResponse);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_BAD_REQUEST);

    // Act
    CloseableHttpResponse result =
        HttpRequestTypeBasedRetryHandler.executeWithRetry(
            mockHttpClient, mockRequest, HTTPRequestType.CLOUD_FETCH, mockConnectionContext);

    // Assert
    assertSame(mockResponse, result);
    verify(mockHttpClient, times(1)).execute(eq(mockRequest));
  }

  @Test
  public void testNonIdempotentRequestRetryOn503WithRetryAfter() throws Exception {
    // Arrange
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(mockConnectionContext.shouldRetryTemporarilyUnavailableError()).thenReturn(true);
    when(mockConnectionContext.getTemporarilyUnavailableRetryTimeout())
        .thenReturn(900); // 15 minutes timeout
    setupMockConnectionContextForErrorHandling();
    CloseableHttpResponse failureResponse = mock(CloseableHttpResponse.class);
    StatusLine failureStatusLine = mock(StatusLine.class);
    when(failureResponse.getStatusLine()).thenReturn(failureStatusLine);
    when(failureStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_SERVICE_UNAVAILABLE);
    when(failureResponse.containsHeader("Retry-After")).thenReturn(true);
    when(failureResponse.getFirstHeader("Retry-After"))
        .thenReturn(new BasicHeader("Retry-After", "1"));

    when(mockHttpClient.execute(eq(mockRequest)))
        .thenReturn(failureResponse)
        .thenReturn(mockResponse);

    // Act
    CloseableHttpResponse result =
        HttpRequestTypeBasedRetryHandler.executeWithRetry(
            mockHttpClient,
            mockRequest,
            HTTPRequestType.THRIFT_EXECUTE_STATEMENT,
            mockConnectionContext);

    // Assert
    assertSame(mockResponse, result);
    verify(mockHttpClient, times(2)).execute(eq(mockRequest));
    verify(failureResponse).close();
  }

  @Test
  public void testNonIdempotentRequestNoRetryOn503WithoutRetryAfter() throws Exception {
    // Arrange
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockConnectionContext.shouldRetryTemporarilyUnavailableError()).thenReturn(true);
    when(mockHttpClient.execute(eq(mockRequest))).thenReturn(mockResponse);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_SERVICE_UNAVAILABLE);
    when(mockResponse.containsHeader("Retry-After")).thenReturn(false);

    // Act
    CloseableHttpResponse result =
        HttpRequestTypeBasedRetryHandler.executeWithRetry(
            mockHttpClient,
            mockRequest,
            HTTPRequestType.THRIFT_EXECUTE_STATEMENT,
            mockConnectionContext);

    // Assert
    assertSame(mockResponse, result);
    verify(mockHttpClient, times(1)).execute(eq(mockRequest));
  }

  @Test
  public void testNonIdempotentRequestNoRetryWhenConfigurationDisabled() throws Exception {
    // Arrange
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockConnectionContext.shouldRetryTemporarilyUnavailableError()).thenReturn(false);
    when(mockHttpClient.execute(eq(mockRequest))).thenReturn(mockResponse);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_SERVICE_UNAVAILABLE);

    // Act
    CloseableHttpResponse result =
        HttpRequestTypeBasedRetryHandler.executeWithRetry(
            mockHttpClient,
            mockRequest,
            HTTPRequestType.THRIFT_EXECUTE_STATEMENT,
            mockConnectionContext);

    // Assert
    assertSame(mockResponse, result);
    verify(mockHttpClient, times(1)).execute(eq(mockRequest));
  }

  @Test
  public void testMaxRetriesExceeded() throws Exception {
    // Arrange
    CloseableHttpResponse failureResponse = mock(CloseableHttpResponse.class);
    StatusLine failureStatusLine = mock(StatusLine.class);
    when(failureResponse.getStatusLine()).thenReturn(failureStatusLine);
    when(failureStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    when(failureResponse.containsHeader("Retry-After")).thenReturn(false); // No retry-after header

    setupMockConnectionContextForErrorHandling();

    when(mockHttpClient.execute(eq(mockRequest))).thenReturn(failureResponse);

    // Act
    CloseableHttpResponse result =
        HttpRequestTypeBasedRetryHandler.executeWithRetry(
            mockHttpClient, mockRequest, HTTPRequestType.CLOUD_FETCH, mockConnectionContext);

    // Assert
    assertSame(failureResponse, result);
    verify(mockHttpClient, times(6)).execute(eq(mockRequest)); // 5 retries + initial
    verify(failureResponse, times(5))
        .close(); // 5 responses closed during retries, final one returned
  }

  @Test
  public void testIOExceptionRetry() throws Exception {
    // Arrange
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockHttpClient.execute(eq(mockRequest)))
        .thenThrow(new IOException("Network error"))
        .thenReturn(mockResponse);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    setupMockConnectionContextForErrorHandling();

    // Act
    CloseableHttpResponse result =
        HttpRequestTypeBasedRetryHandler.executeWithRetry(
            mockHttpClient, mockRequest, HTTPRequestType.CLOUD_FETCH, mockConnectionContext);

    // Assert
    assertSame(mockResponse, result);
    verify(mockHttpClient, times(2)).execute(eq(mockRequest));
  }

  @Test
  public void testIOExceptionMaxRetriesExceeded() throws Exception {
    // Arrange
    when(mockHttpClient.execute(eq(mockRequest))).thenThrow(new IOException("Network error"));

    setupMockConnectionContextForErrorHandling();

    // Act
    CloseableHttpResponse result =
        HttpRequestTypeBasedRetryHandler.executeWithRetry(
            mockHttpClient, mockRequest, HTTPRequestType.CLOUD_FETCH, mockConnectionContext);

    // Assert
    assertNull(result);
    verify(mockHttpClient, times(6)).execute(eq(mockRequest)); // 5 retries + initial
  }

  @Test
  public void testRuntimeExceptionThrowsDatabricksHttpException() throws Exception {
    // Arrange
    setupRequestURIForErrorHandling();
    RuntimeException runtimeException = new RuntimeException("Test runtime exception");
    when(mockHttpClient.execute(eq(mockRequest))).thenThrow(runtimeException);

    // Act & Assert
    DatabricksHttpException exception =
        assertThrows(
            DatabricksHttpException.class,
            () ->
                HttpRequestTypeBasedRetryHandler.executeWithRetry(
                    mockHttpClient,
                    mockRequest,
                    HTTPRequestType.CLOUD_FETCH,
                    mockConnectionContext));

    assertTrue(exception.getMessage().contains("Caught error while executing http request"));
  }

  @Test
  public void testAccumulatedTimeTrackingFor503() throws Exception {
    // Arrange
    CloseableHttpResponse failureResponse = mock(CloseableHttpResponse.class);
    StatusLine failureStatusLine = mock(StatusLine.class);
    when(failureResponse.getStatusLine()).thenReturn(failureStatusLine);
    when(failureStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_SERVICE_UNAVAILABLE);
    when(failureResponse.containsHeader("Retry-After")).thenReturn(true);
    when(failureResponse.getFirstHeader("Retry-After"))
        .thenReturn(new BasicHeader("Retry-After", "10"));

    when(mockConnectionContext.shouldRetryTemporarilyUnavailableError()).thenReturn(true);
    when(mockConnectionContext.getTemporarilyUnavailableRetryTimeout())
        .thenReturn(5); // 5 seconds timeout

    when(mockHttpClient.execute(eq(mockRequest))).thenReturn(failureResponse);

    // Act
    CloseableHttpResponse result =
        HttpRequestTypeBasedRetryHandler.executeWithRetry(
            mockHttpClient,
            mockRequest,
            HTTPRequestType.THRIFT_EXECUTE_STATEMENT,
            mockConnectionContext);

    // Assert
    assertSame(failureResponse, result); // Should return immediately due to timeout exceeded
    verify(mockHttpClient, times(1)).execute(eq(mockRequest));
  }

  @Test
  public void testAccumulatedTimeTrackingFor429() throws Exception {
    // Arrange
    CloseableHttpResponse failureResponse = mock(CloseableHttpResponse.class);
    StatusLine failureStatusLine = mock(StatusLine.class);
    when(failureResponse.getStatusLine()).thenReturn(failureStatusLine);
    when(failureStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_TOO_MANY_REQUESTS);
    when(failureResponse.containsHeader("Retry-After")).thenReturn(true);
    when(failureResponse.getFirstHeader("Retry-After"))
        .thenReturn(new BasicHeader("Retry-After", "10"));

    when(mockConnectionContext.shouldRetryRateLimitError()).thenReturn(true);
    when(mockConnectionContext.getRateLimitRetryTimeout()).thenReturn(5); // 5 seconds timeout

    when(mockHttpClient.execute(eq(mockRequest))).thenReturn(failureResponse);

    // Act
    CloseableHttpResponse result =
        HttpRequestTypeBasedRetryHandler.executeWithRetry(
            mockHttpClient,
            mockRequest,
            HTTPRequestType.THRIFT_EXECUTE_STATEMENT,
            mockConnectionContext);

    // Assert
    assertSame(failureResponse, result); // Should return immediately due to timeout exceeded
    verify(mockHttpClient, times(1)).execute(eq(mockRequest));
  }

  @Test
  public void testRequestRetryabilityForKnownTypes() {
    // Test idempotent types
    assertEquals(
        RequestRetryability.IDEMPOTENT, HTTPRequestType.CLOUD_FETCH.getRequestRetryability());
    assertEquals(
        RequestRetryability.IDEMPOTENT,
        HTTPRequestType.THRIFT_OPEN_SESSION.getRequestRetryability());
    assertEquals(
        RequestRetryability.IDEMPOTENT, HTTPRequestType.VOLUME_GET.getRequestRetryability());

    // Test non-idempotent types
    assertEquals(
        RequestRetryability.NON_IDEMPOTENT,
        HTTPRequestType.THRIFT_EXECUTE_STATEMENT.getRequestRetryability());
    assertEquals(
        RequestRetryability.NON_IDEMPOTENT, HTTPRequestType.VOLUME_PUT.getRequestRetryability());
    assertEquals(
        RequestRetryability.NON_IDEMPOTENT, HTTPRequestType.UNKNOWN.getRequestRetryability());
  }

  @Test
  public void testRetryAfterHeaderExtraction() throws Exception {
    // Arrange
    CloseableHttpResponse responseWithValidHeader = mock(CloseableHttpResponse.class);
    CloseableHttpResponse responseWithInvalidHeader = mock(CloseableHttpResponse.class);
    CloseableHttpResponse responseWithoutHeader = mock(CloseableHttpResponse.class);

    when(responseWithValidHeader.containsHeader("Retry-After")).thenReturn(true);
    when(responseWithValidHeader.getFirstHeader("Retry-After"))
        .thenReturn(new BasicHeader("Retry-After", "30"));

    when(responseWithInvalidHeader.containsHeader("Retry-After")).thenReturn(true);
    when(responseWithInvalidHeader.getFirstHeader("Retry-After"))
        .thenReturn(new BasicHeader("Retry-After", "invalid"));

    when(responseWithoutHeader.containsHeader("Retry-After")).thenReturn(false);

    // Test valid header extraction through the strategy
    NonIdempotentRetryStrategy strategy = new NonIdempotentRetryStrategy();

    // Set up connection context to allow retries for 503 errors
    when(mockConnectionContext.shouldRetryTemporarilyUnavailableError()).thenReturn(true);

    StatusLine statusLine503 = mock(StatusLine.class);
    when(statusLine503.getStatusCode()).thenReturn(HttpStatus.SC_SERVICE_UNAVAILABLE);
    when(responseWithValidHeader.getStatusLine()).thenReturn(statusLine503);

    int retryDelay = strategy.retryRequestAfter(responseWithValidHeader, 1, mockConnectionContext);
    assertEquals(30000, retryDelay); // 30 seconds converted to milliseconds

    // Test invalid header returns -1
    when(responseWithInvalidHeader.getStatusLine()).thenReturn(statusLine503);
    retryDelay = strategy.retryRequestAfter(responseWithInvalidHeader, 1, mockConnectionContext);
    assertEquals(-1, retryDelay);

    // Test missing header returns -1
    when(responseWithoutHeader.getStatusLine()).thenReturn(statusLine503);
    retryDelay = strategy.retryRequestAfter(responseWithoutHeader, 1, mockConnectionContext);
    assertEquals(-1, retryDelay);
  }

  @Test
  public void testExponentialBackoffCalculation() {
    IdempotentRetryStrategy strategy = new IdempotentRetryStrategy();

    // Set up mock response with retriable status code
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    when(mockResponse.containsHeader("Retry-After")).thenReturn(false); // No retry-after header

    // Note: This test directly calls the retry strategy, no connection context needed

    // Test exponential backoff progression
    int delay1 = strategy.retryRequestAfter(mockResponse, 1, mockConnectionContext);
    int delay2 = strategy.retryRequestAfter(mockResponse, 2, mockConnectionContext);
    int delay3 = strategy.retryRequestAfter(mockResponse, 3, mockConnectionContext);

    // Verify exponential growth (with minimum 1s and maximum 10s)
    assertEquals(1000, delay1);
    assertTrue(delay1 >= 1000); // At least 1 second
    assertTrue(delay2 > delay1); // Should increase
    assertTrue(delay3 > delay2); // Should increase
    assertTrue(delay3 <= 10000); // Should not exceed 10 seconds
  }
}
