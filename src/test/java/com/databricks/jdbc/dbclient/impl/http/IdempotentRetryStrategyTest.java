package com.databricks.jdbc.dbclient.impl.http;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class IdempotentRetryStrategyTest {

  private IdempotentRetryStrategy strategy;

  @Mock private CloseableHttpResponse mockResponse;
  @Mock private IDatabricksConnectionContext mockConnectionContext;

  @BeforeEach
  public void setUp() {
    strategy = new IdempotentRetryStrategy();
  }

  @Test
  public void testRetriableStatusCodes() {
    // Set up mock connection context to allow retries for 503 and 429 status codes
    when(mockConnectionContext.shouldRetryTemporarilyUnavailableError()).thenReturn(true);
    when(mockConnectionContext.shouldRetryRateLimitError()).thenReturn(true);

    // Retriable codes (all except non-retryable client errors and successful 2xx codes)
    assertTrue(
        strategy.isStatusCodeRetriable(HttpStatus.SC_INTERNAL_SERVER_ERROR, mockConnectionContext));
    assertTrue(strategy.isStatusCodeRetriable(HttpStatus.SC_BAD_GATEWAY, mockConnectionContext));
    assertTrue(
        strategy.isStatusCodeRetriable(HttpStatus.SC_SERVICE_UNAVAILABLE, mockConnectionContext));
    assertTrue(
        strategy.isStatusCodeRetriable(HttpStatus.SC_GATEWAY_TIMEOUT, mockConnectionContext));
    assertTrue(
        strategy.isStatusCodeRetriable(HttpStatus.SC_TOO_MANY_REQUESTS, mockConnectionContext));
    assertTrue(
        strategy.isStatusCodeRetriable(HttpStatus.SC_REQUEST_TIMEOUT, mockConnectionContext));
  }

  @Test
  public void testNonRetriableSuccessfulStatusCodes() {
    // Successful 2xx codes should not be retried
    assertFalse(strategy.isStatusCodeRetriable(HttpStatus.SC_OK, mockConnectionContext));
    assertFalse(strategy.isStatusCodeRetriable(HttpStatus.SC_CREATED, mockConnectionContext));
    assertFalse(strategy.isStatusCodeRetriable(HttpStatus.SC_ACCEPTED, mockConnectionContext));
    assertFalse(strategy.isStatusCodeRetriable(HttpStatus.SC_NO_CONTENT, mockConnectionContext));
  }

  @Test
  public void testNonRetriableClientErrors() {
    // Non-retriable client errors
    assertFalse(strategy.isStatusCodeRetriable(HttpStatus.SC_BAD_REQUEST, mockConnectionContext));
    assertFalse(strategy.isStatusCodeRetriable(HttpStatus.SC_UNAUTHORIZED, mockConnectionContext));
    assertFalse(strategy.isStatusCodeRetriable(HttpStatus.SC_FORBIDDEN, mockConnectionContext));
    assertFalse(strategy.isStatusCodeRetriable(HttpStatus.SC_NOT_FOUND, mockConnectionContext));
    assertFalse(
        strategy.isStatusCodeRetriable(HttpStatus.SC_METHOD_NOT_ALLOWED, mockConnectionContext));
    assertFalse(strategy.isStatusCodeRetriable(HttpStatus.SC_CONFLICT, mockConnectionContext));
    assertFalse(strategy.isStatusCodeRetriable(HttpStatus.SC_GONE, mockConnectionContext));
    assertFalse(
        strategy.isStatusCodeRetriable(HttpStatus.SC_LENGTH_REQUIRED, mockConnectionContext));
    assertFalse(
        strategy.isStatusCodeRetriable(HttpStatus.SC_PRECONDITION_FAILED, mockConnectionContext));
    assertFalse(
        strategy.isStatusCodeRetriable(HttpStatus.SC_REQUEST_TOO_LONG, mockConnectionContext));
    assertFalse(
        strategy.isStatusCodeRetriable(HttpStatus.SC_REQUEST_URI_TOO_LONG, mockConnectionContext));
    assertFalse(
        strategy.isStatusCodeRetriable(
            HttpStatus.SC_UNSUPPORTED_MEDIA_TYPE, mockConnectionContext));
    assertFalse(
        strategy.isStatusCodeRetriable(
            HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE, mockConnectionContext));
  }

  @Test
  public void testExponentialBackoffCalculation() {
    // Set up mock response with retriable status code
    StatusLine mockStatusLine = mock(StatusLine.class);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockResponse.containsHeader("Retry-After")).thenReturn(false);

    // Test that each attempt increases the delay exponentially
    int delay1 = strategy.retryRequestAfter(mockResponse, 1, mockConnectionContext);
    int delay2 = strategy.retryRequestAfter(mockResponse, 2, mockConnectionContext);
    int delay3 = strategy.retryRequestAfter(mockResponse, 3, mockConnectionContext);
    int delay4 = strategy.retryRequestAfter(mockResponse, 4, mockConnectionContext);
    int delay5 = strategy.retryRequestAfter(mockResponse, 5, mockConnectionContext);

    // Verify minimum delay
    assertTrue(delay1 >= 1000, "First delay should be at least 1 second");

    // Verify exponential growth
    assertTrue(delay2 >= delay1, "Second delay should be >= first delay");
    assertTrue(delay3 >= delay2, "Third delay should be >= second delay");
    assertTrue(delay4 >= delay3, "Fourth delay should be >= third delay");
    assertTrue(delay5 >= delay4, "Fifth delay should be >= fourth delay");

    // Verify maximum cap
    assertTrue(delay5 <= 10000, "Fifth delay should not exceed 10 seconds");
  }

  @Test
  public void testUsesRetryAfterHeaderWhenPresent() {
    // Set up mock response with retriable status code and retry-after header
    StatusLine mockStatusLine = mock(StatusLine.class);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);

    Header retryAfterHeader = mock(Header.class);
    when(retryAfterHeader.getValue()).thenReturn("30");
    when(mockResponse.containsHeader("Retry-After")).thenReturn(true);
    when(mockResponse.getFirstHeader("Retry-After")).thenReturn(retryAfterHeader);

    int delay = strategy.retryRequestAfter(mockResponse, 1, mockConnectionContext);

    // Should use the retry-after header value (30 seconds)
    assertEquals(30, delay);
  }

  @Test
  public void testFallsBackToExponentialBackoffWhenNoRetryAfterHeader() {
    // Set up mock response with retriable status code but no retry-after header
    StatusLine mockStatusLine = mock(StatusLine.class);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockResponse.containsHeader("Retry-After")).thenReturn(false);

    int delay = strategy.retryRequestAfter(mockResponse, 1, mockConnectionContext);

    // Should use exponential backoff (minimum 1 second)
    assertEquals(1000, delay);
  }

  @Test
  public void testNullConnectionContext() {
    // With null connection context, strategy should return false (no retry) for safety
    IDatabricksConnectionContext nullContext = null;

    // Should not throw NPE and should return false for all status codes when context is null
    assertFalse(strategy.isStatusCodeRetriable(HttpStatus.SC_INTERNAL_SERVER_ERROR, nullContext));
    assertFalse(strategy.isStatusCodeRetriable(HttpStatus.SC_SERVICE_UNAVAILABLE, nullContext));
    assertFalse(strategy.isStatusCodeRetriable(HttpStatus.SC_TOO_MANY_REQUESTS, nullContext));
    assertFalse(strategy.isStatusCodeRetriable(HttpStatus.SC_BAD_REQUEST, nullContext));

    // Set up mock response for delay calculation test
    StatusLine mockStatusLine = mock(StatusLine.class);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);

    // Delay calculation should return -1 for null context (no retry)
    int delay = strategy.retryRequestAfter(mockResponse, 1, nullContext);
    assertEquals(-1, delay);
  }

  @Test
  public void testHighAttemptNumber() {
    // Set up mock response with retriable status code
    StatusLine mockStatusLine = mock(StatusLine.class);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockResponse.containsHeader("Retry-After")).thenReturn(false);

    // Test with very high attempt numbers to ensure no overflow/unexpected behavior
    int delay10 = strategy.retryRequestAfter(mockResponse, 10, mockConnectionContext);
    int delay20 = strategy.retryRequestAfter(mockResponse, 20, mockConnectionContext);

    // Should still be capped at maximum
    assertEquals(10000, delay10);
    assertEquals(10000, delay20);
  }
}
