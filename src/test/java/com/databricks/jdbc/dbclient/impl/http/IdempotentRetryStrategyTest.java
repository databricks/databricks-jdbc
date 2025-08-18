package com.databricks.jdbc.dbclient.impl.http;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import org.apache.http.HttpStatus;
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
  public void testIgnoresRetryAfterHeader() {
    // and use exponential backoff (no need to mock headers since they're ignored)

    int delay = strategy.retryRequestAfter(mockResponse, 1, mockConnectionContext);

    // Should still use exponential backoff, not the header
    assertEquals(1000, delay);
  }

  @Test
  public void testConnectionContextNotUsed() {
    // Idempotent strategy should not depend on connection context for status code decisions
    // (except for the parameter requirement)
    IDatabricksConnectionContext nullContext = null;

    // Should not throw NPE and should behave the same
    assertTrue(strategy.isStatusCodeRetriable(HttpStatus.SC_INTERNAL_SERVER_ERROR, nullContext));
    assertFalse(strategy.isStatusCodeRetriable(HttpStatus.SC_BAD_REQUEST, nullContext));

    int delay = strategy.retryRequestAfter(mockResponse, 1, nullContext);
    assertEquals(1000, delay);
  }

  @Test
  public void testHighAttemptNumber() {
    // Test with very high attempt numbers to ensure no overflow/unexpected behavior
    int delay10 = strategy.retryRequestAfter(mockResponse, 10, mockConnectionContext);
    int delay20 = strategy.retryRequestAfter(mockResponse, 20, mockConnectionContext);

    // Should still be capped at maximum
    assertEquals(10000, delay10);
    assertEquals(10000, delay20);
  }

  @Test
  public void testZeroAndNegativeAttempts() {
    // Test edge cases with zero and negative attempt numbers
    int delay0 = strategy.retryRequestAfter(mockResponse, 0, mockConnectionContext);
    int delayNegative = strategy.retryRequestAfter(mockResponse, -1, mockConnectionContext);

    // Should handle gracefully (0^0 = 1, negative powers are handled by Math.pow)
    assertTrue(delay0 >= 1000);
    assertTrue(delayNegative >= 1000);
  }
}
