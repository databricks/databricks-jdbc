package com.databricks.jdbc.dbclient.impl.http;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RetryTimeoutManagerTest {

  private IDatabricksConnectionContext mockContext;
  private RetryTimeoutManager timeoutManager;

  @BeforeEach
  void setUp() {
    mockContext = mock(IDatabricksConnectionContext.class);
    when(mockContext.getTemporarilyUnavailableRetryTimeout()).thenReturn(120); // 120 seconds
    when(mockContext.getRateLimitRetryTimeout()).thenReturn(300); // 300 seconds
    when(mockContext.getApiRetryTimeout()).thenReturn(300); // 300 seconds for API retriable codes
    timeoutManager = new RetryTimeoutManager(mockContext);
  }

  @Test
  void testServiceUnavailableTimeoutExhausted() {
    // Make multiple retries that exhaust the service unavailable timeout (120 seconds)
    // Each retry delays for 30 seconds
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_SERVICE_UNAVAILABLE, 30000, false));
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_SERVICE_UNAVAILABLE, 30000, false));
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_SERVICE_UNAVAILABLE, 30000, false));
    // This should exhaust the timeout (30+30+30+31 > 120 seconds)
    assertFalse(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_SERVICE_UNAVAILABLE, 31000, false));
  }

  @Test
  void testRateLimitTimeoutExhausted() {
    // Make multiple retries that exhaust the rate limit timeout (300 seconds)
    // Each retry delays for 80 seconds
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_TOO_MANY_REQUESTS, 80000, false));
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_TOO_MANY_REQUESTS, 80000, false));
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_TOO_MANY_REQUESTS, 80000, false));
    // This should exhaust the timeout (80+80+80+61 > 300 seconds)
    assertFalse(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_TOO_MANY_REQUESTS, 61000, false));
  }

  @Test
  void testOtherErrorCodesTimeout() {
    // Test other error codes (e.g., 500) using the default 10-second timeout
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_INTERNAL_SERVER_ERROR, 3000, false));
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_INTERNAL_SERVER_ERROR, 3000, false));
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_INTERNAL_SERVER_ERROR, 3000, false));
    // This should exhaust the timeout (3+3+3+2 > 10 seconds)
    assertFalse(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_INTERNAL_SERVER_ERROR, 2000, false));
  }

  @Test
  void testExceptionTimeout() {
    // Test exception timeout (default 10 seconds)
    assertTrue(timeoutManager.evaluateRetryTimeoutForException(3000));
    assertTrue(timeoutManager.evaluateRetryTimeoutForException(3000));
    assertTrue(timeoutManager.evaluateRetryTimeoutForException(3000));
    // This should exhaust the timeout (3+3+3+2 > 10 seconds)
    assertFalse(timeoutManager.evaluateRetryTimeoutForException(2000));
  }

  @Test
  void testMixedRetries() {
    // Test combination of different status codes
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_SERVICE_UNAVAILABLE, 20000, false));
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_TOO_MANY_REQUESTS, 50000, false));
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_INTERNAL_SERVER_ERROR, 2000, false));
    assertTrue(timeoutManager.evaluateRetryTimeoutForException(2000));

    // All timeouts should still have capacity
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_SERVICE_UNAVAILABLE, 10000, false));
  }

  @Test
  void testImmediateTimeoutExhaustion() {
    // A single large delay can exhaust the timeout
    assertFalse(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_SERVICE_UNAVAILABLE, 121000, false)); // > 120 seconds
  }

  @Test
  void testApiRetriableCodesTimeout() {
    // Test custom API retriable codes (e.g., 404) using the API retry timeout (300 seconds)
    // When isApiRetriableCode=true, only API codes timeout is used
    assertTrue(timeoutManager.evaluateRetryTimeoutForResponse(404, 99000, true)); // 99s
    assertTrue(timeoutManager.evaluateRetryTimeoutForResponse(404, 99000, true)); // 198s total
    assertTrue(timeoutManager.evaluateRetryTimeoutForResponse(404, 99000, true)); // 297s total
    // This should exhaust the timeout (99+99+99+4 > 300 seconds)
    assertFalse(timeoutManager.evaluateRetryTimeoutForResponse(404, 4000, true));
  }

  @Test
  void testApiRetriableCodesIndependentTimeout() {
    // Test that API retriable codes have independent timeout from standard codes
    // Exhaust standard 503 timeout (120 seconds)
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_SERVICE_UNAVAILABLE, 60000, false));
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_SERVICE_UNAVAILABLE, 59000, false)); // 119 seconds total
    assertFalse(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_SERVICE_UNAVAILABLE, 2000, false)); // Would exceed (119+2 > 120)

    // API retriable codes should still work (300 second timeout)
    assertTrue(timeoutManager.evaluateRetryTimeoutForResponse(404, 99000, true));
    assertTrue(timeoutManager.evaluateRetryTimeoutForResponse(404, 99000, true));
  }
}
