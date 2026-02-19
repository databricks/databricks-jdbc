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
    timeoutManager = new RetryTimeoutManager(mockContext);
  }

  @Test
  void testServiceUnavailableTimeoutExhausted() {
    // Make multiple retries that exhaust the service unavailable timeout (120 seconds)
    // Each retry delays for 30 seconds
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(HttpStatus.SC_SERVICE_UNAVAILABLE, 30000));
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(HttpStatus.SC_SERVICE_UNAVAILABLE, 30000));
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(HttpStatus.SC_SERVICE_UNAVAILABLE, 30000));
    // This should exhaust the timeout (30+30+30+31 > 120 seconds)
    assertFalse(
        timeoutManager.evaluateRetryTimeoutForResponse(HttpStatus.SC_SERVICE_UNAVAILABLE, 31000));
  }

  @Test
  void testRateLimitTimeoutExhausted() {
    // Make multiple retries that exhaust the rate limit timeout (300 seconds)
    // Each retry delays for 80 seconds
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(HttpStatus.SC_TOO_MANY_REQUESTS, 80000));
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(HttpStatus.SC_TOO_MANY_REQUESTS, 80000));
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(HttpStatus.SC_TOO_MANY_REQUESTS, 80000));
    // This should exhaust the timeout (80+80+80+61 > 300 seconds)
    assertFalse(
        timeoutManager.evaluateRetryTimeoutForResponse(HttpStatus.SC_TOO_MANY_REQUESTS, 61000));
  }

  @Test
  void testOtherErrorCodesTimeout() {
    // Test other error codes (e.g., 500) using the default 10-second timeout
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(HttpStatus.SC_INTERNAL_SERVER_ERROR, 3000));
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(HttpStatus.SC_INTERNAL_SERVER_ERROR, 3000));
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(HttpStatus.SC_INTERNAL_SERVER_ERROR, 3000));
    // This should exhaust the timeout (3+3+3+2 > 10 seconds)
    assertFalse(
        timeoutManager.evaluateRetryTimeoutForResponse(HttpStatus.SC_INTERNAL_SERVER_ERROR, 2000));
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
        timeoutManager.evaluateRetryTimeoutForResponse(HttpStatus.SC_SERVICE_UNAVAILABLE, 20000));
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(HttpStatus.SC_TOO_MANY_REQUESTS, 50000));
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(HttpStatus.SC_INTERNAL_SERVER_ERROR, 2000));
    assertTrue(timeoutManager.evaluateRetryTimeoutForException(2000));

    // All timeouts should still have capacity
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(HttpStatus.SC_SERVICE_UNAVAILABLE, 10000));
  }

  @Test
  void testImmediateTimeoutExhaustion() {
    // A single large delay can exhaust the timeout
    assertFalse(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_SERVICE_UNAVAILABLE, 121000)); // > 120 seconds
  }
}
