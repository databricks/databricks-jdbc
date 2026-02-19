package com.databricks.jdbc.dbclient.impl.http;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RetryTimeoutManagerTest {

  private static final int TEMP_UNAVAILABLE_TIMEOUT_SECONDS = 120;
  private static final int RATE_LIMIT_TIMEOUT_SECONDS = 300;
  private static final int API_RETRIABLE_TIMEOUT_SECONDS = 300;

  private IDatabricksConnectionContext mockContext;
  private RetryTimeoutManager timeoutManager;

  @BeforeEach
  void setUp() {
    mockContext = mock(IDatabricksConnectionContext.class);
    when(mockContext.getTemporarilyUnavailableRetryTimeout())
        .thenReturn(TEMP_UNAVAILABLE_TIMEOUT_SECONDS);
    when(mockContext.getRateLimitRetryTimeout()).thenReturn(RATE_LIMIT_TIMEOUT_SECONDS);
    when(mockContext.getApiRetryTimeout()).thenReturn(API_RETRIABLE_TIMEOUT_SECONDS);
    timeoutManager = new RetryTimeoutManager(mockContext);
  }

  @Test
  void testServiceUnavailableTimeoutExhausted() {
    // Make multiple retries that exhaust the service unavailable timeout
    int delaySeconds = TEMP_UNAVAILABLE_TIMEOUT_SECONDS / 4;
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_SERVICE_UNAVAILABLE, delaySeconds * 1000, false));
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_SERVICE_UNAVAILABLE, delaySeconds * 1000, false));
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_SERVICE_UNAVAILABLE, delaySeconds * 1000, false));
    // This should exhaust the timeout
    assertFalse(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_SERVICE_UNAVAILABLE, (delaySeconds + 1) * 1000, false));
  }

  @Test
  void testRateLimitTimeoutExhausted() {
    // Make multiple retries that exhaust the rate limit timeout
    int delaySeconds = RATE_LIMIT_TIMEOUT_SECONDS / 4;
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_TOO_MANY_REQUESTS, delaySeconds * 1000, false));
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_TOO_MANY_REQUESTS, delaySeconds * 1000, false));
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_TOO_MANY_REQUESTS, delaySeconds * 1000, false));
    // This should exhaust the timeout
    assertFalse(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_TOO_MANY_REQUESTS, (delaySeconds + 1) * 1000, false));
  }

  @Test
  void testOtherErrorCodesTimeout() {
    // Test other error codes (e.g., 500) using the default 120-second timeout
    int otherErrorsTimeout = 120; // RetryUtils.REQUEST_TIMEOUT_SECONDS
    int delaySeconds = otherErrorsTimeout / 4;
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_INTERNAL_SERVER_ERROR, delaySeconds * 1000, false));
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_INTERNAL_SERVER_ERROR, delaySeconds * 1000, false));
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_INTERNAL_SERVER_ERROR, delaySeconds * 1000, false));
    // This should exhaust the timeout
    assertFalse(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_INTERNAL_SERVER_ERROR, (delaySeconds + 1) * 1000, false));
  }

  @Test
  void testExceptionTimeout() {
    // Test exception timeout (default 120 seconds)
    int exceptionTimeout = 120; // RetryUtils.REQUEST_EXCEPTION_TIMEOUT_SECONDS
    int delaySeconds = exceptionTimeout / 4;
    assertTrue(timeoutManager.evaluateRetryTimeoutForException(delaySeconds * 1000));
    assertTrue(timeoutManager.evaluateRetryTimeoutForException(delaySeconds * 1000));
    assertTrue(timeoutManager.evaluateRetryTimeoutForException(delaySeconds * 1000));
    // This should exhaust the timeout
    assertFalse(timeoutManager.evaluateRetryTimeoutForException((delaySeconds + 1) * 1000));
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
            HttpStatus.SC_SERVICE_UNAVAILABLE,
            (TEMP_UNAVAILABLE_TIMEOUT_SECONDS + 1) * 1000,
            false));
  }

  @Test
  void testApiRetriableCodesTimeout() {
    // Test custom API retriable codes (e.g., 404) using the API retry timeout
    // When isApiRetriableCode=true, only API codes timeout is used
    int delaySeconds = API_RETRIABLE_TIMEOUT_SECONDS / 4;
    assertTrue(timeoutManager.evaluateRetryTimeoutForResponse(404, delaySeconds * 1000, true));
    assertTrue(timeoutManager.evaluateRetryTimeoutForResponse(404, delaySeconds * 1000, true));
    assertTrue(timeoutManager.evaluateRetryTimeoutForResponse(404, delaySeconds * 1000, true));
    // This should exhaust the timeout
    assertFalse(
        timeoutManager.evaluateRetryTimeoutForResponse(404, (delaySeconds + 1) * 1000, true));
  }

  @Test
  void testApiRetriableCodesIndependentTimeout() {
    // Test that API retriable codes have independent timeout from standard codes
    // Exhaust standard 503 timeout
    int delaySeconds = TEMP_UNAVAILABLE_TIMEOUT_SECONDS / 2;
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_SERVICE_UNAVAILABLE, delaySeconds * 1000, false));
    assertTrue(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_SERVICE_UNAVAILABLE, (delaySeconds - 1) * 1000, false));
    assertFalse(
        timeoutManager.evaluateRetryTimeoutForResponse(
            HttpStatus.SC_SERVICE_UNAVAILABLE, 2000, false));

    // API retriable codes should still work
    int apiDelaySeconds = API_RETRIABLE_TIMEOUT_SECONDS / 3;
    assertTrue(timeoutManager.evaluateRetryTimeoutForResponse(404, apiDelaySeconds * 1000, true));
    assertTrue(timeoutManager.evaluateRetryTimeoutForResponse(404, apiDelaySeconds * 1000, true));
  }
}
