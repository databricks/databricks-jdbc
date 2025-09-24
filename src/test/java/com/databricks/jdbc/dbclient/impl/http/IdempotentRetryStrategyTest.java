package com.databricks.jdbc.dbclient.impl.http;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import java.util.Optional;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class IdempotentRetryStrategyTest {

  private IdempotentRetryStrategy strategy;

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
    // Test that each attempt increases the delay exponentially (0-indexed attempts)
    int statusCode = HttpStatus.SC_INTERNAL_SERVER_ERROR;
    Optional<Integer> retryAfterHeader = Optional.empty(); // No retry-after header

    int delay0 =
        strategy.retryRequestAfter(statusCode, retryAfterHeader, 0, mockConnectionContext).get();
    int delay1 =
        strategy.retryRequestAfter(statusCode, retryAfterHeader, 1, mockConnectionContext).get();
    int delay2 =
        strategy.retryRequestAfter(statusCode, retryAfterHeader, 2, mockConnectionContext).get();
    int delay3 =
        strategy.retryRequestAfter(statusCode, retryAfterHeader, 3, mockConnectionContext).get();
    int delay4 =
        strategy.retryRequestAfter(statusCode, retryAfterHeader, 4, mockConnectionContext).get();

    // Verify exponential backoff with jitter ranges using the same calculation as production code
    assertDelayInRange(delay0, 0, "First delay (attempt 0)");
    assertDelayInRange(delay1, 1, "Second delay (attempt 1)");
    assertDelayInRange(delay2, 2, "Third delay (attempt 2)");
    assertDelayInRange(delay3, 3, "Fourth delay (attempt 3)");
    assertDelayInRange(delay4, 4, "Fifth delay (attempt 4)");
  }

  @Test
  public void testUsesRetryAfterHeaderWhenPresent() {
    int statusCode = HttpStatus.SC_INTERNAL_SERVER_ERROR;
    Optional<Integer> retryAfterHeaderValue = Optional.of(30000); // 30 seconds in milliseconds
    int delay =
        strategy
            .retryRequestAfter(statusCode, retryAfterHeaderValue, 0, mockConnectionContext)
            .get();

    // Should use the retry-after header value (30000 milliseconds)
    assertEquals(30000, delay);
  }

  @Test
  public void testFallsBackToExponentialBackoffWhenNoRetryAfterHeader() {
    int statusCode = HttpStatus.SC_INTERNAL_SERVER_ERROR;
    Optional<Integer> retryAfterHeader = Optional.empty(); // No retry-after header
    int delay =
        strategy.retryRequestAfter(statusCode, retryAfterHeader, 0, mockConnectionContext).get();

    // Should use exponential backoff with jitter
    assertDelayInRange(delay, 0, "Delay when no Retry-After header is present");
  }

  @Test
  public void testHighAttemptNumber() {
    // Test with very high attempt numbers to ensure no overflow/unexpected behavior (0-indexed)
    int statusCode = HttpStatus.SC_INTERNAL_SERVER_ERROR;
    Optional<Integer> retryAfterHeader = Optional.empty(); // No retry-after header
    int delay9 =
        strategy.retryRequestAfter(statusCode, retryAfterHeader, 9, mockConnectionContext).get();
    int delay19 =
        strategy.retryRequestAfter(statusCode, retryAfterHeader, 19, mockConnectionContext).get();

    // Should be capped at maximum with jitter - use helper to verify
    assertDelayInRange(delay9, 9, "Delay for attempt 9 (should be capped)");
    assertDelayInRange(delay19, 19, "Delay for attempt 19 (should be capped)");
  }

  /**
   * Helper method to verify delay is within expected exponential backoff range with jitter. Uses
   * the same constants and formula as the production RetryUtils.calculateExponentialBackoff.
   */
  private void assertDelayInRange(int actualDelay, int attempt, String delayDescription) {
    // Calculate expected base delay using same formula as RetryUtils
    int baseDelay = calculateExpectedBaseDelay(attempt);
    int minDelay = baseDelay; // No jitter applied = base value
    int maxDelay = (int) (baseDelay * 1.2); // 20% jitter as per RetryUtils.addJitter

    assertTrue(
        actualDelay >= minDelay && actualDelay <= maxDelay,
        String.format(
            "%s should be %d-%dms (base: %dms + 0-20%% jitter), but was %dms",
            delayDescription, minDelay, maxDelay, baseDelay, actualDelay));
  }

  /**
   * Calculates expected base delay using same constants as RetryUtils.calculateExponentialBackoff.
   */
  private int calculateExpectedBaseDelay(int attempt) {
    final int MIN_BACKOFF_INTERVAL = 1000; // From RetryUtils
    final int DEFAULT_BACKOFF_FACTOR = 2; // From RetryUtils
    final int MAX_RETRY_INTERVAL = 10000; // From RetryUtils

    return (int)
        Math.min(
            MIN_BACKOFF_INTERVAL * Math.pow(DEFAULT_BACKOFF_FACTOR, attempt), MAX_RETRY_INTERVAL);
  }
}
