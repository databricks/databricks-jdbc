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
public class NonIdempotentRetryStrategyTest {

  private NonIdempotentRetryStrategy strategy;

  @Mock private IDatabricksConnectionContext mockConnectionContext;

  @BeforeEach
  public void setUp() {
    strategy = new NonIdempotentRetryStrategy();
  }

  @Test
  public void testRetriable503WhenEnabled() {
    when(mockConnectionContext.shouldRetryTemporarilyUnavailableError()).thenReturn(true);

    assertTrue(
        strategy.isStatusCodeRetriable(HttpStatus.SC_SERVICE_UNAVAILABLE, mockConnectionContext));
  }

  @Test
  public void testNonRetriable503WhenDisabled() {
    when(mockConnectionContext.shouldRetryTemporarilyUnavailableError()).thenReturn(false);

    assertFalse(
        strategy.isStatusCodeRetriable(HttpStatus.SC_SERVICE_UNAVAILABLE, mockConnectionContext));
  }

  @Test
  public void testRetriable429WhenEnabled() {
    when(mockConnectionContext.shouldRetryRateLimitError()).thenReturn(true);

    assertTrue(
        strategy.isStatusCodeRetriable(HttpStatus.SC_TOO_MANY_REQUESTS, mockConnectionContext));
  }

  @Test
  public void testNonRetriable429WhenDisabled() {
    when(mockConnectionContext.shouldRetryRateLimitError()).thenReturn(false);

    assertFalse(
        strategy.isStatusCodeRetriable(HttpStatus.SC_TOO_MANY_REQUESTS, mockConnectionContext));
  }

  @Test
  public void testNonRetriableOtherStatusCodes() {
    // All other status codes should not be retriable for non-idempotent requests
    // (connection context settings don't matter for these codes)
    assertFalse(strategy.isStatusCodeRetriable(HttpStatus.SC_OK, mockConnectionContext));
    assertFalse(strategy.isStatusCodeRetriable(HttpStatus.SC_BAD_REQUEST, mockConnectionContext));
    assertFalse(strategy.isStatusCodeRetriable(HttpStatus.SC_UNAUTHORIZED, mockConnectionContext));
    assertFalse(strategy.isStatusCodeRetriable(HttpStatus.SC_FORBIDDEN, mockConnectionContext));
    assertFalse(strategy.isStatusCodeRetriable(HttpStatus.SC_NOT_FOUND, mockConnectionContext));
    assertFalse(
        strategy.isStatusCodeRetriable(HttpStatus.SC_INTERNAL_SERVER_ERROR, mockConnectionContext));
    assertFalse(strategy.isStatusCodeRetriable(HttpStatus.SC_BAD_GATEWAY, mockConnectionContext));
    assertFalse(
        strategy.isStatusCodeRetriable(HttpStatus.SC_GATEWAY_TIMEOUT, mockConnectionContext));
  }

  @Test
  public void testRetryAfter503WithValidHeader() {
    when(mockConnectionContext.shouldRetryTemporarilyUnavailableError()).thenReturn(true);

    int statusCode = HttpStatus.SC_SERVICE_UNAVAILABLE;
    Optional<Integer> retryAfterHeader = Optional.of(30000); // 30 seconds in milliseconds
    int retryDelay =
        strategy.retryRequestAfter(statusCode, retryAfterHeader, 0, mockConnectionContext).get();

    assertEquals(30000, retryDelay); // 30 seconds converted to milliseconds
  }

  @Test
  public void testRetryAfter429WithValidHeader() {
    when(mockConnectionContext.shouldRetryRateLimitError()).thenReturn(true);

    int statusCode = HttpStatus.SC_TOO_MANY_REQUESTS;
    Optional<Integer> retryAfterHeader = Optional.of(15000); // 15 seconds in milliseconds
    int retryDelay =
        strategy.retryRequestAfter(statusCode, retryAfterHeader, 0, mockConnectionContext).get();

    assertEquals(15000, retryDelay); // 15 seconds converted to milliseconds
  }

  @Test
  public void testRetryAfter503WithoutHeader() {
    when(mockConnectionContext.shouldRetryTemporarilyUnavailableError()).thenReturn(true);

    int statusCode = HttpStatus.SC_SERVICE_UNAVAILABLE;
    Optional<Integer> retryAfterHeader = Optional.empty(); // No retry-after header
    Optional<Integer> retryDelay =
        strategy.retryRequestAfter(statusCode, retryAfterHeader, 0, mockConnectionContext);

    assertTrue(retryDelay.isEmpty()); // Should not retry if no header
  }

  @Test
  public void testRetryAfter429WithoutHeader() {
    when(mockConnectionContext.shouldRetryRateLimitError()).thenReturn(true);

    int statusCode = HttpStatus.SC_TOO_MANY_REQUESTS;
    Optional<Integer> retryAfterHeader = Optional.empty(); // No retry-after header
    Optional<Integer> retryDelay =
        strategy.retryRequestAfter(statusCode, retryAfterHeader, 0, mockConnectionContext);

    assertTrue(retryDelay.isEmpty()); // Should not retry if no header
  }

  @Test
  public void testRetryAfterWithInvalidHeader() {
    when(mockConnectionContext.shouldRetryTemporarilyUnavailableError()).thenReturn(true);

    int statusCode = HttpStatus.SC_SERVICE_UNAVAILABLE;
    Optional<Integer> retryAfterHeader = Optional.empty(); // Invalid header means no retry
    Optional<Integer> retryDelay =
        strategy.retryRequestAfter(statusCode, retryAfterHeader, 0, mockConnectionContext);

    assertTrue(retryDelay.isEmpty()); // Should not retry if header is invalid
  }

  @Test
  public void testRetryAfterForOtherStatusCodes() {
    // Test that other status codes return -1 (should not retry)
    // No need to mock headers since they're ignored for non-503/429 codes
    int statusCode = HttpStatus.SC_INTERNAL_SERVER_ERROR;
    Optional<Integer> retryAfterHeader = Optional.empty();
    Optional<Integer> retryDelay =
        strategy.retryRequestAfter(statusCode, retryAfterHeader, 0, mockConnectionContext);

    assertTrue(retryDelay.isEmpty()); // Should not retry for non-503/429 codes
  }

  @Test
  public void testRetryAfterIgnoresExecutionAttempt() {
    // Non-idempotent strategy should ignore execution attempt and only look at Retry-After header
    when(mockConnectionContext.shouldRetryTemporarilyUnavailableError()).thenReturn(true);

    int statusCode = HttpStatus.SC_SERVICE_UNAVAILABLE;
    Optional<Integer> retryAfterHeader = Optional.of(45000); // 45 seconds in milliseconds
    int retryDelay0 =
        strategy.retryRequestAfter(statusCode, retryAfterHeader, 0, mockConnectionContext).get();
    int retryDelay9 =
        strategy.retryRequestAfter(statusCode, retryAfterHeader, 9, mockConnectionContext).get();

    assertEquals(45000, retryDelay0);
    assertEquals(45000, retryDelay9); // Should be same regardless of attempt number
  }

  @Test
  public void testRetryAfterWithLargeValue() {
    when(mockConnectionContext.shouldRetryRateLimitError()).thenReturn(true);

    int statusCode = HttpStatus.SC_TOO_MANY_REQUESTS;
    Optional<Integer> retryAfterHeader = Optional.of(3600000); // 1 hour in milliseconds
    int retryDelay =
        strategy.retryRequestAfter(statusCode, retryAfterHeader, 0, mockConnectionContext).get();

    assertEquals(3600000, retryDelay); // 1 hour converted to milliseconds
  }

  @Test
  public void testRetryAfterHeaderEdgeCases() {
    // Test with whitespace in header value
    when(mockConnectionContext.shouldRetryTemporarilyUnavailableError()).thenReturn(true);

    int statusCode = HttpStatus.SC_SERVICE_UNAVAILABLE;
    Optional<Integer> retryAfterHeader = Optional.of(25000); // 25 seconds in milliseconds
    int retryDelay =
        strategy.retryRequestAfter(statusCode, retryAfterHeader, 0, mockConnectionContext).get();

    assertEquals(25000, retryDelay); // Should handle whitespace correctly
  }

  @Test
  public void testRetryAfterHeaderWithFloat() {
    // Test with float value in header (should fail to parse)
    when(mockConnectionContext.shouldRetryTemporarilyUnavailableError()).thenReturn(true);

    int statusCode = HttpStatus.SC_SERVICE_UNAVAILABLE;
    Optional<Integer> retryAfterHeader = Optional.empty(); // Invalid float header means no retry
    Optional<Integer> retryDelay =
        strategy.retryRequestAfter(statusCode, retryAfterHeader, 0, mockConnectionContext);

    assertTrue(retryDelay.isEmpty()); // Should not retry if header contains float
  }
}
