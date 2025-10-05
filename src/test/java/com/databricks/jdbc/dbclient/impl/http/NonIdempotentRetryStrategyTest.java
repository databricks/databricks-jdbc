package com.databricks.jdbc.dbclient.impl.http;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import java.net.ConnectException;
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
  @Mock private RetryTimeoutManager mockRetryTimeoutManager;

  @BeforeEach
  public void setUp() {
    strategy = new NonIdempotentRetryStrategy();
  }

  // Test 1: Retriable response (503/429) WITH Retry-After header
  @Test
  public void testRetriableResponseWithRetryAfterHeader() {
    when(mockConnectionContext.shouldRetryTemporarilyUnavailableError()).thenReturn(true);
    when(mockRetryTimeoutManager.evaluateRetryTimeoutForResponse(anyInt(), anyInt()))
        .thenReturn(true);

    Optional<Integer> retryAfter = Optional.of(30000);
    Optional<Integer> result =
        strategy.shouldRetryAfter(
            HttpStatus.SC_SERVICE_UNAVAILABLE,
            retryAfter,
            0,
            mockConnectionContext,
            mockRetryTimeoutManager);

    assertTrue(
        result.isPresent(),
        "Should retry retriable response (503) with Retry-After header for non-idempotent request");
    assertEquals(30000, result.get(), "Should use exact Retry-After header value");
  }

  // Test 2: Retriable response (503/429) WITHOUT Retry-After header
  @Test
  public void testRetriableResponseWithoutRetryAfterHeader() {
    when(mockConnectionContext.shouldRetryTemporarilyUnavailableError()).thenReturn(true);

    Optional<Integer> result =
        strategy.shouldRetryAfter(
            HttpStatus.SC_SERVICE_UNAVAILABLE,
            Optional.empty(),
            0,
            mockConnectionContext,
            mockRetryTimeoutManager);

    assertFalse(
        result.isPresent(),
        "Should NOT retry retriable response (503) without Retry-After header for non-idempotent request");
  }

  // Test 3: Non-retriable response (500, 502, etc.) WITH Retry-After header
  @Test
  public void testNonRetriableResponseWithRetryAfterHeader() {
    Optional<Integer> result =
        strategy.shouldRetryAfter(
            HttpStatus.SC_INTERNAL_SERVER_ERROR,
            Optional.of(5000),
            0,
            mockConnectionContext,
            mockRetryTimeoutManager);

    assertFalse(
        result.isPresent(),
        "Should not retry non-retriable response (500) even with Retry-After header for non-idempotent request");
  }

  // Test 4: Non-retriable response WITHOUT Retry-After header
  @Test
  public void testNonRetriableResponseWithoutRetryAfterHeader() {
    Optional<Integer> result =
        strategy.shouldRetryAfter(
            HttpStatus.SC_BAD_REQUEST,
            Optional.empty(),
            0,
            mockConnectionContext,
            mockRetryTimeoutManager);

    assertFalse(
        result.isPresent(),
        "Should not retry non-retriable response (400) without Retry-After header for non-idempotent request");
  }

  // Test 5: Retriable exception (uses exponential backoff)
  @Test
  public void testRetriableException() {
    when(mockRetryTimeoutManager.evaluateRetryTimeoutForException(anyInt())).thenReturn(true);

    ConnectException exception = new ConnectException("Connection refused");
    Optional<Integer> result = strategy.shouldRetryAfter(exception, 0, mockRetryTimeoutManager);

    assertTrue(
        result.isPresent(),
        "Should retry retriable exception for non-idempotent request (network errors)");
    assertTrue(result.get() > 0, "Should use exponential backoff for exceptions");
  }

  // Test 6: Non-retriable exception
  @Test
  public void testNonRetriableException() {
    IllegalArgumentException exception = new IllegalArgumentException("Invalid argument");
    Optional<Integer> result = strategy.shouldRetryAfter(exception, 0, mockRetryTimeoutManager);

    assertFalse(
        result.isPresent(), "Should not retry non-retriable exception for non-idempotent request");
  }

  // Test 7: Retry stops when timeout is reached for response
  @Test
  public void testRetryStopsWhenTimeoutReachedForResponse() {
    when(mockConnectionContext.shouldRetryRateLimitError()).thenReturn(true);
    when(mockRetryTimeoutManager.evaluateRetryTimeoutForResponse(anyInt(), anyInt()))
        .thenReturn(false);

    Optional<Integer> result =
        strategy.shouldRetryAfter(
            HttpStatus.SC_TOO_MANY_REQUESTS,
            Optional.of(5000),
            0,
            mockConnectionContext,
            mockRetryTimeoutManager);

    assertFalse(
        result.isPresent(),
        "Should not retry when timeout is reached for response in non-idempotent request");
  }

  // Test 8: Retry stops when timeout is reached for exception
  @Test
  public void testRetryStopsWhenTimeoutReachedForException() {
    when(mockRetryTimeoutManager.evaluateRetryTimeoutForException(anyInt())).thenReturn(false);

    ConnectException exception = new ConnectException("Connection refused");
    Optional<Integer> result = strategy.shouldRetryAfter(exception, 0, mockRetryTimeoutManager);

    assertFalse(
        result.isPresent(),
        "Should not retry when timeout is reached for exception in non-idempotent request");
  }
}
