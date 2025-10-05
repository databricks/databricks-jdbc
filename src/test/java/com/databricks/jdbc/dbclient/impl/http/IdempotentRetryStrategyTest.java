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
public class IdempotentRetryStrategyTest {

  private IdempotentRetryStrategy strategy;

  @Mock private IDatabricksConnectionContext mockConnectionContext;
  @Mock private RetryTimeoutManager mockRetryTimeoutManager;

  @BeforeEach
  public void setUp() {
    strategy = new IdempotentRetryStrategy();
  }

  // Test 1: Retriable response WITH Retry-After header
  @Test
  public void testRetriableResponseWithRetryAfterHeader() {
    when(mockConnectionContext.shouldRetryTemporarilyUnavailableError()).thenReturn(true);
    when(mockRetryTimeoutManager.evaluateRetryTimeoutForResponse(anyInt(), anyInt()))
        .thenReturn(true);

    Optional<Integer> retryAfter = Optional.of(5000);
    Optional<Integer> result =
        strategy.shouldRetryAfter(
            HttpStatus.SC_SERVICE_UNAVAILABLE,
            retryAfter,
            0,
            mockConnectionContext,
            mockRetryTimeoutManager);

    assertTrue(result.isPresent(), "Should retry retriable response with Retry-After header");
    assertEquals(5000, result.get(), "Should use Retry-After header value");
  }

  // Test 2: Retriable response WITHOUT Retry-After header
  @Test
  public void testRetriableResponseWithoutRetryAfterHeader() {
    when(mockRetryTimeoutManager.evaluateRetryTimeoutForResponse(anyInt(), anyInt()))
        .thenReturn(true);

    Optional<Integer> result =
        strategy.shouldRetryAfter(
            HttpStatus.SC_INTERNAL_SERVER_ERROR,
            Optional.empty(),
            0,
            mockConnectionContext,
            mockRetryTimeoutManager);

    assertTrue(result.isPresent(), "Should retry retriable response without Retry-After header");
    assertTrue(
        result.get() > 0, "Should use exponential backoff when Retry-After header is not present");
  }

  // Test 3: Non-retriable response WITH Retry-After header
  @Test
  public void testNonRetriableResponseWithRetryAfterHeader() {
    Optional<Integer> result =
        strategy.shouldRetryAfter(
            HttpStatus.SC_BAD_REQUEST,
            Optional.of(5000),
            0,
            mockConnectionContext,
            mockRetryTimeoutManager);

    assertFalse(
        result.isPresent(), "Should not retry non-retriable response even with Retry-After header");
  }

  // Test 4: Non-retriable response WITHOUT Retry-After header
  @Test
  public void testNonRetriableResponseWithoutRetryAfterHeader() {
    Optional<Integer> result =
        strategy.shouldRetryAfter(
            HttpStatus.SC_UNAUTHORIZED,
            Optional.empty(),
            0,
            mockConnectionContext,
            mockRetryTimeoutManager);

    assertFalse(
        result.isPresent(), "Should not retry non-retriable response without Retry-After header");
  }

  // Test 5: Retriable exception (uses exponential backoff)
  @Test
  public void testRetriableException() {
    when(mockRetryTimeoutManager.evaluateRetryTimeoutForException(anyInt())).thenReturn(true);

    ConnectException exception = new ConnectException("Connection refused");
    Optional<Integer> result = strategy.shouldRetryAfter(exception, 0, mockRetryTimeoutManager);

    assertTrue(result.isPresent(), "Should retry retriable exception");
    assertTrue(result.get() > 0, "Should use exponential backoff for exceptions");
  }

  // Test 6: Non-retriable exception
  @Test
  public void testNonRetriableException() {
    IllegalArgumentException exception = new IllegalArgumentException("Invalid argument");
    Optional<Integer> result = strategy.shouldRetryAfter(exception, 0, mockRetryTimeoutManager);

    assertFalse(result.isPresent(), "Should not retry non-retriable exception");
  }

  // Test 7: Retry stops when timeout is reached for response
  @Test
  public void testRetryStopsWhenTimeoutReachedForResponse() {
    when(mockConnectionContext.shouldRetryTemporarilyUnavailableError()).thenReturn(true);
    when(mockRetryTimeoutManager.evaluateRetryTimeoutForResponse(anyInt(), anyInt()))
        .thenReturn(false);

    Optional<Integer> result =
        strategy.shouldRetryAfter(
            HttpStatus.SC_SERVICE_UNAVAILABLE,
            Optional.of(5000),
            0,
            mockConnectionContext,
            mockRetryTimeoutManager);

    assertFalse(result.isPresent(), "Should not retry when timeout is reached for response");
  }

  // Test 8: Retry stops when timeout is reached for exception
  @Test
  public void testRetryStopsWhenTimeoutReachedForException() {
    when(mockRetryTimeoutManager.evaluateRetryTimeoutForException(anyInt())).thenReturn(false);

    ConnectException exception = new ConnectException("Connection refused");
    Optional<Integer> result = strategy.shouldRetryAfter(exception, 0, mockRetryTimeoutManager);

    assertFalse(result.isPresent(), "Should not retry when timeout is reached for exception");
  }
}
