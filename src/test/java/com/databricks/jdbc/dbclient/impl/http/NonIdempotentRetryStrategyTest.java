package com.databricks.jdbc.dbclient.impl.http;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import java.util.Optional;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.message.BasicHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class NonIdempotentRetryStrategyTest {

  private NonIdempotentRetryStrategy strategy;

  @Mock private CloseableHttpResponse mockResponse;
  @Mock private StatusLine mockStatusLine;
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
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_SERVICE_UNAVAILABLE);
    when(mockResponse.containsHeader("Retry-After")).thenReturn(true);
    when(mockResponse.getFirstHeader("Retry-After"))
        .thenReturn(new BasicHeader("Retry-After", "30"));
    when(mockConnectionContext.shouldRetryTemporarilyUnavailableError()).thenReturn(true);

    int retryDelay = strategy.retryRequestAfter(mockResponse, 0, mockConnectionContext).get();

    assertEquals(30000, retryDelay); // 30 seconds converted to milliseconds
  }

  @Test
  public void testRetryAfter429WithValidHeader() {
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_TOO_MANY_REQUESTS);
    when(mockResponse.containsHeader("Retry-After")).thenReturn(true);
    when(mockResponse.getFirstHeader("Retry-After"))
        .thenReturn(new BasicHeader("Retry-After", "15"));
    when(mockConnectionContext.shouldRetryRateLimitError()).thenReturn(true);

    int retryDelay = strategy.retryRequestAfter(mockResponse, 0, mockConnectionContext).get();

    assertEquals(15000, retryDelay); // 15 seconds converted to milliseconds
  }

  @Test
  public void testRetryAfter503WithoutHeader() {
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_SERVICE_UNAVAILABLE);
    when(mockResponse.containsHeader("Retry-After")).thenReturn(false);
    when(mockConnectionContext.shouldRetryTemporarilyUnavailableError()).thenReturn(true);

    Optional<Integer> retryDelay =
        strategy.retryRequestAfter(mockResponse, 0, mockConnectionContext);

    assertTrue(retryDelay.isEmpty()); // Should not retry if no header
  }

  @Test
  public void testRetryAfter429WithoutHeader() {
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_TOO_MANY_REQUESTS);
    when(mockResponse.containsHeader("Retry-After")).thenReturn(false);
    when(mockConnectionContext.shouldRetryRateLimitError()).thenReturn(true);

    Optional<Integer> retryDelay =
        strategy.retryRequestAfter(mockResponse, 0, mockConnectionContext);

    assertTrue(retryDelay.isEmpty()); // Should not retry if no header
  }

  @Test
  public void testRetryAfterWithInvalidHeader() {
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_SERVICE_UNAVAILABLE);
    when(mockResponse.containsHeader("Retry-After")).thenReturn(true);
    when(mockResponse.getFirstHeader("Retry-After"))
        .thenReturn(new BasicHeader("Retry-After", "invalid-number"));
    when(mockConnectionContext.shouldRetryTemporarilyUnavailableError()).thenReturn(true);

    Optional<Integer> retryDelay =
        strategy.retryRequestAfter(mockResponse, 0, mockConnectionContext);

    assertTrue(retryDelay.isEmpty()); // Should not retry if header is invalid
  }

  @Test
  public void testRetryAfterForOtherStatusCodes() {
    // Test that other status codes return -1 (should not retry)
    // No need to mock headers since they're ignored for non-503/429 codes
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_INTERNAL_SERVER_ERROR);

    Optional<Integer> retryDelay =
        strategy.retryRequestAfter(mockResponse, 0, mockConnectionContext);

    assertTrue(retryDelay.isEmpty()); // Should not retry for non-503/429 codes
  }

  @Test
  public void testRetryAfterIgnoresExecutionAttempt() {
    // Non-idempotent strategy should ignore execution attempt and only look at Retry-After header
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_SERVICE_UNAVAILABLE);
    when(mockResponse.containsHeader("Retry-After")).thenReturn(true);
    when(mockResponse.getFirstHeader("Retry-After"))
        .thenReturn(new BasicHeader("Retry-After", "45"));
    when(mockConnectionContext.shouldRetryTemporarilyUnavailableError()).thenReturn(true);

    int retryDelay0 = strategy.retryRequestAfter(mockResponse, 0, mockConnectionContext).get();
    int retryDelay9 = strategy.retryRequestAfter(mockResponse, 9, mockConnectionContext).get();

    assertEquals(45000, retryDelay0);
    assertEquals(45000, retryDelay9); // Should be same regardless of attempt number
  }

  @Test
  public void testRetryAfterWithLargeValue() {
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_TOO_MANY_REQUESTS);
    when(mockResponse.containsHeader("Retry-After")).thenReturn(true);
    when(mockResponse.getFirstHeader("Retry-After"))
        .thenReturn(new BasicHeader("Retry-After", "3600")); // 1 hour
    when(mockConnectionContext.shouldRetryRateLimitError()).thenReturn(true);

    int retryDelay = strategy.retryRequestAfter(mockResponse, 0, mockConnectionContext).get();

    assertEquals(3600000, retryDelay); // 1 hour converted to milliseconds
  }

  @Test
  public void testNullConnectionContextHandling() {
    // Test behavior when connection context is null
    assertFalse(strategy.isStatusCodeRetriable(HttpStatus.SC_SERVICE_UNAVAILABLE, null));
    assertFalse(strategy.isStatusCodeRetriable(HttpStatus.SC_TOO_MANY_REQUESTS, null));
    assertFalse(strategy.isStatusCodeRetriable(HttpStatus.SC_INTERNAL_SERVER_ERROR, null));
  }

  @Test
  public void testRetryAfterHeaderEdgeCases() {
    // Test with whitespace in header value
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_SERVICE_UNAVAILABLE);
    when(mockResponse.containsHeader("Retry-After")).thenReturn(true);
    when(mockResponse.getFirstHeader("Retry-After"))
        .thenReturn(new BasicHeader("Retry-After", " 25 "));
    when(mockConnectionContext.shouldRetryTemporarilyUnavailableError()).thenReturn(true);

    int retryDelay = strategy.retryRequestAfter(mockResponse, 0, mockConnectionContext).get();

    assertEquals(25000, retryDelay); // Should handle whitespace correctly
  }

  @Test
  public void testRetryAfterHeaderWithFloat() {
    // Test with float value in header (should fail to parse)
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_SERVICE_UNAVAILABLE);
    when(mockResponse.containsHeader("Retry-After")).thenReturn(true);
    when(mockResponse.getFirstHeader("Retry-After"))
        .thenReturn(new BasicHeader("Retry-After", "30.5"));
    when(mockConnectionContext.shouldRetryTemporarilyUnavailableError()).thenReturn(true);

    Optional<Integer> retryDelay =
        strategy.retryRequestAfter(mockResponse, 0, mockConnectionContext);

    assertTrue(retryDelay.isEmpty()); // Should not retry if header contains float
  }
}
