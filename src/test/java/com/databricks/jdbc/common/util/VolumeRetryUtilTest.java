package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class VolumeRetryUtilTest {

  @Mock private IDatabricksConnectionContext mockConnectionContext;

  @Test
  public void testIsRetryableHttpCode_WithConnectionContext() {
    // Setup mock connection context
    when(mockConnectionContext.getUCIngestionRetriableHttpCodes())
        .thenReturn(java.util.Arrays.asList(408, 429, 500, 502, 503, 504));

    // Test retryable codes
    assertTrue(VolumeRetryUtil.isRetryableHttpCode(408, mockConnectionContext));
    assertTrue(VolumeRetryUtil.isRetryableHttpCode(429, mockConnectionContext));
    assertTrue(VolumeRetryUtil.isRetryableHttpCode(500, mockConnectionContext));
    assertTrue(VolumeRetryUtil.isRetryableHttpCode(502, mockConnectionContext));
    assertTrue(VolumeRetryUtil.isRetryableHttpCode(503, mockConnectionContext));
    assertTrue(VolumeRetryUtil.isRetryableHttpCode(504, mockConnectionContext));

    // Test non-retryable codes
    assertFalse(VolumeRetryUtil.isRetryableHttpCode(200, mockConnectionContext));
    assertFalse(VolumeRetryUtil.isRetryableHttpCode(400, mockConnectionContext));
    assertFalse(VolumeRetryUtil.isRetryableHttpCode(401, mockConnectionContext));
    assertFalse(VolumeRetryUtil.isRetryableHttpCode(403, mockConnectionContext));
    assertFalse(VolumeRetryUtil.isRetryableHttpCode(404, mockConnectionContext));
    assertFalse(VolumeRetryUtil.isRetryableHttpCode(499, mockConnectionContext));
    assertFalse(VolumeRetryUtil.isRetryableHttpCode(501, mockConnectionContext));
  }

  @Test
  public void testIsRetryableHttpCode_CustomRetryableCodes() {
    // Test with custom retryable codes
    when(mockConnectionContext.getUCIngestionRetriableHttpCodes())
        .thenReturn(java.util.Arrays.asList(503, 504, 507)); // Custom list

    assertTrue(VolumeRetryUtil.isRetryableHttpCode(503, mockConnectionContext));
    assertTrue(VolumeRetryUtil.isRetryableHttpCode(504, mockConnectionContext));
    assertTrue(VolumeRetryUtil.isRetryableHttpCode(507, mockConnectionContext));

    // Codes not in custom list should not be retryable
    assertFalse(VolumeRetryUtil.isRetryableHttpCode(408, mockConnectionContext));
    assertFalse(VolumeRetryUtil.isRetryableHttpCode(429, mockConnectionContext));
    assertFalse(VolumeRetryUtil.isRetryableHttpCode(500, mockConnectionContext));
    assertFalse(VolumeRetryUtil.isRetryableHttpCode(502, mockConnectionContext));
  }

  @Test
  public void testShouldRetry_WithConnectionContext() {
    when(mockConnectionContext.getUCIngestionRetryTimeoutSeconds()).thenReturn(60);

    long startTime = System.currentTimeMillis();

    // Test attempt 1 - should retry
    assertTrue(VolumeRetryUtil.shouldRetry(1, startTime, mockConnectionContext));

    // Test attempt 4 - should not retry
    assertFalse(VolumeRetryUtil.shouldRetry(4, startTime, mockConnectionContext));
  }

  @Test
  public void testShouldRetry_TimeoutExceeded() {
    when(mockConnectionContext.getUCIngestionRetryTimeoutSeconds()).thenReturn(5); // 5 seconds

    // Start time 10 seconds ago
    long startTime = System.currentTimeMillis() - 10000;

    // Should not retry because timeout is exceeded
    assertFalse(VolumeRetryUtil.shouldRetry(1, startTime, mockConnectionContext));
    assertFalse(VolumeRetryUtil.shouldRetry(2, startTime, mockConnectionContext));
  }

  @Test
  public void testShouldRetry_TimeoutExceeded_WithoutConnectionContext() {
    // Start time beyond default timeout (900 seconds)
    long startTime = System.currentTimeMillis() - 1000000; // Way beyond timeout

    // Should not retry because timeout is exceeded
    assertFalse(VolumeRetryUtil.shouldRetry(1, startTime, null));
    assertFalse(VolumeRetryUtil.shouldRetry(2, startTime, null));
  }

  @Test
  public void testIntegration_RetryScenario() {
    // Integration test simulating a real retry scenario
    when(mockConnectionContext.getUCIngestionRetriableHttpCodes())
        .thenReturn(java.util.Arrays.asList(500, 502, 503, 504));
    when(mockConnectionContext.getUCIngestionRetryTimeoutSeconds()).thenReturn(60);

    long startTime = System.currentTimeMillis();

    // First attempt fails with 500 - should retry
    assertTrue(VolumeRetryUtil.isRetryableHttpCode(500, mockConnectionContext));
    assertTrue(VolumeRetryUtil.shouldRetry(1, startTime, mockConnectionContext));

    // Second attempt fails with 503 - should retry
    assertTrue(VolumeRetryUtil.isRetryableHttpCode(503, mockConnectionContext));
    assertTrue(VolumeRetryUtil.shouldRetry(2, startTime, mockConnectionContext));

    // Third attempt fails with 404 - should not retry (not retryable code)
    assertFalse(VolumeRetryUtil.isRetryableHttpCode(404, mockConnectionContext));
    // Even though attempt count allows retry, 404 is not retryable
    assertFalse(VolumeRetryUtil.shouldRetry(3, startTime, mockConnectionContext));
  }

  @Test
  public void testIntegration_TimeoutScenario() {
    when(mockConnectionContext.getUCIngestionRetriableHttpCodes())
        .thenReturn(java.util.Arrays.asList(500, 502, 503, 504));
    when(mockConnectionContext.getUCIngestionRetryTimeoutSeconds()).thenReturn(2); // 2 seconds

    // Start time 3 seconds ago (exceeds timeout)
    long startTime = System.currentTimeMillis() - 3000;

    // Code is retryable but timeout is exceeded
    assertTrue(VolumeRetryUtil.isRetryableHttpCode(500, mockConnectionContext));
    assertFalse(VolumeRetryUtil.shouldRetry(1, startTime, mockConnectionContext));
  }
}
