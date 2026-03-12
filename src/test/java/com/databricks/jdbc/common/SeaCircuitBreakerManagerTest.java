package com.databricks.jdbc.common;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SeaCircuitBreakerManagerTest {

  @BeforeEach
  void setUp() {
    // Reset circuit breaker state before each test
    SeaCircuitBreakerManager.reset();
  }

  @AfterEach
  void tearDown() {
    // Clean up after each test
    SeaCircuitBreakerManager.reset();
  }

  @Test
  void testCircuitInitiallyClosed() {
    // Circuit should be closed initially (no failures recorded)
    assertFalse(SeaCircuitBreakerManager.isCircuitOpen());
    assertEquals(0, SeaCircuitBreakerManager.getTimeRemainingMs());
    assertEquals("closed", SeaCircuitBreakerManager.getTimeRemainingFormatted());
  }

  @Test
  void testCircuitOpensAfter429Failure() {
    // Record a 429 failure
    SeaCircuitBreakerManager.record429Failure();

    // Circuit should be open immediately after recording failure
    assertTrue(SeaCircuitBreakerManager.isCircuitOpen());
    assertTrue(SeaCircuitBreakerManager.getTimeRemainingMs() > 0);
    assertNotEquals("closed", SeaCircuitBreakerManager.getTimeRemainingFormatted());
  }

  @Test
  void testCircuitStaysOpenFor24Hours() throws Exception {
    // Record a 429 failure
    SeaCircuitBreakerManager.record429Failure();

    // Verify circuit is open
    assertTrue(SeaCircuitBreakerManager.isCircuitOpen());

    // Simulate time passage using reflection
    Field timestampField =
        SeaCircuitBreakerManager.class.getDeclaredField("last429FailureTimestamp");
    timestampField.setAccessible(true);

    // Set timestamp to 23 hours 59 minutes ago
    long almostExpired = System.currentTimeMillis() - (24 * 60 * 60 * 1000 - 60 * 1000);
    timestampField.set(null, almostExpired);

    // Circuit should still be open
    assertTrue(SeaCircuitBreakerManager.isCircuitOpen());
    assertTrue(SeaCircuitBreakerManager.getTimeRemainingMs() > 0);
    assertTrue(
        SeaCircuitBreakerManager.getTimeRemainingMs() < 2 * 60 * 1000); // Less than 2 minutes
  }

  @Test
  void testCircuitClosesAfter24Hours() throws Exception {
    // Record a 429 failure
    SeaCircuitBreakerManager.record429Failure();

    // Verify circuit is open
    assertTrue(SeaCircuitBreakerManager.isCircuitOpen());

    // Simulate time passage using reflection - set to 24 hours + 1 second ago
    Field timestampField =
        SeaCircuitBreakerManager.class.getDeclaredField("last429FailureTimestamp");
    timestampField.setAccessible(true);
    long expired = System.currentTimeMillis() - (24 * 60 * 60 * 1000 + 1000);
    timestampField.set(null, expired);

    // Circuit should be closed now
    assertFalse(SeaCircuitBreakerManager.isCircuitOpen());
    assertEquals(0, SeaCircuitBreakerManager.getTimeRemainingMs());
    assertEquals("closed", SeaCircuitBreakerManager.getTimeRemainingFormatted());
  }

  @Test
  void testMultiple429FailuresUpdateTimestamp() throws Exception {
    // Record first failure
    SeaCircuitBreakerManager.record429Failure();

    // Get timestamp field
    Field timestampField =
        SeaCircuitBreakerManager.class.getDeclaredField("last429FailureTimestamp");
    timestampField.setAccessible(true);
    long firstTimestamp = timestampField.getLong(null);

    // Wait a bit
    Thread.sleep(100);

    // Record second failure
    SeaCircuitBreakerManager.record429Failure();
    long secondTimestamp = timestampField.getLong(null);

    // Second timestamp should be later than first
    assertTrue(secondTimestamp > firstTimestamp);

    // Circuit should still be open
    assertTrue(SeaCircuitBreakerManager.isCircuitOpen());
  }

  @Test
  void testGetTimeRemainingFormatted() throws Exception {
    // Record a 429 failure
    SeaCircuitBreakerManager.record429Failure();

    // Set timestamp to exactly 5 hours 30 minutes ago
    Field timestampField =
        SeaCircuitBreakerManager.class.getDeclaredField("last429FailureTimestamp");
    timestampField.setAccessible(true);
    long fiveHoursThirtyMinutesAgo =
        System.currentTimeMillis() - (5 * 60 * 60 * 1000 + 30 * 60 * 1000);
    timestampField.set(null, fiveHoursThirtyMinutesAgo);

    // Should have approximately 18 hours 30 minutes remaining
    String formatted = SeaCircuitBreakerManager.getTimeRemainingFormatted();
    assertTrue(formatted.contains("18 hours"));
    assertTrue(formatted.contains("29 minutes") || formatted.contains("30 minutes"));
  }

  @Test
  void testResetClearsState() throws Exception {
    // Record a 429 failure
    SeaCircuitBreakerManager.record429Failure();

    // Verify circuit is open
    assertTrue(SeaCircuitBreakerManager.isCircuitOpen());

    // Reset
    SeaCircuitBreakerManager.reset();

    // Circuit should be closed
    assertFalse(SeaCircuitBreakerManager.isCircuitOpen());
    assertEquals(0, SeaCircuitBreakerManager.getTimeRemainingMs());
    assertEquals("closed", SeaCircuitBreakerManager.getTimeRemainingFormatted());

    // Verify timestamp is reset to -1
    Field timestampField =
        SeaCircuitBreakerManager.class.getDeclaredField("last429FailureTimestamp");
    timestampField.setAccessible(true);
    assertEquals(-1L, timestampField.getLong(null));
  }

  @Test
  void testConcurrentAccess() throws InterruptedException {
    // Test thread safety with multiple threads accessing circuit breaker
    int threadCount = 10;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch endLatch = new CountDownLatch(threadCount);
    AtomicInteger openCount = new AtomicInteger(0);

    // Record a failure first
    SeaCircuitBreakerManager.record429Failure();

    // Start multiple threads checking circuit state
    for (int i = 0; i < threadCount; i++) {
      executor.submit(
          () -> {
            try {
              startLatch.await(); // Wait for all threads to be ready
              // Check circuit state multiple times
              for (int j = 0; j < 100; j++) {
                if (SeaCircuitBreakerManager.isCircuitOpen()) {
                  openCount.incrementAndGet();
                }
                SeaCircuitBreakerManager.getTimeRemainingMs();
                SeaCircuitBreakerManager.getTimeRemainingFormatted();
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              endLatch.countDown();
            }
          });
    }

    // Start all threads at once
    startLatch.countDown();

    // Wait for all threads to complete
    assertTrue(endLatch.await(10, TimeUnit.SECONDS));
    executor.shutdown();

    // All threads should have seen circuit as open
    assertEquals(1000, openCount.get()); // 10 threads * 100 iterations
  }

  @Test
  void testConcurrentRecord429Failures() throws InterruptedException {
    // Test thread safety when multiple threads record failures simultaneously
    int threadCount = 10;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch endLatch = new CountDownLatch(threadCount);

    // Start multiple threads recording failures
    for (int i = 0; i < threadCount; i++) {
      executor.submit(
          () -> {
            try {
              startLatch.await(); // Wait for all threads to be ready
              SeaCircuitBreakerManager.record429Failure();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              endLatch.countDown();
            }
          });
    }

    // Start all threads at once
    startLatch.countDown();

    // Wait for all threads to complete
    assertTrue(endLatch.await(10, TimeUnit.SECONDS));
    executor.shutdown();

    // Circuit should be open
    assertTrue(SeaCircuitBreakerManager.isCircuitOpen());

    // There should be a valid timestamp
    assertTrue(SeaCircuitBreakerManager.getTimeRemainingMs() > 0);
  }

  @Test
  void testHttpTooManyRequestsConstant() {
    // Verify the constant is correctly defined
    assertEquals(429, SeaCircuitBreakerManager.HTTP_TOO_MANY_REQUESTS);
  }

  @Test
  void testTimeRemainingAtExactBoundary() throws Exception {
    // Record a 429 failure
    SeaCircuitBreakerManager.record429Failure();

    // Set timestamp to exactly 24 hours ago
    Field timestampField =
        SeaCircuitBreakerManager.class.getDeclaredField("last429FailureTimestamp");
    timestampField.setAccessible(true);
    long exactly24HoursAgo = System.currentTimeMillis() - (24 * 60 * 60 * 1000);
    timestampField.set(null, exactly24HoursAgo);

    // Circuit should be closed (>= 24 hours means closed)
    assertFalse(SeaCircuitBreakerManager.isCircuitOpen());
    assertEquals(0, SeaCircuitBreakerManager.getTimeRemainingMs());
  }

  @Test
  void testGetTimeRemainingFormattedWithZeroMinutes() throws Exception {
    // Record a 429 failure
    SeaCircuitBreakerManager.record429Failure();

    // Set timestamp to exactly 5 hours ago (no minutes)
    // Note: We calculate it relative to when we read the timestamp to minimize timing drift
    Field timestampField =
        SeaCircuitBreakerManager.class.getDeclaredField("last429FailureTimestamp");
    timestampField.setAccessible(true);

    // Use a fixed reference time to avoid timing issues
    long now = System.currentTimeMillis();
    long fiveHoursAgo = now - (5 * 60 * 60 * 1000);
    timestampField.set(null, fiveHoursAgo);

    // Should have approximately 19 hours remaining
    // Allow for timing variations between setting timestamp and checking (18-19 hours)
    String formatted = SeaCircuitBreakerManager.getTimeRemainingFormatted();
    assertTrue(
        formatted.contains("18 hours") || formatted.contains("19 hours"),
        "Expected 18 or 19 hours, got: " + formatted);
  }

  @Test
  void testGetTimeRemainingFormattedWithSingleDigitHours() throws Exception {
    // Record a 429 failure
    SeaCircuitBreakerManager.record429Failure();

    // Set timestamp to 23 hours ago (1 hour remaining)
    Field timestampField =
        SeaCircuitBreakerManager.class.getDeclaredField("last429FailureTimestamp");
    timestampField.setAccessible(true);
    long twentyThreeHoursAgo = System.currentTimeMillis() - (23 * 60 * 60 * 1000);
    timestampField.set(null, twentyThreeHoursAgo);

    // Should have approximately 1 hour remaining
    String formatted = SeaCircuitBreakerManager.getTimeRemainingFormatted();
    assertTrue(formatted.contains("1 hours") || formatted.contains("0 hours"));
  }
}
