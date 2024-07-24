package com.databricks.jdbc.client.http;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RetryHandlerTest {
  volatile boolean interrupted = false;

  @Test
  void testIsImmediateRetryNotAllowed() {
    assertTrue(RetryHandler.isImmediateRetryNotAllowed(429, true, false, -1));
    assertFalse(RetryHandler.isImmediateRetryNotAllowed(200, false, true, -1));
  }

  @Test
  void testIsTemporarilyUnavailableRetryTimeoutExceeded() {
    assertTrue(RetryHandler.isTemporarilyUnavailableRetryTimeoutExceeded(503, 2, 1, 1));
    assertFalse(RetryHandler.isTemporarilyUnavailableRetryTimeoutExceeded(503, 0, 1, 2));
  }

  @Test
  void testIsRateLimitRetryTimeoutExceeded() {
    assertTrue(RetryHandler.isRateLimitRetryTimeoutExceeded(429, 2, 1, 1));
    assertFalse(RetryHandler.isRateLimitRetryTimeoutExceeded(429, 1, 1, 3));
  }

  @Test
  void testIsRetryDisabledButReceivedResponse() {
    assertTrue(RetryHandler.isRetryDisabledButReceivedResponse(503, false, true, 1));
    assertFalse(RetryHandler.isRetryDisabledButReceivedResponse(429, true, false, -1));
  }

  @Test
  void testSleepForDelay() {
    long startTime = System.currentTimeMillis();
    RetryHandler.sleepForDelay(1);
    long endTime = System.currentTimeMillis();
    assertTrue((endTime - startTime) >= 1000, "Sleep did not last for at least 1 second.");
  }

  @Test
  void testSleepForDelayWithInterruptedException() {
    Thread testThread =
        new Thread(
            () -> {
              try {
                RetryHandler.sleepForDelay(1);
              } catch (RuntimeException e) {
                interrupted = true;
                Thread.currentThread().interrupt();
              }
            });
    testThread.start();
    try {
      Thread.sleep(500);
      testThread.interrupt();
      testThread.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    assertTrue(interrupted, "Thread should be interrupted.");
  }
}
