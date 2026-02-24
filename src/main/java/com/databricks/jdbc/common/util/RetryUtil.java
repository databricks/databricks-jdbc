package com.databricks.jdbc.common.util;

public final class RetryUtil {

  private static final int DEFAULT_BACKOFF_FACTOR = 2;
  private static final int MIN_BACKOFF_INTERVAL = 1000;
  private static final int MAX_RETRY_INTERVAL = 10 * 1000;

  private RetryUtil() {}

  public static long calculateExponentialBackoff(int executionCount) {
    return Math.min(
        MIN_BACKOFF_INTERVAL * (long) Math.pow(DEFAULT_BACKOFF_FACTOR, executionCount),
        MAX_RETRY_INTERVAL);
  }

  public static void doSleepForDelay(long delayMillis) {
    try {
      Thread.sleep(delayMillis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Sleep interrupted", e);
    }
  }
}
