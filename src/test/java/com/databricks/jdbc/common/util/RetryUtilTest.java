package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class RetryUtilTest {

  @Test
  void calculateExponentialBackoff_firstAttempt() {
    long result = RetryUtil.calculateExponentialBackoff(0);
    assertEquals(1000, result);
  }

  @Test
  void calculateExponentialBackoff_secondAttempt() {
    long result = RetryUtil.calculateExponentialBackoff(1);
    assertEquals(2000, result);
  }

  @Test
  void calculateExponentialBackoff_thirdAttempt() {
    long result = RetryUtil.calculateExponentialBackoff(2);
    assertEquals(4000, result);
  }

  @Test
  void calculateExponentialBackoff_exceedsMax() {
    long result = RetryUtil.calculateExponentialBackoff(10);
    assertEquals(10000, result);
  }

  @Test
  void doSleepForDelay_negativeDelay() {
    assertThrows(IllegalArgumentException.class, () -> RetryUtil.doSleepForDelay(-1));
  }

  @Test
  void doSleepForDelay_zeroDelay() {
    long start = System.currentTimeMillis();
    RetryUtil.doSleepForDelay(0);
    long elapsed = System.currentTimeMillis() - start;
    assertEquals(0, elapsed, 10);
  }

  @Test
  void doSleepForDelay_smallDelay() {
    long start = System.currentTimeMillis();
    RetryUtil.doSleepForDelay(50);
    long elapsed = System.currentTimeMillis() - start;
    assertEquals(50, elapsed, 20);
  }
}
