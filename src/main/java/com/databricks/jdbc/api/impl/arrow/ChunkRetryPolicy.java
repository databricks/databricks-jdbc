package com.databricks.jdbc.api.impl.arrow;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongUnaryOperator;

/** Adds equal jitter so concurrent chunk downloads do not retry in lockstep. */
final class ChunkRetryPolicy {

  static final long MIN_RETRY_DELAY_MS = 750;
  static final long MAX_RETRY_DELAY_MS = 1500;
  private final LongUnaryOperator randomLong;

  ChunkRetryPolicy() {
    this(bound -> ThreadLocalRandom.current().nextLong(bound));
  }

  ChunkRetryPolicy(LongUnaryOperator randomLong) {
    this.randomLong = randomLong;
  }

  long getRetryDelayMs() {
    long range = MAX_RETRY_DELAY_MS - MIN_RETRY_DELAY_MS + 1;
    return MIN_RETRY_DELAY_MS + randomLong.applyAsLong(range);
  }

  void sleep(long retryDelayMs) throws InterruptedException {
    Thread.sleep(retryDelayMs);
  }
}
