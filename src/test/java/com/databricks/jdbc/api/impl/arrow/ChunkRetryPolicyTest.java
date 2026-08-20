package com.databricks.jdbc.api.impl.arrow;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class ChunkRetryPolicyTest {

  @Test
  void retryDelayUsesInclusiveEqualJitterBounds() {
    AtomicLong requestedBound = new AtomicLong();
    ChunkRetryPolicy minimumPolicy =
        new ChunkRetryPolicy(
            bound -> {
              requestedBound.set(bound);
              return 0;
            });
    ChunkRetryPolicy maximumPolicy = new ChunkRetryPolicy(bound -> bound - 1);

    long minimumDelay = minimumPolicy.getRetryDelayMs();
    long maximumDelay = maximumPolicy.getRetryDelayMs();

    assertEquals(751, requestedBound.get());
    assertEquals(ChunkRetryPolicy.MIN_RETRY_DELAY_MS, minimumDelay);
    assertEquals(ChunkRetryPolicy.MAX_RETRY_DELAY_MS, maximumDelay);
  }
}
