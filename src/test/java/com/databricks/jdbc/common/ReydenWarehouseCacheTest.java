package com.databricks.jdbc.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * Tests the TTL/eviction behavior of {@link ReydenWarehouseCache}, using the package-private
 * clock/TTL constructor so expiry is driven deterministically rather than by wall-clock sleeps.
 */
public class ReydenWarehouseCacheTest {
  private static final String HOST = "sample-host.azuredatabricks.net";
  private static final String WAREHOUSE_ID = "warehouse_id";
  private static final long TTL_MILLIS = 1000L;

  /** Advanceable fake clock. */
  private static AtomicLong clockAt(long start) {
    return new AtomicLong(start);
  }

  @Test
  public void entryStaysUntilTtlThenIsEvictedOnAccess() {
    AtomicLong now = clockAt(0L);
    ReydenWarehouseCache cache = new ReydenWarehouseCache(now::get, TTL_MILLIS);

    cache.markReydenWarehouse(HOST, WAREHOUSE_ID);
    assertTrue(cache.isReydenWarehouse(HOST, WAREHOUSE_ID));

    // At exactly the TTL boundary the entry is still valid (expiry uses strict >).
    now.set(TTL_MILLIS);
    assertTrue(cache.isReydenWarehouse(HOST, WAREHOUSE_ID));

    // One tick past the TTL: expired, and evicted on access.
    now.set(TTL_MILLIS + 1);
    assertFalse(cache.isReydenWarehouse(HOST, WAREHOUSE_ID));
    assertEquals(0, cache.size());
  }

  @Test
  public void markSweepsExpiredEntries() {
    AtomicLong now = clockAt(0L);
    ReydenWarehouseCache cache = new ReydenWarehouseCache(now::get, TTL_MILLIS);

    cache.markReydenWarehouse(HOST, "warehouse-a");
    assertEquals(1, cache.size());

    // Advance past the TTL so the first entry is expired, then mark a second warehouse.
    // markReydenWarehouse sweeps the expired entry rather than only adding — proven by the
    // size dropping back to 1 without warehouse-a ever being looked up (which would otherwise
    // trigger the lazy per-entry eviction in isReydenWarehouse).
    now.set(TTL_MILLIS + 1);
    cache.markReydenWarehouse(HOST, "warehouse-b");

    assertEquals(1, cache.size());
    assertTrue(cache.isReydenWarehouse(HOST, "warehouse-b"));
  }
}
