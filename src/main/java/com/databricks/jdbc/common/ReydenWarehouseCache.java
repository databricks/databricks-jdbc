package com.databricks.jdbc.common;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Thread-safe, process-wide cache of warehouses known to be Real-Time SQL (Reyden) warehouses.
 *
 * <p>When a Thrift OpenSession fails with SQLSTATE "KP001" (not supported for Thrift protocol), the
 * warehouse is marked as Reyden and future connections to the same warehouse skip Thrift and use
 * SEA directly, avoiding the rejection error.
 *
 * <p>Cache entries expire after ~6 hours to allow warehouses to be de-registered or reconfigured.
 * The cache key is (host, warehouse_id) for multi-tenant safety; the host is normalized
 * case-insensitively in {@link #buildKey}, so callers need not lowercase it.
 */
public final class ReydenWarehouseCache {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ReydenWarehouseCache.class);

  /** Approximate TTL in milliseconds: 6 hours */
  private static final long ENTRY_TTL_MILLIS = 6 * 60 * 60 * 1000L;

  private static final ReydenWarehouseCache INSTANCE = new ReydenWarehouseCache();

  // Non-static so it reads the enclosing cache's clock/TTL, which the test
  // constructor can override to exercise the expiry and eviction paths.
  private class CacheEntry {
    final long timestamp;

    CacheEntry() {
      this.timestamp = clock.getAsLong();
    }

    boolean isExpired() {
      return clock.getAsLong() - timestamp > ttlMillis;
    }
  }

  private final ConcurrentHashMap<String, CacheEntry> cache = new ConcurrentHashMap<>();

  // Time source and TTL are injectable so tests can drive expiry deterministically
  // without sleeping; production uses the wall clock and the 6h TTL.
  private final LongSupplier clock;
  private final long ttlMillis;

  private ReydenWarehouseCache() {
    this(System::currentTimeMillis, ENTRY_TTL_MILLIS);
  }

  ReydenWarehouseCache(LongSupplier clock, long ttlMillis) {
    this.clock = clock;
    this.ttlMillis = ttlMillis;
  }

  public static ReydenWarehouseCache getInstance() {
    return INSTANCE;
  }

  /**
   * Returns true if the warehouse (host, warehouse_id) pair is known to be Reyden. Removes expired
   * entries opportunistically.
   */
  public boolean isReydenWarehouse(String host, String warehouseId) {
    if (host == null || warehouseId == null) {
      return false;
    }
    String key = buildKey(host, warehouseId);
    CacheEntry entry = cache.get(key);
    if (entry == null) {
      return false;
    }
    if (entry.isExpired()) {
      cache.remove(key, entry); // remove if expired
      return false;
    }
    return true;
  }

  /** Marks the warehouse (host, warehouse_id) pair as Reyden. */
  public void markReydenWarehouse(String host, String warehouseId) {
    if (host == null || warehouseId == null) {
      return;
    }
    String key = buildKey(host, warehouseId);
    cache.put(key, new CacheEntry());
    evictExpiredEntries(); // opportunistic sweep on write, when the map may grow
    LOGGER.debug(
        "Marked warehouse as Reyden (host={}, warehouse_id={}). "
            + "Future connections will use SEA directly.",
        host,
        warehouseId);
  }

  /** Removes all entries. Intended for test isolation of the process-wide singleton. */
  public void clearCache() {
    cache.clear();
  }

  /** Current number of cached entries. Intended for test observability. */
  int size() {
    return cache.size();
  }

  /** Opportunistically evicts expired entries. Invoked on writes, when the map may grow. */
  void evictExpiredEntries() {
    cache.forEachEntry(
        Long.MAX_VALUE,
        entry -> {
          if (entry.getValue().isExpired()) {
            cache.remove(entry.getKey(), entry.getValue());
          }
        });
  }

  private static String buildKey(String host, String warehouseId) {
    // Normalize the host here so case-insensitivity holds regardless of what the caller passes.
    // Locale.ROOT avoids locale-sensitive folding (e.g. the Turkish dotless-i).
    return host.toLowerCase(Locale.ROOT) + "|" + warehouseId;
  }
}
