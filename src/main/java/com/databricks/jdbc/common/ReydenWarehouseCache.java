package com.databricks.jdbc.common;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe, process-wide cache of warehouses known to be Real-Time SQL (Reyden) warehouses.
 *
 * <p>When a Thrift OpenSession fails with SQLSTATE "KP001" (not supported for Thrift protocol), the
 * warehouse is marked as Reyden and future connections to the same warehouse skip Thrift and use
 * SEA directly, avoiding the rejection error.
 *
 * <p>Cache entries expire after ~6 hours to allow warehouses to be de-registered or reconfigured.
 * The cache key is (host_lowercased, warehouse_id) for multi-tenant safety.
 */
public final class ReydenWarehouseCache {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ReydenWarehouseCache.class);

  /** Approximate TTL in milliseconds: 6 hours */
  private static final long ENTRY_TTL_MILLIS = 6 * 60 * 60 * 1000L;

  private static final ReydenWarehouseCache INSTANCE = new ReydenWarehouseCache();

  private static class CacheEntry {
    final long timestamp;

    CacheEntry() {
      this.timestamp = System.currentTimeMillis();
    }

    boolean isExpired() {
      return System.currentTimeMillis() - timestamp > ENTRY_TTL_MILLIS;
    }
  }

  private final ConcurrentHashMap<String, CacheEntry> cache = new ConcurrentHashMap<>();

  private ReydenWarehouseCache() {}

  public static ReydenWarehouseCache getInstance() {
    return INSTANCE;
  }

  /**
   * Returns true if the warehouse (host, warehouse_id) pair is known to be Reyden. Removes expired
   * entries opportunistically.
   */
  public boolean isReydenWarehouse(String hostLowercased, String warehouseId) {
    if (hostLowercased == null || warehouseId == null) {
      return false;
    }
    String key = buildKey(hostLowercased, warehouseId);
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
  public void markReydenWarehouse(String hostLowercased, String warehouseId) {
    if (hostLowercased == null || warehouseId == null) {
      return;
    }
    String key = buildKey(hostLowercased, warehouseId);
    cache.put(key, new CacheEntry());
    evictExpiredEntries(); // opportunistic sweep on write, when the map may grow
    LOGGER.debug(
        "Marked warehouse as Reyden (host={}, warehouse_id={}). "
            + "Future connections will use SEA directly.",
        hostLowercased,
        warehouseId);
  }

  /** Removes all entries. Intended for test isolation of the process-wide singleton. */
  public void clearCache() {
    cache.clear();
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

  private static String buildKey(String hostLowercased, String warehouseId) {
    return hostLowercased.toLowerCase() + "|" + warehouseId;
  }
}
