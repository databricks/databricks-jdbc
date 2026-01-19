package com.databricks.jdbc.telemetry.latency;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Manages TelemetryCollector instances per connection. Each connection gets its own
 * TelemetryCollector instance to ensure telemetry data is properly isolated.
 */
public class TelemetryCollectorManager {
  private static final TelemetryCollectorManager INSTANCE = new TelemetryCollectorManager();
  private static final String DEFAULT_CONNECTION = "unknown-connection";

  private final ConcurrentHashMap<String, TelemetryCollector> collectors =
      new ConcurrentHashMap<>();

  private TelemetryCollectorManager() {
    // Private constructor for singleton
  }

  public static TelemetryCollectorManager getInstance() {
    return INSTANCE;
  }

  /**
   * Gets or creates a TelemetryCollector for the given connection context. Null checks are retained
   * due to ThreadLocal usage patterns that may result in null context.
   *
   * @param context the connection context
   * @return the TelemetryCollector instance for this connection
   */
  public TelemetryCollector getOrCreateCollector(IDatabricksConnectionContext context) {
    String key =
        (context != null && context.getConnectionUuid() != null)
            ? context.getConnectionUuid()
            : DEFAULT_CONNECTION;
    return collectors.computeIfAbsent(key, k -> new TelemetryCollector());
  }

  /**
   * Removes and returns the TelemetryCollector for the given connection. Should be called when a
   * connection is closed.
   *
   * @param connectionUuid the connection UUID
   * @return the removed TelemetryCollector, or null if not found
   */
  public TelemetryCollector removeCollector(String connectionUuid) {
    String key = connectionUuid != null ? connectionUuid : DEFAULT_CONNECTION;
    return collectors.remove(key);
  }

  /**
   * Removes and returns the TelemetryCollector for the given connection context.
   *
   * @param context the connection context
   * @return the removed TelemetryCollector, or null if not found
   */
  public TelemetryCollector removeCollector(IDatabricksConnectionContext context) {
    if (context == null) {
      return removeCollector(DEFAULT_CONNECTION);
    }
    return removeCollector(context.getConnectionUuid());
  }

  /**
   * Safely gets a TelemetryCollector from connection context, catching and ignoring any errors.
   * This is useful for code that doesn't want telemetry failures to affect main logic.
   *
   * @param connectionContextSupplier a function that provides the connection context
   * @return the TelemetryCollector, or null if an error occurred
   */
  public TelemetryCollector getCollectorSafely(
      Supplier<IDatabricksConnectionContext> connectionContextSupplier) {
    try {
      IDatabricksConnectionContext context = connectionContextSupplier.get();
      return getOrCreateCollector(context);
    } catch (Exception e) {
      // Silently ignore errors when retrieving telemetry collector
      return null;
    }
  }

  /** Clears all collectors. For testing purposes only. */
  public void clear() {
    collectors.clear();
  }
}
