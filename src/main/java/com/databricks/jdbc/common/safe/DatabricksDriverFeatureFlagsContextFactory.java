package com.databricks.jdbc.common.safe;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.telemetry.TelemetryHelper;
import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Factory class to manage DatabricksDriverFeatureFlagsContext instances */
public class DatabricksDriverFeatureFlagsContextFactory {
  private static final Map<String, FeatureFlagsContextHolder> contextMap =
      new ConcurrentHashMap<>();

  private DatabricksDriverFeatureFlagsContextFactory() {
    // Private constructor to prevent instantiation
  }

  /**
   * Gets or creates a DatabricksDriverFeatureFlagsContext instance for the given compute. Multiple
   * calls with the same connection UUID are idempotent — only one reference is tracked per
   * connection.
   *
   * @param context the connection context
   * @return the DatabricksDriverFeatureFlagsContext instance
   */
  public static DatabricksDriverFeatureFlagsContext getInstance(
      IDatabricksConnectionContext context) {
    String key = TelemetryHelper.keyOf(context);
    FeatureFlagsContextHolder holder =
        contextMap.compute(
            key,
            (k, existing) -> {
              if (existing == null) {
                // First connection for this host
                return new FeatureFlagsContextHolder(
                    new DatabricksDriverFeatureFlagsContext(context), context);
              }
              // Track this connection UUID (idempotent for the same UUID)
              existing.addContext(context);
              return existing;
            });
    return holder.context;
  }

  /**
   * Removes the reference for the given connection context. When the last connection for a host is
   * removed, the context is shut down. If the removed connection was the one whose context was
   * stored internally, the stored context is updated to another active connection to prevent HTTP
   * client leaks.
   *
   * @param connectionContext the connection context
   */
  public static void removeInstance(IDatabricksConnectionContext connectionContext) {
    if (connectionContext != null) {
      String key = TelemetryHelper.keyOf(connectionContext);
      String uuid = connectionContext.getConnectionUuid();
      contextMap.computeIfPresent(
          key,
          (k, holder) -> {
            if (uuid != null) {
              holder.activeContexts.remove(uuid);
            }
            if (holder.activeContexts.isEmpty()) {
              holder.context.shutdown();
              return null;
            }
            // If the removed connection's UUID matches the one stored in the feature flags context,
            // update to another active connection to prevent stale HTTP client lookups.
            IDatabricksConnectionContext current = holder.context.getConnectionContext();
            if (current != null && uuid != null && uuid.equals(current.getConnectionUuid())) {
              IDatabricksConnectionContext replacement =
                  holder.activeContexts.values().iterator().next();
              holder.context.updateConnectionContext(replacement);
            }
            return holder;
          });
    }
  }

  @VisibleForTesting
  public static void setFeatureFlagsContext(
      IDatabricksConnectionContext connectionContext, Map<String, String> featureFlags) {
    String key = TelemetryHelper.keyOf(connectionContext);
    contextMap.compute(
        key,
        (k, existing) -> {
          if (existing != null) {
            existing.context.shutdown();
          }
          return new FeatureFlagsContextHolder(
              new DatabricksDriverFeatureFlagsContext(connectionContext, featureFlags),
              connectionContext);
        });
  }
}
