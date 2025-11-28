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
   * Gets or creates a DatabricksDriverFeatureFlagsContext instance for the given compute
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
                // First reference for this compute
                return new FeatureFlagsContextHolder(
                    new DatabricksDriverFeatureFlagsContext(context), 1);
              }
              // Additional reference for the same compute
              existing.refCount.incrementAndGet();
              return existing;
            });
    return holder.context;
  }

  /**
   * Removes the DatabricksDriverFeatureFlagsContext instance for the given compute.
   *
   * @param connectionContext the connection context
   */
  public static void removeInstance(IDatabricksConnectionContext connectionContext) {
    if (connectionContext != null) {
      String key = TelemetryHelper.keyOf(connectionContext);
      contextMap.computeIfPresent(
          key,
          (k, holder) -> {
            // Last reference being removed: shutdown and remove entry
            if (holder.refCount.get() <= 1) {
              holder.context.shutdown();
              return null;
            }
            // Still referenced elsewhere: just decrement
            holder.refCount.decrementAndGet();
            return holder;
          });
    }
  }

  @VisibleForTesting
  public static void setFeatureFlagsContext(
      IDatabricksConnectionContext connectionContext, Map<String, String> featureFlags) {
    String key = TelemetryHelper.keyOf(connectionContext);
    contextMap.put(
        key,
        new FeatureFlagsContextHolder(
            new DatabricksDriverFeatureFlagsContext(connectionContext, featureFlags), 1));
  }
}
