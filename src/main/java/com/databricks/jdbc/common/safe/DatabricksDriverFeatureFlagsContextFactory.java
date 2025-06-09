package com.databricks.jdbc.common.safe;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Factory class to manage DatabricksDriverFeatureFlagsContext instances */
public class DatabricksDriverFeatureFlagsContextFactory {
  private static final Map<String, DatabricksDriverFeatureFlagsContext> contextMap =
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
    return contextMap.computeIfAbsent(
        context.getComputeResource().getUniqueIdentifier(),
        k -> new DatabricksDriverFeatureFlagsContext(context));
  }

  /**
   * Removes the DatabricksDriverFeatureFlagsContext instance for the given compute.
   *
   * @param connectionContext the connection context
   */
  public static void removeInstance(IDatabricksConnectionContext connectionContext) {
    if (connectionContext != null) {
      contextMap.remove(connectionContext.getComputeResource().getUniqueIdentifier());
    }
  }

  @VisibleForTesting
  static void setFeatureFlagsContext(
      IDatabricksConnectionContext connectionContext, Map<String, String> featureFlags) {
    contextMap.put(
        connectionContext.getComputeResource().getUniqueIdentifier(),
        new DatabricksDriverFeatureFlagsContext(connectionContext, featureFlags));
  }
}
