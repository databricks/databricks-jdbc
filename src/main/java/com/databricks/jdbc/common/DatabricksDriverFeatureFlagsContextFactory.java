package com.databricks.jdbc.common;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.auth.DatabricksAuthClientFactory;
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
   * Gets or creates a DatabricksDriverFeatureFlagsContext instance for the given connection
   * context.
   *
   * @param context the connection context
   * @return the DatabricksDriverFeatureFlagsContext instance
   */
  public static DatabricksDriverFeatureFlagsContext getInstance(
      IDatabricksConnectionContext context) {
    return contextMap.computeIfAbsent(
        context.getConnectionUuid(),
        k ->
            new DatabricksDriverFeatureFlagsContext(
                context,
                DatabricksAuthClientFactory.getInstance()
                    .getConfigurator(context)
                    .getDatabricksConfig()
                    .authenticate()));
  }

  /**
   * Removes the DatabricksDriverFeatureFlagsContext instance for the given connection context.
   *
   * @param connectionContext the connection context
   */
  public static void removeInstance(IDatabricksConnectionContext connectionContext) {
    if (connectionContext != null) {
      contextMap.remove(connectionContext.getConnectionUuid());
    }
  }
}
