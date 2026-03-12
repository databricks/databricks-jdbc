package com.databricks.jdbc.common.safe;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import java.util.Map;

public class FeatureFlagTestUtil {
  public static void enableFeatureFlagForTesting(
      IDatabricksConnectionContext connectionContext, Map<String, String> flags) {
    DatabricksDriverFeatureFlagsContextFactory.setFeatureFlagsContext(connectionContext, flags);
  }
}
