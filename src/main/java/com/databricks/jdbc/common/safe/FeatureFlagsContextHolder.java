package com.databricks.jdbc.common.safe;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import java.util.concurrent.ConcurrentHashMap;

final class FeatureFlagsContextHolder {
  final DatabricksDriverFeatureFlagsContext context;
  final ConcurrentHashMap<String, IDatabricksConnectionContext> activeContexts;

  FeatureFlagsContextHolder(
      DatabricksDriverFeatureFlagsContext context, IDatabricksConnectionContext initialContext) {
    this.context = context;
    this.activeContexts = new ConcurrentHashMap<>();
    addContext(initialContext);
  }

  void addContext(IDatabricksConnectionContext ctx) {
    String uuid = ctx != null ? ctx.getConnectionUuid() : null;
    if (uuid != null) {
      activeContexts.put(uuid, ctx);
    }
  }
}
