package com.databricks.jdbc.common.safe;

import java.util.concurrent.atomic.AtomicInteger;

final class FeatureFlagsContextHolder {
  final DatabricksDriverFeatureFlagsContext context;
  AtomicInteger refCount;

  FeatureFlagsContextHolder(DatabricksDriverFeatureFlagsContext context, int refCount) {
    this.context = context;
    this.refCount = new AtomicInteger(refCount);
  }
}
