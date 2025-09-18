package com.databricks.jdbc.common.safe;

final class FeatureFlagsContextHolder {
  final DatabricksDriverFeatureFlagsContext context;
  int refCount;

  FeatureFlagsContextHolder(DatabricksDriverFeatureFlagsContext context, int refCount) {
    this.context = context;
    this.refCount = refCount;
  }
}
