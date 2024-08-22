package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.client.impl.thrift.generated.TSessionHandle;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import javax.annotation.Nullable;
import org.immutables.value.Value;

@Value.Immutable
public interface SessionInfo {

  String sessionId();

  IDatabricksComputeResource computeResource();

  @Nullable
  TSessionHandle sessionHandle(); // This field is set only for all-purpose cluster compute
}
