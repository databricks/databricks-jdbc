package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.model.client.thrift.generated.TSessionHandle;
import javax.annotation.Nullable;
import org.immutables.value.Value;

@Value.Immutable
public interface SessionInfo {

  String sessionId();

  IDatabricksComputeResource computeResource();

  @Nullable
  TSessionHandle sessionHandle(); // This field is set only for all-purpose cluster compute
}
