package com.databricks.jdbc.core;

import com.databricks.jdbc.client.impl.thrift.generated.TSessionHandle;
import com.databricks.jdbc.core.types.ComputeResource;
import javax.annotation.Nullable;
import org.immutables.value.Value;

@Value.Immutable
public interface SessionInfo {

  String sessionId();

  ComputeResource computeResource();

  @Nullable
  TSessionHandle sessionHandle(); // This field is set only for all-purpose cluster compute
}
