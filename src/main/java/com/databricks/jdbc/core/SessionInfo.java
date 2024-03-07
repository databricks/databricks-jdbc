package com.databricks.jdbc.core;

import com.databricks.jdbc.core.types.ComputeResource;
import javax.annotation.Nullable;
import org.immutables.value.Value;

@Value.Immutable
public interface SessionInfo {

  String sessionId();

  @Nullable
  byte[] secret();

  ComputeResource computeResource();
}
