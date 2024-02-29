package com.databricks.jdbc.core;

import com.databricks.jdbc.core.types.ComputeResource;
import org.immutables.value.Value;

@Value.Immutable
public interface SessionInfo {

  String sessionId();

  ComputeResource computeResource();
}
