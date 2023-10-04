package com.databricks.jdbc.core;

import org.immutables.value.Value;

@Value.Immutable
public interface SessionInfo {

  String sessionId();

  String warehouseId();
}
