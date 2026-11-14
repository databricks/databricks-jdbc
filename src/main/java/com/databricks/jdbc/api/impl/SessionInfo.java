package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.model.client.thrift.generated.TSessionHandle;
import javax.annotation.Nullable;
import org.immutables.value.Value;

@Value.Immutable
public interface SessionInfo {

  String sessionId();

  IDatabricksComputeResource computeResource();

  @Value.Default
  default DatabricksClientType clientType() {
    return sessionHandle() == null ? DatabricksClientType.SEA : DatabricksClientType.THRIFT;
  }

  @Nullable
  Long negotiatedProtocolVersion();

  /** A saved identifier's version is diagnostic and cannot grant protocol capabilities. */
  @Value.Default
  default boolean liveProtocolVersion() {
    return false;
  }

  /** Returns a version only when it came from this connection's CreateSession response. */
  @Nullable
  default Long nativeProtocolVersion() {
    return liveProtocolVersion() ? negotiatedProtocolVersion() : null;
  }

  @Nullable
  Long sessionVersionId();

  @Value.Default
  default boolean realtimeSession() {
    return false;
  }

  @Nullable
  TSessionHandle sessionHandle(); // Set for Thrift sessions on either compute type.
}
