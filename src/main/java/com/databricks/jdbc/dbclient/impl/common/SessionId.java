package com.databricks.jdbc.dbclient.impl.common;

import com.databricks.jdbc.api.impl.ImmutableSessionInfo;
import com.databricks.jdbc.common.AllPurposeCluster;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.common.Warehouse;
import com.databricks.jdbc.dbclient.impl.thrift.ResourceId;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.THandleIdentifier;
import com.databricks.jdbc.model.client.thrift.generated.TSessionHandle;
import java.util.Objects;

/** A Session-Id identifier to uniquely identify a connection session */
public class SessionId {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(SessionId.class);
  final DatabricksClientType clientType;
  final String guid;
  final String secret;
  final String clusterResourceId;

  SessionId(DatabricksClientType clientType, String guid, String secret, String clusterResourceId) {
    this.clientType = clientType;
    this.guid = guid;
    this.secret = secret;
    this.clusterResourceId = clusterResourceId;
  }

  /** Constructs a SessionId identifier for a given SQL Exec session-Id */
  public SessionId(String sessionId, String warehouseId) {
    this(DatabricksClientType.SQL_EXEC, sessionId, null, warehouseId);
  }

  /** Constructs a SessionId identifier for a given Thrift Server session-Id */
  public SessionId(THandleIdentifier identifier, String clusterId) {
    this(
        DatabricksClientType.THRIFT,
        ResourceId.fromBytes(identifier.getGuid()).toString(),
        ResourceId.fromBytes(identifier.getSecret()).toString(),
        clusterId);
  }

  /** Creates a SessionId identifier for a given Thrift Server session-Id */
  public static SessionId create(ImmutableSessionInfo sessionInfo) {
    if (sessionInfo.computeResource() instanceof Warehouse) {
      return new SessionId(
          sessionInfo.sessionId(), ((Warehouse) sessionInfo.computeResource()).getWarehouseId());
    } else {
      assert sessionInfo.sessionHandle() != null;
      return new SessionId(
          sessionInfo.sessionHandle().getSessionId(),
          ((AllPurposeCluster) sessionInfo.computeResource()).getClusterId());
    }
  }

  /** Deserializes a SessionId from a serialized string */
  public static SessionId deserialize(String serializedSessionId) {
    // We serialize the session-Id as:
    // For thrift: t/clusterId/session-id
    // For SEA: s/warehouseId/session-id
    String[] parts = serializedSessionId.split("/");
    if (parts.length != 3) {
      throw new IllegalArgumentException("Invalid session-Id " + serializedSessionId);
    }
    switch (parts[0]) {
      case "s":
        return new SessionId(parts[2], parts[1]);

      case "t":
        String[] idParts = parts[2].split("\\|");
        if (idParts.length != 2) {
          throw new IllegalArgumentException("Invalid session-Id " + serializedSessionId);
        }
        return new SessionId(DatabricksClientType.THRIFT, idParts[0], idParts[1], parts[1]);
      default:
        throw new IllegalArgumentException("Invalid session-Id " + serializedSessionId);
    }
  }

  @Override
  public String toString() {
    switch (clientType) {
      case SQL_EXEC:
        return String.format("s/%s/%s", clusterResourceId, guid);
      case THRIFT:
        return String.format("t/%s/%s|%s", clusterResourceId, guid, secret);
    }
    return guid;
  }

  /** Returns an ImmutableSessionInfo for the given session-Id */
  public ImmutableSessionInfo getSessionInfo() {
    switch (clientType) {
      case THRIFT:
        return ImmutableSessionInfo.builder()
            .sessionId("")
            .computeResource(new AllPurposeCluster("", clusterResourceId))
            .sessionHandle(
                new TSessionHandle(
                    new THandleIdentifier()
                        .setGuid(ResourceId.fromBase64(guid).toBytes())
                        .setSecret(ResourceId.fromBase64(secret).toBytes())))
            .build();
      case SQL_EXEC:
        return ImmutableSessionInfo.builder()
            .sessionHandle(null)
            .sessionId(guid)
            .computeResource(new Warehouse(clusterResourceId))
            .build();
    }
    // should not reach here
    return null;
  }

  /** Returns the client-type for the given session-Id */
  public DatabricksClientType getClientType() {
    return clientType;
  }

  @Override
  public boolean equals(Object otherSession) {
    if (!(otherSession instanceof SessionId)
        || (this.clientType != ((SessionId) otherSession).clientType)) {
      return false;
    }
    return Objects.equals(this.guid, ((SessionId) otherSession).guid)
        && Objects.equals(this.secret, ((SessionId) otherSession).secret)
        && Objects.equals(this.clusterResourceId, ((SessionId) otherSession).clusterResourceId);
  }
}
