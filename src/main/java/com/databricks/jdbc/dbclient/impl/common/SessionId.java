package com.databricks.jdbc.dbclient.impl.common;

import com.databricks.jdbc.api.impl.ImmutableSessionInfo;
import com.databricks.jdbc.common.AllPurposeCluster;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.common.Warehouse;
import com.databricks.jdbc.common.util.DatabricksThriftUtil;
import com.databricks.jdbc.dbclient.impl.thrift.ResourceId;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.THandleIdentifier;
import com.databricks.jdbc.model.client.thrift.generated.TSessionHandle;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Objects;

/** A Session-Id identifier to uniquely identify a connection session */
public class SessionId {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(SessionId.class);
  final DatabricksClientType clientType;
  final String guid;
  final String secret;
  final IDatabricksComputeResource clusterResource;

  SessionId(
      DatabricksClientType clientType,
      String guid,
      String secret,
      IDatabricksComputeResource clusterResource) {
    this.clientType = clientType;
    this.guid = guid;
    this.secret = secret;
    this.clusterResource = clusterResource;
  }

  /** Constructs a SessionId identifier for a given SQL Exec session-Id */
  public SessionId(String sessionId, IDatabricksComputeResource warehouseId) {
    this(DatabricksClientType.SEA, sessionId, null, warehouseId);
  }

  /** Constructs a SessionId identifier for a given Thrift Server session-Id */
  public SessionId(THandleIdentifier identifier, IDatabricksComputeResource computeResource) {
    this(
        DatabricksClientType.THRIFT,
        ResourceId.fromBytes(identifier.getGuid()).toString(),
        ResourceId.fromBytes(identifier.getSecret()).toString(),
        computeResource);
  }

  /** Creates a SessionId identifier for a given Thrift Server session-Id */
  public static SessionId create(ImmutableSessionInfo sessionInfo) {
    if (sessionInfo.computeResource() instanceof Warehouse) {
      return new SessionId(sessionInfo.sessionId(), sessionInfo.computeResource());
    } else {
      assert sessionInfo.sessionHandle() != null;
      return new SessionId(
          sessionInfo.sessionHandle().getSessionId(), sessionInfo.computeResource());
    }
  }

  /** Deserializes a SessionId from a serialized string */
  public static SessionId deserialize(String serializedSessionId) throws SQLException {
    // We serialize the session-Id as:
    // For thrift: t|session-guid-id|session-secret
    // For SEA: s|warehouseId|session-id
    String[] parts = serializedSessionId.split("\\|");
    if (parts.length != 3) {
      String errorMessage =
          String.format("Session ID has invalid number of parts %s", serializedSessionId);
      LOGGER.error(errorMessage);
      throw new DatabricksParsingException(
          errorMessage, DatabricksDriverErrorCode.SESSION_ID_PARSING_EXCEPTION);
    }
    switch (parts[0]) {
      case "s":
        return new SessionId(parts[2], new Warehouse(parts[1]));

      case "t":
        return new SessionId(DatabricksClientType.THRIFT, parts[1], parts[2], null);
    }
    String errorMessage =
        String.format("Session ID has 3 parts but is invalid %s", serializedSessionId);
    LOGGER.error(errorMessage);
    throw new DatabricksParsingException(
        errorMessage, DatabricksDriverErrorCode.SESSION_ID_PARSING_EXCEPTION);
  }

  @Override
  public String toString() {
    switch (clientType) {
      case SEA:
        return String.format("s|%s|%s", ((Warehouse) clusterResource).getWarehouseId(), guid);
      case THRIFT:
        return String.format("t|%s|%s", guid, secret);
    }
    return guid;
  }

  /** Returns an ImmutableSessionInfo for the given session-Id */
  public ImmutableSessionInfo getSessionInfo() {
    switch (clientType) {
      case THRIFT:
        return ImmutableSessionInfo.builder()
            .sessionId(
                DatabricksThriftUtil.byteBufferToString(
                    ByteBuffer.wrap(ResourceId.fromBase64(guid).toBytes())))
            // compute resource is not needed for Thrift flow, setting a dummy value to bypass null
            // check
            .computeResource(
                clusterResource != null ? clusterResource : new AllPurposeCluster("", ""))
            .sessionHandle(
                new TSessionHandle(
                    new THandleIdentifier()
                        .setGuid(ResourceId.fromBase64(guid).toBytes())
                        .setSecret(ResourceId.fromBase64(secret).toBytes())))
            .build();
      case SEA:
        return ImmutableSessionInfo.builder()
            .sessionHandle(null)
            .sessionId(guid)
            .computeResource(clusterResource)
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
        // For Thrift client type, cluster resource is ignored
        && (this.clientType == DatabricksClientType.THRIFT
            || Objects.equals(this.clusterResource, ((SessionId) otherSession).clusterResource));
  }
}
