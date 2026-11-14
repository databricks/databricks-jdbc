package com.databricks.jdbc.dbclient.impl.common;

import com.databricks.jdbc.api.impl.ImmutableSessionInfo;
import com.databricks.jdbc.common.AllPurposeCluster;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.common.Warehouse;
import com.databricks.jdbc.common.util.DatabricksThriftUtil;
import com.databricks.jdbc.dbclient.impl.sqlexec.SeaComputeTarget;
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
  private final boolean realtimeSession;
  private final Long negotiatedProtocolVersion;

  private SessionId(
      DatabricksClientType clientType,
      String guid,
      String secret,
      IDatabricksComputeResource clusterResource,
      boolean realtimeSession,
      Long negotiatedProtocolVersion) {
    this.clientType = clientType;
    this.guid = guid;
    this.secret = secret;
    this.clusterResource = clusterResource;
    this.realtimeSession = realtimeSession;
    this.negotiatedProtocolVersion = negotiatedProtocolVersion;
  }

  /** Constructs a SessionId identifier for a given SQL Exec session-Id */
  public SessionId(String sessionId, IDatabricksComputeResource warehouseId) {
    this(sessionId, warehouseId, false);
  }

  public SessionId(
      String sessionId, IDatabricksComputeResource warehouseId, boolean realtimeSession) {
    this(DatabricksClientType.SEA, sessionId, null, warehouseId, realtimeSession, null);
  }

  /** Constructs a SessionId identifier for a given Thrift Server session-Id */
  public SessionId(THandleIdentifier identifier, IDatabricksComputeResource computeResource) {
    this(
        DatabricksClientType.THRIFT,
        ResourceId.fromBytes(identifier.getGuid()).toString(),
        ResourceId.fromBytes(identifier.getSecret()).toString(),
        computeResource,
        false,
        null);
  }

  /** Creates a SessionId identifier for a given Thrift Server session-Id */
  public static SessionId create(ImmutableSessionInfo sessionInfo) {
    if (sessionInfo.clientType() == DatabricksClientType.THRIFT) {
      assert sessionInfo.sessionHandle() != null;
      return new SessionId(
          sessionInfo.sessionHandle().getSessionId(), sessionInfo.computeResource());
    }
    return new SessionId(
        DatabricksClientType.SEA,
        sessionInfo.sessionId(),
        null,
        sessionInfo.computeResource(),
        sessionInfo.realtimeSession(),
        SeaComputeTarget.of(sessionInfo.computeResource()).isApc()
            ? sessionInfo.negotiatedProtocolVersion()
            : null);
  }

  /** Deserializes a SessionId from a serialized string */
  public static SessionId deserialize(String serializedSessionId) throws SQLException {
    // We serialize the session-Id as:
    // For thrift: t|session-guid-id|session-secret
    // For SQL Execution API: s|warehouseId|session-id[|r] for Real-time sessions
    // For APC SEA: s|a|org-id|cluster-id|session-id|protocol-version-or--
    if (serializedSessionId == null) {
      throw invalid("Session ID has invalid structure");
    }
    String[] parts = serializedSessionId.split("\\|", -1);
    if (parts.length == 6 && "s".equals(parts[0]) && "a".equals(parts[1])) {
      for (int i = 2; i < 5; i++) {
        if (parts[i].isEmpty()) {
          throw invalid("APC session ID has an empty segment");
        }
      }
      Long version = null;
      if (!"-".equals(parts[5])) {
        if (!parts[5].matches("[0-9]+")) {
          throw invalid("APC session ID has an invalid protocol version");
        }
        try {
          version = Long.parseLong(parts[5]);
        } catch (NumberFormatException e) {
          throw invalid("APC session ID has an invalid protocol version");
        }
      }
      return new SessionId(
          DatabricksClientType.SEA,
          parts[4],
          null,
          new AllPurposeCluster(parts[2], parts[3]),
          false,
          version);
    }
    if (parts.length < 3 || parts.length > 4 || parts[0].isEmpty()) {
      throw invalid("Session ID has invalid structure");
    }
    for (String part : parts) {
      if (part.isEmpty()) {
        throw invalid("Session ID has an empty segment");
      }
    }
    switch (parts[0]) {
      case "s":
        if ("a".equals(parts[1])) {
          throw invalid("APC session ID has invalid structure");
        }
        if (parts.length == 4 && !"r".equals(parts[3])) {
          throw invalid("Session ID has invalid route marker");
        }
        return new SessionId(parts[2], new Warehouse(parts[1]), parts.length == 4);

      case "t":
        if (parts.length == 3) {
          return new SessionId(DatabricksClientType.THRIFT, parts[1], parts[2], null, false, null);
        }
        break;
    }
    throw invalid("Session ID has invalid structure");
  }

  private static DatabricksParsingException invalid(String errorMessage) {
    LOGGER.error(errorMessage);
    return new DatabricksParsingException(
        errorMessage,
        null,
        DatabricksDriverErrorCode.SESSION_ID_PARSING_EXCEPTION.name(),
        DatabricksDriverErrorCode.SESSION_ID_PARSING_EXCEPTION.getCode());
  }

  @Override
  public String toString() {
    switch (clientType) {
      case SEA:
        if (SeaComputeTarget.of(clusterResource).isApc()) {
          AllPurposeCluster cluster = (AllPurposeCluster) clusterResource;
          return String.format(
              "s|a|%s|%s|%s|%s",
              cluster.getOrgId(),
              cluster.getClusterId(),
              guid,
              negotiatedProtocolVersion == null ? "-" : negotiatedProtocolVersion);
        }
        String serialized =
            String.format("s|%s|%s", SeaComputeTarget.of(clusterResource).warehouseId(), guid);
        return realtimeSession ? serialized + "|r" : serialized;
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
            .clientType(DatabricksClientType.THRIFT)
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
            .clientType(DatabricksClientType.SEA)
            .sessionHandle(null)
            .sessionId(guid)
            .computeResource(clusterResource)
            .realtimeSession(realtimeSession)
            .negotiatedProtocolVersion(negotiatedProtocolVersion)
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
        && this.realtimeSession == ((SessionId) otherSession).realtimeSession
        && Objects.equals(
            this.negotiatedProtocolVersion, ((SessionId) otherSession).negotiatedProtocolVersion)
        // For Thrift client type, cluster resource is ignored
        && (this.clientType == DatabricksClientType.THRIFT
            || Objects.equals(this.clusterResource, ((SessionId) otherSession).clusterResource));
  }
}
