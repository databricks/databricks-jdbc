package com.databricks.jdbc.dbclient.impl.common;

import static com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR;

import com.databricks.jdbc.api.impl.ImmutableSessionInfo;
import com.databricks.jdbc.common.AllPurposeCluster;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.dbclient.impl.sqlexec.SeaComputeTarget;
import com.databricks.jdbc.dbclient.impl.thrift.ResourceId;
import com.databricks.jdbc.exception.DatabricksDriverException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.THandleIdentifier;
import com.databricks.jdbc.model.client.thrift.generated.TOperationHandle;
import java.util.Objects;

/** A Statement-Id identifier to uniquely identify an executed statement */
public class StatementId {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(StatementId.class);
  final DatabricksClientType clientType;
  final String guid;
  final String secret;
  private final String sessionId;
  private final String warehouseId;
  private final AllPurposeCluster apc;

  StatementId(DatabricksClientType clientType, String guid, String secret) {
    this(clientType, guid, secret, null, null, null);
  }

  private StatementId(
      DatabricksClientType clientType,
      String guid,
      String secret,
      String sessionId,
      String warehouseId,
      AllPurposeCluster apc) {
    this.clientType = clientType;
    this.guid = guid;
    this.secret = secret;
    this.sessionId = sessionId;
    this.warehouseId = warehouseId;
    this.apc = apc;
  }

  /** Constructs a StatementId identifier for a given SQl Exec statement-Id */
  public StatementId(String statementId) {
    this(DatabricksClientType.SEA, statementId, null);
  }

  public static StatementId forCurrentSession(
      String statementId, ImmutableSessionInfo sessionInfo) {
    SeaComputeTarget target = SeaComputeTarget.of(sessionInfo.computeResource());
    if (target.isApc()) {
      return new StatementId(
          DatabricksClientType.SEA,
          statementId,
          null,
          sessionInfo.sessionId(),
          null,
          (AllPurposeCluster) sessionInfo.computeResource());
    }
    if (!sessionInfo.realtimeSession()) {
      return new StatementId(statementId);
    }
    return new StatementId(
        DatabricksClientType.SEA,
        statementId,
        null,
        sessionInfo.sessionId(),
        target.warehouseId(),
        null);
  }

  public String sessionId() {
    return sessionId;
  }

  public String warehouseId() {
    return warehouseId;
  }

  public IDatabricksComputeResource computeResource() {
    return apc;
  }

  public DatabricksClientType clientType() {
    return clientType;
  }

  /** Internal routing form; public statement IDs remain the raw server ID. */
  String serialize() {
    if (apc != null) {
      return String.format("s|a|%s|%s|%s", apc.getOrgId(), apc.getClusterId(), guid);
    }
    return toString();
  }

  /** Constructs a StatementId identifier for a given Thrift Server operation handle */
  public StatementId(THandleIdentifier identifier) {
    this(
        DatabricksClientType.THRIFT,
        ResourceId.fromBytes(identifier.getGuid()).toString(),
        ResourceId.fromBytes(identifier.getSecret()).toString());
  }

  /** Deserializes a StatementId from a serialized string */
  public static StatementId deserialize(String serializedStatementId) {
    String[] idParts = serializedStatementId.split("\\|", -1);
    if (idParts.length == 5 && "s".equals(idParts[0]) && "a".equals(idParts[1])) {
      for (String part : idParts) {
        if (part.isEmpty()) {
          throw new DatabricksDriverException(
              "Invalid statement-Id structure", INPUT_VALIDATION_ERROR);
        }
      }
      return new StatementId(
          DatabricksClientType.SEA,
          idParts[4],
          null,
          null,
          null,
          new AllPurposeCluster(idParts[2], idParts[3]));
    }
    if (idParts.length == 1) {
      return new StatementId(DatabricksClientType.SEA, serializedStatementId, null);
    } else if (idParts.length == 2) {
      return new StatementId(DatabricksClientType.THRIFT, idParts[0], idParts[1]);
    } else {
      LOGGER.error("Invalid statement-Id structure");
      throw new DatabricksDriverException("Invalid statement-Id structure", INPUT_VALIDATION_ERROR);
    }
  }

  @Override
  public String toString() {
    switch (clientType) {
      case SEA:
        return guid;
      case THRIFT:
        return String.format("%s|%s", guid, secret);
    }
    return guid;
  }

  /** Returns a Thrift operation handle for the given StatementId */
  public THandleIdentifier toOperationIdentifier() {
    if (clientType.equals(DatabricksClientType.SEA)) {
      return null;
    }
    return new THandleIdentifier()
        .setGuid(ResourceId.fromBase64(guid).toBytes())
        .setSecret(ResourceId.fromBase64(secret).toBytes());
  }

  /** Returns a SQL Exec statement handle for the given StatementId */
  public String toSQLExecStatementId() {
    return guid;
  }

  /**
   * Returns a loggable statement Id for the given Thrift operation handle. This is used for logging
   * purposes to avoid logging sensitive information.
   */
  public static String loggableStatementId(TOperationHandle operationHandle) {
    if (operationHandle == null || operationHandle.getOperationId() == null) {
      return null;
    }
    return new StatementId(operationHandle.getOperationId()).toSQLExecStatementId();
  }

  /** Returns the public identifier for diagnostics, without the serialized Thrift secret. */
  public static String loggableStatementId(StatementId statementId) {
    return statementId == null ? null : statementId.toSQLExecStatementId();
  }

  @Override
  public boolean equals(Object otherStatement) {
    if (!(otherStatement instanceof StatementId)
        || (this.clientType != ((StatementId) otherStatement).clientType)) {
      return false;
    }
    return Objects.equals(this.guid, ((StatementId) otherStatement).guid)
        && Objects.equals(this.secret, ((StatementId) otherStatement).secret);
  }

  @Override
  public int hashCode() {
    return Objects.hash(clientType, guid, secret);
  }
}
