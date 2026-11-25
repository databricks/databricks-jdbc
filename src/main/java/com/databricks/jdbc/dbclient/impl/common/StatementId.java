package com.databricks.jdbc.dbclient.impl.common;

import static com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR;

import com.databricks.jdbc.api.impl.ImmutableSessionInfo;
import com.databricks.jdbc.common.AllPurposeCluster;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.common.Warehouse;
import com.databricks.jdbc.dbclient.impl.sqlexec.SeaComputeTarget;
import com.databricks.jdbc.dbclient.impl.thrift.ResourceId;
import com.databricks.jdbc.exception.DatabricksDriverException;
import com.databricks.jdbc.exception.DatabricksValidationException;
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
  private final String orgId;
  private final String clusterId;

  StatementId(DatabricksClientType clientType, String guid, String secret) {
    this(clientType, guid, secret, null, null, null, null);
  }

  private StatementId(
      DatabricksClientType clientType,
      String guid,
      String secret,
      String sessionId,
      String warehouseId,
      String orgId,
      String clusterId) {
    this.clientType = clientType;
    this.guid = guid;
    this.secret = secret;
    this.sessionId = sessionId;
    this.warehouseId = warehouseId;
    this.orgId = orgId;
    this.clusterId = clusterId;
  }

  /** Constructs a StatementId identifier for a given SQl Exec statement-Id */
  public StatementId(String statementId) {
    this(DatabricksClientType.SEA, statementId, null);
  }

  public static StatementId forCurrentSession(
      String statementId, ImmutableSessionInfo sessionInfo) {
    try {
      SeaComputeTarget target = SeaComputeTarget.from(sessionInfo);
      if (target.isAllPurposeCluster()) {
        return new StatementId(
            DatabricksClientType.SEA,
            statementId,
            null,
            null,
            null,
            target.orgId(),
            target.clusterId());
      }
      if (!target.isRealtimeSession()) {
        return new StatementId(statementId);
      }
      return new StatementId(
          DatabricksClientType.SEA,
          statementId,
          null,
          sessionInfo.sessionId(),
          target.warehouseId(),
          null,
          null);
    } catch (DatabricksValidationException e) {
      throw new DatabricksDriverException(
          "Unsupported SQL Execution API compute resource", e, INPUT_VALIDATION_ERROR);
    }
  }

  public static StatementId forAllPurposeCluster(String statementId, AllPurposeCluster cluster) {
    return new StatementId(
        DatabricksClientType.SEA,
        statementId,
        null,
        null,
        null,
        cluster.getOrgId(),
        cluster.getClusterId());
  }

  public String sessionId() {
    return sessionId;
  }

  public String warehouseId() {
    return warehouseId;
  }

  /** Returns the compute target embedded in a routed statement identifier, if present. */
  public SeaComputeTarget computeTarget() throws DatabricksValidationException {
    if (orgId != null && clusterId != null) {
      return SeaComputeTarget.from(new AllPurposeCluster(orgId, clusterId));
    }
    if (warehouseId != null && sessionId != null) {
      return SeaComputeTarget.from(new Warehouse(warehouseId), true);
    }
    return null;
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
    if (serializedStatementId.startsWith("a|")) {
      String[] idParts = serializedStatementId.split("\\|", -1);
      if (idParts.length != 4
          || idParts[1].isEmpty()
          || idParts[2].isEmpty()
          || idParts[3].isEmpty()) {
        LOGGER.error("Invalid APC statement-Id structure");
        throw new DatabricksDriverException(
            "Invalid APC statement-Id structure", INPUT_VALIDATION_ERROR);
      }
      return new StatementId(
          DatabricksClientType.SEA, idParts[3], null, null, null, idParts[1], idParts[2]);
    }

    String[] idParts = serializedStatementId.split("\\|");
    if (idParts.length == 1) {
      return new StatementId(DatabricksClientType.SEA, serializedStatementId, null);
    } else if (idParts.length == 2) {
      return new StatementId(DatabricksClientType.THRIFT, idParts[0], idParts[1]);
    } else {
      LOGGER.error("Invalid statement-Id {}", serializedStatementId);
      throw new DatabricksDriverException(
          "Invalid statement-Id " + serializedStatementId, INPUT_VALIDATION_ERROR);
    }
  }

  @Override
  public String toString() {
    switch (clientType) {
      case SEA:
        if (orgId != null && clusterId != null) {
          return String.format("a|%s|%s|%s", orgId, clusterId, guid);
        }
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

  /** Returns the identifier exposed by the JDBC ResultSet API. */
  public String toPublicString() {
    return clientType == DatabricksClientType.SEA && orgId != null ? guid : toString();
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
