package com.databricks.jdbc.dbclient.impl.common;

import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.dbclient.impl.thrift.ResourceId;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.THandleIdentifier;

/** A Statement-Id identifier to uniquely identify an executed statement */
public class StatementId {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(StatementId.class);

  private final DatabricksClientType clientType;
  private final String guid;
  private final String secret;

  private StatementId(DatabricksClientType clientType, String guid, String secret) {
    this.clientType = clientType;
    this.guid = guid;
    this.secret = secret;
  }

  /** Constructs a StatementId identifier for a given SQl Exec statement-Id */
  public StatementId(String statementId) {
    this(DatabricksClientType.SQL_EXEC, statementId, null);
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
    String[] idParts = serializedStatementId.split("\\|");
    if (idParts.length == 1) {
      return new StatementId(DatabricksClientType.SQL_EXEC, serializedStatementId, null);
    } else if (idParts.length == 2) {
      return new StatementId(DatabricksClientType.THRIFT, idParts[0], idParts[1]);
    } else {
      LOGGER.error("Invalid statement-Id {%s}", serializedStatementId);
      throw new IllegalArgumentException("Invalid statement-Id " + serializedStatementId);
    }
  }

  @Override
  public String toString() {
    switch (clientType) {
      case SQL_EXEC:
        return guid;
      case THRIFT:
        return String.format("%s|%s", guid, secret);
    }
    return guid;
  }

  /** Returns a Thrift operation handle for the given StatementId */
  public THandleIdentifier toOperationIdentifier() {
    if (clientType.equals(DatabricksClientType.SQL_EXEC)) {
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
}
