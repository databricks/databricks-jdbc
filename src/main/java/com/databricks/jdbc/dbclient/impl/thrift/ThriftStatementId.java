package com.databricks.jdbc.dbclient.impl.thrift;

import com.databricks.jdbc.model.client.thrift.generated.THandleIdentifier;
import com.databricks.jdbc.model.client.thrift.generated.TOperationHandle;
import com.databricks.jdbc.model.client.thrift.generated.TOperationType;
import java.nio.ByteBuffer;

class ThriftStatementId {
  ResourceId guid;
  ResourceId secret;

  private ThriftStatementId(ResourceId guid, ResourceId secret) {
    this.guid = guid;
    this.secret = secret;
  }

  static ThriftStatementId fromOperationHandle(TOperationHandle operationHandle) {
    ResourceId guid = ResourceId.fromBytes(operationHandle.getOperationId().getGuid());
    ResourceId secret = ResourceId.fromBytes(operationHandle.getOperationId().getSecret());
    return new ThriftStatementId(guid, secret);
  }

  static ThriftStatementId fromBase64String(String statementId) {
    String[] parts = statementId.split("\\|");
    ResourceId guid = ResourceId.fromBase64(parts[0]);
    ResourceId secret = ResourceId.fromBase64(parts[1]);
    return new ThriftStatementId(guid, secret);
  }

  @Override
  public String toString() {
    return String.format("%s|%s", guid.toString(), secret.toString());
  }

  THandleIdentifier toHandleIdentifier() {
    byte[] publicId = new byte[16];
    byte[] secretId = new byte[16];
    ByteBuffer publicIdBB = ByteBuffer.wrap(publicId);
    ByteBuffer secretIdBB = ByteBuffer.wrap(secretId);
    publicIdBB.putLong(guid.getId().getMostSignificantBits());
    publicIdBB.putLong(guid.getId().getLeastSignificantBits());
    secretIdBB.putLong(secret.getId().getMostSignificantBits());
    secretIdBB.putLong(secret.getId().getLeastSignificantBits());

    return new THandleIdentifier(ByteBuffer.wrap(publicId), ByteBuffer.wrap(secretId));
  }

  TOperationHandle toOperationHandle() {
    return new TOperationHandle()
        .setOperationId(toHandleIdentifier())
        .setOperationType(TOperationType.UNKNOWN);
  }
}
