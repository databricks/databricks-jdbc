package com.databricks.jdbc.client.impl.thrift.commons;

import com.databricks.jdbc.client.impl.thrift.generated.THandleIdentifier;
import com.databricks.jdbc.client.impl.thrift.generated.TProtocolVersion;
import com.databricks.jdbc.client.impl.thrift.generated.TSessionHandle;
import com.databricks.jdbc.core.IDatabricksSession;

import java.nio.ByteBuffer;
import java.util.UUID;

public class DatabricksThriftHelper {
  public static final TProtocolVersion PROTOCOL = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10;

  public static TSessionHandle getSessionHandle(IDatabricksSession session) {
    THandleIdentifier identifier =
        new THandleIdentifier().setGuid(session.getSessionId().getBytes()).setSecret(session.getSecret());
    return new TSessionHandle().setSessionId(identifier).setServerProtocolVersion(PROTOCOL);
  }

  public static String byteBufferToString(ByteBuffer buffer) {
    long sigBits = buffer.getLong();
    return new UUID(sigBits, sigBits).toString();
  }
}
