package com.databricks.jdbc.client.impl.thrift;

import com.databricks.jdbc.client.impl.thrift.generated.THandleIdentifier;
import com.databricks.jdbc.client.impl.thrift.generated.TSessionHandle;

import java.nio.ByteBuffer;
import java.util.UUID;

public class DatabricksThriftHelper {

    private static final byte[] SESSION_SECRET = "338d529d-8272-46eb-8482-cb419466839d".getBytes();
    public static TSessionHandle getSessionHandle(String sessionId){
        THandleIdentifier identifier =
                new THandleIdentifier().setGuid(sessionId.getBytes()).setSecret(SESSION_SECRET);
        return new TSessionHandle().setSessionId(identifier);
    }

    public static String byteBufferToString(ByteBuffer buffer) {
        long sigBits = buffer.getLong();
        return new UUID(sigBits, sigBits).toString();
    }
}
