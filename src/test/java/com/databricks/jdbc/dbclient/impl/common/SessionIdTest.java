package com.databricks.jdbc.dbclient.impl.common;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.impl.ImmutableSessionInfo;
import com.databricks.jdbc.common.AllPurposeCluster;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.common.Warehouse;
import com.databricks.jdbc.common.util.DatabricksThriftUtil;
import com.databricks.jdbc.dbclient.impl.thrift.ResourceId;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.model.client.thrift.generated.THandleIdentifier;
import com.databricks.jdbc.model.client.thrift.generated.TSessionHandle;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

public class SessionIdTest {

  private static final String WAREHOUSE_ID = "warehouse";
  private static final String CLUSTER_ID = "cluster";
  private static final byte[] testGuidBytes =
      new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
  private static final byte[] testSecretBytes =
      new byte[] {17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32};

  /** Test the constructor with a SQL Exec statement ID string. */
  @Test
  public void testConstructorSEA() throws Exception {
    String sessionIdString = "test-session-id";
    ImmutableSessionInfo sessionInfo =
        ImmutableSessionInfo.builder()
            .sessionId(sessionIdString)
            .computeResource(new Warehouse(WAREHOUSE_ID))
            .sessionHandle(null)
            .build();
    SessionId sessionId = SessionId.create(sessionInfo);

    String expected = "s|warehouse|test-session-id";
    assertEquals(expected, sessionId.toString());
    assertEquals(DatabricksClientType.SEA, sessionId.getClientType());

    SessionId deserializedSessionId = SessionId.deserialize(expected);
    ImmutableSessionInfo deserializedSessionInfo = deserializedSessionId.getSessionInfo();
    assertEquals(sessionId, deserializedSessionId);
    assertEquals(sessionInfo.sessionId(), deserializedSessionInfo.sessionId());
    assertEquals(sessionInfo.sessionHandle(), deserializedSessionInfo.sessionHandle());
    assertEquals(sessionInfo.computeResource(), deserializedSessionInfo.computeResource());
  }

  @Test
  public void testConstructorThrift() throws Exception {
    THandleIdentifier tHandleIdentifier =
        new THandleIdentifier().setGuid(testGuidBytes).setSecret(testSecretBytes);
    ImmutableSessionInfo sessionInfo =
        ImmutableSessionInfo.builder()
            .sessionHandle(new TSessionHandle().setSessionId(tHandleIdentifier))
            .sessionId(DatabricksThriftUtil.byteBufferToString(ByteBuffer.wrap(testGuidBytes)))
            .computeResource(new AllPurposeCluster("", CLUSTER_ID))
            .build();

    String expectedGuid = ResourceId.fromBytes(testGuidBytes).toString();
    String expectedSecret = ResourceId.fromBytes(testSecretBytes).toString();
    SessionId sessionId = SessionId.create(sessionInfo);
    String expected = String.format("t|%s|%s", expectedGuid, expectedSecret);
    assertEquals(expected, sessionId.toString());
    assertEquals(DatabricksClientType.THRIFT, sessionId.getClientType());

    SessionId deserializedSessionId = SessionId.deserialize(expected);
    ImmutableSessionInfo deserializedSessionInfo = deserializedSessionId.getSessionInfo();
    assertEquals(sessionId, deserializedSessionId);
    assertEquals(sessionInfo.sessionId(), deserializedSessionInfo.sessionId());
    assertEquals(sessionInfo.sessionHandle(), deserializedSessionInfo.sessionHandle());
  }

  @Test
  public void testInvalidSessionId() throws Exception {
    final String sessionId = "q|warehouse|test-session-id";
    assertThrows(DatabricksParsingException.class, () -> SessionId.deserialize(sessionId));

    final String sessionId1 = "s|warehouse|test-session-id|invalid";
    assertThrows(DatabricksParsingException.class, () -> SessionId.deserialize(sessionId1));

    final String sessionId2 = "t|invalid";
    assertThrows(DatabricksParsingException.class, () -> SessionId.deserialize(sessionId2));

    final String sessionId3 = "t|test-session-id|invalid|part3";
    assertThrows(DatabricksParsingException.class, () -> SessionId.deserialize(sessionId3));
  }
}
