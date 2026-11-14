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
  public void testRealtimeSessionRetainsRouteForConnectionClose() throws Exception {
    ImmutableSessionInfo sessionInfo =
        ImmutableSessionInfo.builder()
            .sessionId("test-session-id")
            .computeResource(new Warehouse(WAREHOUSE_ID))
            .realtimeSession(true)
            .build();

    SessionId sessionId = SessionId.create(sessionInfo);
    assertEquals("s|warehouse|test-session-id|r", sessionId.toString());
    assertEquals(sessionId, new SessionId("test-session-id", new Warehouse(WAREHOUSE_ID), true));
    assertTrue(SessionId.deserialize(sessionId.toString()).getSessionInfo().realtimeSession());
    assertFalse(
        SessionId.deserialize("s|warehouse|test-session-id").getSessionInfo().realtimeSession());
  }

  @Test
  public void testLegacyWarehouseIdAStillParsesWithBothRoutes() throws Exception {
    for (String serialized : new String[] {"s|a|session", "s|a|session|r"}) {
      SessionId parsed = SessionId.deserialize(serialized);
      assertEquals(serialized, parsed.toString());
      assertEquals(DatabricksClientType.SEA, parsed.getClientType());
      assertEquals(new Warehouse("a"), parsed.getSessionInfo().computeResource());
      assertEquals(serialized.endsWith("|r"), parsed.getSessionInfo().realtimeSession());
    }
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
    assertThrows(DatabricksParsingException.class, () -> SessionId.deserialize("s|warehouse|"));
    assertThrows(DatabricksParsingException.class, () -> SessionId.deserialize("t|session-id|"));
    final String sessionId = "q|warehouse|test-session-id";
    assertThrows(DatabricksParsingException.class, () -> SessionId.deserialize(sessionId));

    final String sessionId1 = "s|warehouse|test-session-id|invalid";
    assertThrows(DatabricksParsingException.class, () -> SessionId.deserialize(sessionId1));

    final String sessionId2 = "t|invalid";
    assertThrows(DatabricksParsingException.class, () -> SessionId.deserialize(sessionId2));

    final String sessionId3 = "t|test-session-id|invalid|part3";
    assertThrows(DatabricksParsingException.class, () -> SessionId.deserialize(sessionId3));
  }

  @Test
  public void testApcSeaRoundTripAndSavedVersionCannotGrantCapabilities() throws Exception {
    for (Long version : new Long[] {8L, null}) {
      ImmutableSessionInfo live =
          ImmutableSessionInfo.builder()
              .clientType(DatabricksClientType.SEA)
              .sessionId("session")
              .computeResource(new AllPurposeCluster("org", "cluster"))
              .negotiatedProtocolVersion(version)
              .liveProtocolVersion(true)
              .build();
      String serialized = SessionId.create(live).toString();
      assertEquals("s|a|org|cluster|session|" + (version == null ? "-" : version), serialized);
      ImmutableSessionInfo saved = SessionId.deserialize(serialized).getSessionInfo();
      assertEquals(DatabricksClientType.SEA, saved.clientType());
      assertEquals(live.computeResource(), saved.computeResource());
      assertEquals(version, saved.negotiatedProtocolVersion());
      assertNull(saved.nativeProtocolVersion());
      assertFalse(saved.liveProtocolVersion());
      assertEquals(version, live.nativeProtocolVersion());
    }
    assertNull(
        SessionId.deserialize("s|a|org|cluster|session|99")
            .getSessionInfo()
            .nativeProtocolVersion());
  }

  @Test
  public void testMalformedApcSessionIdsReportOnlyStructure() {
    for (String input :
        new String[] {
          "s|a|org|cluster",
          "s|a|org|cluster|session",
          "s|a|org|cluster|session|-|extra",
          "s|a||cluster|session|-",
          "s|a|org|cluster||-",
          "s|a|org|cluster|session|invalid",
          "s|a|org|cluster|session|-1"
        }) {
      DatabricksParsingException error =
          assertThrows(DatabricksParsingException.class, () -> SessionId.deserialize(input));
      assertEquals("SESSION_ID_PARSING_EXCEPTION", error.getSQLState());
      assertEquals(1029, error.getErrorCode());
      assertFalse(error.getMessage().contains(input));
      assertFalse(error.getMessage().contains("org"));
      assertFalse(error.getMessage().contains("cluster"));
    }
  }

  @Test
  public void testWarehouseThriftSessionDispatchesByClientType() {
    THandleIdentifier handle =
        new THandleIdentifier().setGuid(testGuidBytes).setSecret(testSecretBytes);
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .clientType(DatabricksClientType.THRIFT)
            .sessionId("thrift")
            .computeResource(new Warehouse(WAREHOUSE_ID))
            .sessionHandle(new TSessionHandle().setSessionId(handle))
            .build();
    assertEquals(DatabricksClientType.THRIFT, SessionId.create(info).getClientType());
  }
}
