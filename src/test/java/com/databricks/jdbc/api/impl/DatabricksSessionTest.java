package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.TestConstants.*;
import static com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode.TEMPORARY_REDIRECT_EXCEPTION;
import static com.databricks.jdbc.telemetry.TelemetryHelper.getDriverSystemConfiguration;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.common.DatabricksJdbcUrlParams;
import com.databricks.jdbc.dbclient.impl.sqlexec.DatabricksMetadataSdkClient;
import com.databricks.jdbc.dbclient.impl.sqlexec.DatabricksSdkClient;
import com.databricks.jdbc.dbclient.impl.thrift.DatabricksThriftServiceClient;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksTemporaryRedirectException;
import com.databricks.jdbc.model.client.thrift.generated.TSessionHandle;
import com.databricks.jdbc.telemetry.latency.DatabricksMetricsTimedProcessor;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksSessionTest {
  @Mock DatabricksSdkClient sdkClient;
  @Mock DatabricksThriftServiceClient thriftClient;
  @Mock TSessionHandle tSessionHandle;
  private static final String JDBC_URL_INVALID =
      "jdbc:databricks://sample-host.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehou/999999999;";
  private static final String NEW_CATALOG = "new_catalog";
  private static final String NEW_SCHEMA = "new_schema";
  private static final String SESSION_ID = "session_id";
  private static final String VALID_CLUSTER_URL =
      "jdbc:databricks://sample-host.cloud.databricks.com:443/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/o/9999999999999999/9999999999999999999;AuthMech=3;conncatalog=field_demos;connschema=ossjdbc";
  private static IDatabricksConnectionContext connectionContext;

  static void setupWarehouse(boolean useThrift) throws SQLException {
    String url = useThrift ? WAREHOUSE_JDBC_URL : WAREHOUSE_JDBC_URL_WITH_SEA;
    connectionContext = DatabricksConnectionContext.parse(url, new Properties());
  }

  private void setupCluster() throws SQLException {
    connectionContext = DatabricksConnectionContext.parse(VALID_CLUSTER_URL, new Properties());
    ImmutableSessionInfo sessionInfo =
        ImmutableSessionInfo.builder()
            .sessionHandle(tSessionHandle)
            .sessionId(SESSION_ID)
            .computeResource(CLUSTER_COMPUTE)
            .build();
    when(thriftClient.createSession(any(), any(), any(), any())).thenReturn(sessionInfo);
  }

  @Test
  public void testOpenAndCloseSession() throws SQLException {
    setupWarehouse(true /* useThrift */);
    ImmutableSessionInfo sessionInfo =
        ImmutableSessionInfo.builder()
            .sessionId(SESSION_ID)
            .computeResource(WAREHOUSE_COMPUTE)
            .build();
    when(thriftClient.createSession(any(), any(), any(), any())).thenReturn(sessionInfo);
    DatabricksSession session = new DatabricksSession(connectionContext, thriftClient);
    assertEquals(DatabricksClientType.THRIFT, connectionContext.getClientType());
    assertFalse(session.isOpen());
    session.open();
    assertTrue(session.isOpen());
    assertEquals(SESSION_ID, session.getSessionId());
    assertInstanceOf(DatabricksThriftServiceClient.class, session.getDatabricksMetadataClient());
    assertEquals(WAREHOUSE_COMPUTE, session.getComputeResource());
    session.close();
    assertFalse(session.isOpen());
    assertNull(session.getSessionId());
  }

  @Test
  public void testOpenRedirectedThriftSession() throws SQLException {
    setupWarehouse(false /* useThrift */);
    ImmutableSessionInfo sessionInfo =
        ImmutableSessionInfo.builder()
            .sessionId(SESSION_ID)
            .computeResource(WAREHOUSE_COMPUTE)
            .build();
    when(sdkClient.createSession(eq(WAREHOUSE_COMPUTE), any(), any(), any()))
        .thenThrow(new DatabricksTemporaryRedirectException(TEMPORARY_REDIRECT_EXCEPTION));
    when(thriftClient.createSession(any(), any(), any(), any())).thenReturn(sessionInfo);
    try (MockedStatic<DatabricksMetricsTimedProcessor> proxyMock =
        Mockito.mockStatic(DatabricksMetricsTimedProcessor.class)) {
      proxyMock
          .when(() -> DatabricksMetricsTimedProcessor.createProxy(any()))
          .thenReturn(thriftClient);

      DatabricksSession session = new DatabricksSession(connectionContext, sdkClient);
      assertEquals(DatabricksClientType.SEA, connectionContext.getClientType());
      assertInstanceOf(DatabricksMetadataSdkClient.class, session.getDatabricksMetadataClient());
      assertFalse(session.isOpen());

      session.open();

      assertTrue(session.isOpen());
      assertEquals(SESSION_ID, session.getSessionId());
      assertEquals(DatabricksClientType.THRIFT, connectionContext.getClientType());
      assertInstanceOf(DatabricksThriftServiceClient.class, session.getDatabricksClient());
      assertInstanceOf(DatabricksThriftServiceClient.class, session.getDatabricksMetadataClient());
      assertEquals(WAREHOUSE_COMPUTE, session.getComputeResource());

      session.close();
      assertFalse(session.isOpen());
      assertNull(session.getSessionId());
    }
  }

  @Test
  public void testOpenAndCloseSessionUsingThrift() throws SQLException {
    setupWarehouse(true /* useThrift */);
    ImmutableSessionInfo sessionInfo =
        ImmutableSessionInfo.builder()
            .sessionHandle(tSessionHandle)
            .sessionId(SESSION_ID)
            .computeResource(WAREHOUSE_COMPUTE)
            .build();
    when(thriftClient.createSession(any(), any(), any(), any())).thenReturn(sessionInfo);
    DatabricksSession session = new DatabricksSession(connectionContext, thriftClient);
    assertEquals(DatabricksClientType.THRIFT, connectionContext.getClientType());
    assertFalse(session.isOpen());
    session.open();
    assertTrue(session.isOpen());
    assertEquals(SESSION_ID, session.getSessionId());
    assertEquals(tSessionHandle, session.getSessionInfo().sessionHandle());
    assertEquals(thriftClient, session.getDatabricksMetadataClient());
    assertEquals(WAREHOUSE_COMPUTE, session.getComputeResource());
    session.close();
    assertFalse(session.isOpen());
    assertNull(session.getSessionId());
  }

  @Test
  public void testOpenAndCloseSessionForAllPurposeCluster() throws SQLException {
    setupCluster();
    DatabricksSession session = new DatabricksSession(connectionContext, thriftClient);
    assertFalse(session.isOpen());
    session.open();
    assertTrue(session.isOpen());
    assertEquals(SESSION_ID, session.getSessionId());
    assertEquals(tSessionHandle, session.getSessionInfo().sessionHandle());
    assertEquals(thriftClient, session.getDatabricksMetadataClient());
    session.close();
    assertFalse(session.isOpen());
    assertNull(session.getSessionId());
  }

  @Test
  public void testCloseIgnoresInvalidSession() throws SQLException {
    setupWarehouse(true /* useThrift */);

    ImmutableSessionInfo sessionInfo =
        ImmutableSessionInfo.builder()
            .sessionId(SESSION_ID)
            .computeResource(WAREHOUSE_COMPUTE)
            .build();
    when(thriftClient.createSession(any(), any(), any(), any())).thenReturn(sessionInfo);

    DatabricksSQLException invalidStateEx = Mockito.mock(DatabricksSQLException.class);
    when(invalidStateEx.getMessage()).thenReturn("INVALID_STATE: Invalid SessionHandle");
    doThrow(invalidStateEx).when(thriftClient).deleteSession(any());

    DatabricksSession session = new DatabricksSession(connectionContext, thriftClient);
    session.open();
    assertTrue(session.isOpen());

    assertDoesNotThrow(session::close);

    assertFalse(session.isOpen());
    assertNull(session.getSessionId());

    verify(thriftClient).deleteSession(any());
  }

  @Test
  public void testSessionConstructorForWarehouse() throws SQLException {
    DatabricksSession session =
        new DatabricksSession(
            DatabricksConnectionContext.parse(WAREHOUSE_JDBC_URL, new Properties()));
    assertFalse(session.isOpen());
  }

  @Test
  public void testOpenSession_invalidWarehouseUrl() {
    assertThrows(
        DatabricksParsingException.class,
        () ->
            new DatabricksSession(
                DatabricksConnectionContext.parse(JDBC_URL_INVALID, new Properties())));
  }

  @Test
  public void testCatalogAndSchema() throws SQLException {
    setupWarehouse(false /* useThrift */);
    DatabricksSession session = new DatabricksSession(connectionContext);
    session.setCatalog(NEW_CATALOG);
    assertEquals(NEW_CATALOG, session.getCatalog());
    session.setSchema(NEW_SCHEMA);
    assertEquals(NEW_SCHEMA, session.getSchema());
    assertEquals(connectionContext, session.getConnectionContext());
  }

  @Test
  public void testSessionToString() throws SQLException {
    setupWarehouse(false /* useThrift */);
    DatabricksSession session = new DatabricksSession(connectionContext);
    assertEquals(
        "DatabricksSession[compute='SQL Warehouse with warehouse ID {warehouse_id}', schema='default']",
        session.toString());
  }

  @Test
  public void testSetClientInfoProperty() throws SQLException {
    DatabricksSession session =
        new DatabricksSession(
            DatabricksConnectionContext.parse(VALID_CLUSTER_URL, new Properties()), sdkClient);
    session.setClientInfoProperty("key", "value");
    assertEquals("value", session.getClientInfoProperties().get("key"));
  }

  @Test
  public void testGetClientInfoProperty_ApplicationName() throws SQLException {
    DatabricksSession session =
        new DatabricksSession(
            DatabricksConnectionContext.parse(VALID_CLUSTER_URL, new Properties()), sdkClient);
    session.setClientInfoProperty("applicationName", "testApp");
    assertEquals("testApp", session.getClientInfoProperties().get("applicationName"));
    assertEquals("testApp", getDriverSystemConfiguration().getClientAppName());
  }

  @Test
  public void testSetClientInfoProperty_AuthAccessToken() throws SQLException {
    DatabricksSession session =
        new DatabricksSession(
            DatabricksConnectionContext.parse(VALID_CLUSTER_URL, new Properties()), sdkClient);
    session.setClientInfoProperty(
        DatabricksJdbcUrlParams.AUTH_ACCESS_TOKEN.getParamName(), "token");
    verify(sdkClient).resetAccessToken("token");
  }

  @Test
  public void testSetClientInfoProperty_AuthAccessTokenThrift() throws SQLException {
    DatabricksSession session =
        new DatabricksSession(
            DatabricksConnectionContext.parse(VALID_CLUSTER_URL, new Properties()), thriftClient);
    session.setClientInfoProperty(
        DatabricksJdbcUrlParams.AUTH_ACCESS_TOKEN.getParamName(), "token");
    verify(thriftClient).resetAccessToken("token");
  }

  // ===== Lazy Client Type Integration Tests =====

  @Test
  public void testSessionOpensWithLazyClientType() throws SQLException {
    // Create connection context with conditions for SEA client
    String url =
        "jdbc:databricks://sample-host.18.azuredatabricks.net:9999/default;AuthMech=3;"
            + "httpPath=/sql/1.0/warehouses/warehouse_id;UseThriftClient=0";
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(url, new Properties());

    // Create session (should not evaluate client type yet)
    DatabricksSession session = new DatabricksSession(connectionContext);

    // Client type should be SEA when accessed
    assertEquals(DatabricksClientType.SEA, connectionContext.getClientType());
  }

  @Test
  public void testSessionOpensWithLazyClientTypeForCluster() throws SQLException {
    // Create connection context for all-purpose cluster
    String url =
        "jdbc:databricks://sample-host.cloud.databricks.com:9999/default;AuthMech=3;"
            + "httpPath=sql/protocolv1/o/9999999999999999/9999999999999999999";
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(url, new Properties());

    // Create session
    DatabricksSession session = new DatabricksSession(connectionContext);

    // Client type should be THRIFT for all-purpose cluster
    assertEquals(DatabricksClientType.THRIFT, connectionContext.getClientType());
  }
}
