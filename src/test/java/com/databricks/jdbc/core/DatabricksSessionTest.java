package com.databricks.jdbc.core;

import static com.databricks.jdbc.TestConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.client.DatabricksClientType;
import com.databricks.jdbc.client.impl.sdk.DatabricksMetadataSdkClient;
import com.databricks.jdbc.client.impl.sdk.DatabricksSdkClient;
import com.databricks.jdbc.client.impl.thrift.DatabricksThriftServiceClient;
import com.databricks.jdbc.client.impl.thrift.generated.*;
import com.databricks.jdbc.driver.DatabricksConnectionContext;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksSessionTest {
  @Mock DatabricksSdkClient sdkClient;
  @Mock DatabricksThriftServiceClient thriftClient;
  @Mock TSessionHandle tSessionHandle;
  private static final String JDBC_URL_INVALID =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehou/erg6767gg;";
  private static final String WAREHOUSE_ID = "erg6767gg";
  private static final String NEW_CATALOG = "new_catalog";
  private static final String NEW_SCHEMA = "new_schema";
  private static final String SESSION_ID = "session_id";
  private static final String VALID_CLUSTER_URL =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=3;conncatalog=field_demos;connschema=ossjdbc";
  private static IDatabricksConnectionContext connectionContext;

  static void setupWarehouse(boolean useThrift) throws DatabricksSQLException {
    String url = useThrift ? WAREHOUSE_JDBC_URL_WITH_THRIFT : WAREHOUSE_JDBC_URL;
    connectionContext = DatabricksConnectionContext.parse(url, new Properties());
  }

  private void setupCluster() throws DatabricksSQLException {
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
  public void testOpenAndCloseSession() throws DatabricksSQLException {
    setupWarehouse(false);
    ImmutableSessionInfo sessionInfo =
        ImmutableSessionInfo.builder()
            .sessionId(SESSION_ID)
            .computeResource(WAREHOUSE_COMPUTE)
            .build();
    when(sdkClient.createSession(eq(WAREHOUSE_COMPUTE), any(), any(), any()))
        .thenReturn(sessionInfo);
    DatabricksSession session = new DatabricksSession(connectionContext, sdkClient);
    assertEquals(DatabricksClientType.SQL_EXEC, connectionContext.getClientType());
    assertFalse(session.isOpen());
    session.open();
    assertTrue(session.isOpen());
    assertEquals(SESSION_ID, session.getSessionId());
    assertTrue(session.getDatabricksMetadataClient() instanceof DatabricksMetadataSdkClient);
    assertEquals(WAREHOUSE_COMPUTE, session.getComputeResource());
    session.close();
    assertFalse(session.isOpen());
    assertNull(session.getSessionId());
  }

  @Test
  public void testOpenAndCloseSessionUsingThrift() throws DatabricksSQLException {
    setupWarehouse(true);
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
  public void testOpenAndCloseSessionForAllPurposeCluster() throws DatabricksSQLException {
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
  public void testSessionConstructorForWarehouse() throws DatabricksSQLException {
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
  public void testCatalogAndSchema() throws DatabricksSQLException {
    setupWarehouse(false);
    DatabricksSession session = new DatabricksSession(connectionContext);
    session.setCatalog(NEW_CATALOG);
    assertEquals(NEW_CATALOG, session.getCatalog());
    session.setSchema(NEW_SCHEMA);
    assertEquals(NEW_SCHEMA, session.getSchema());
    assertEquals(connectionContext, session.getConnectionContext());
  }

  @Test
  public void testSessionToString() throws DatabricksSQLException {
    setupWarehouse(false);
    DatabricksSession session = new DatabricksSession(connectionContext);
    assertEquals(
        "DatabricksSession[compute='SQL Warehouse with warehouse ID {warehouse_id}', catalog='SPARK', schema='default']",
        session.toString());
  }

  @Test
  public void testSetClientInfoProperty() throws DatabricksSQLException {
    DatabricksSession session =
        new DatabricksSession(
            DatabricksConnectionContext.parse(VALID_CLUSTER_URL, new Properties()), sdkClient);
    session.setClientInfoProperty("key", "value");
    assertEquals("value", session.getClientInfoProperties().get("key"));
  }
}
