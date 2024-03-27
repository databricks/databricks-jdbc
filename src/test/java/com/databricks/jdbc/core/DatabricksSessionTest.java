package com.databricks.jdbc.core;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.client.impl.sdk.DatabricksSdkClient;
import com.databricks.jdbc.client.impl.thrift.DatabricksThriftClient;
import com.databricks.jdbc.core.types.AllPurposeCluster;
import com.databricks.jdbc.core.types.ComputeResource;
import com.databricks.jdbc.core.types.Warehouse;
import com.databricks.jdbc.driver.DatabricksConnectionContext;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksSessionTest {

  private static final String JDBC_URL =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;";
  private static final String JDBC_URL_INVALID =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehou/erg6767gg;";

  private static final String WAREHOUSE_ID = "erg6767gg";
  private static final String CATALOG = "field_demos";

  private static final String NEW_CATALOG = "new_catalog";
  private static final String NEW_SCHEMA = "new_schema";
  private static final String SCHEMA = "ossjdbc";
  private static final String SESSION_ID = "session_id";
  private static final ComputeResource WAREHOUSE_COMPUTE = new Warehouse(WAREHOUSE_ID);
  private static final ComputeResource CLUSTER_COMPUTE =
      new AllPurposeCluster("6051921418418893", "1115-130834-ms4m0yv");
  private static final Map<String, String> SESSION_CONFIGS =
      Map.of("spark.sql.crossJoin.enabled", "true", "SSP_databricks.catalog", "field_demos");
  private static final String VALID_CLUSTER_URL =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=3";
  private static final String VALID_WAREHOUSE_URL =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;LogLevel=debug;LogPath=test1/application.log;";

  @Mock DatabricksConnectionContext connectionContext;

  @Mock DatabricksSdkClient sdkClient;
  @Mock DatabricksThriftClient thriftClient;

  @Test
  public void testOpenAndCloseSession() throws DatabricksSQLException {
    ImmutableSessionInfo sessionInfo =
        ImmutableSessionInfo.builder()
            .sessionId(SESSION_ID)
            .computeResource(WAREHOUSE_COMPUTE)
            .build();
    when(sdkClient.createSession(eq(WAREHOUSE_COMPUTE), any(), any(), any()))
        .thenReturn(sessionInfo);
    when(connectionContext.getComputeResource()).thenReturn(WAREHOUSE_COMPUTE);
    when(connectionContext.getCatalog()).thenReturn(CATALOG);
    when(connectionContext.getSchema()).thenReturn(SCHEMA);
    DatabricksSession session = new DatabricksSession(connectionContext, sdkClient);
    assertFalse(session.isOpen());
    session.open();
    assertTrue(session.isOpen());
    assertEquals(SESSION_ID, session.getSessionId());
    assertEquals(WAREHOUSE_COMPUTE, session.getComputeResource());
    doNothing().when(sdkClient).deleteSession(SESSION_ID, WAREHOUSE_COMPUTE);
    session.close();
    assertFalse(session.isOpen());
    assertNull(session.getSessionId());
  }

  @Test
  public void testOpenSessionForAllPurposeCluster() throws DatabricksSQLException {
    ImmutableSessionInfo sessionInfo =
        ImmutableSessionInfo.builder()
            .sessionId(SESSION_ID)
            .computeResource(CLUSTER_COMPUTE)
            .build();
    when(thriftClient.createSession(eq(CLUSTER_COMPUTE), any(), any(), any()))
        .thenReturn(sessionInfo);
    when(connectionContext.getComputeResource()).thenReturn(CLUSTER_COMPUTE);
    when(connectionContext.getCatalog()).thenReturn(CATALOG);
    when(connectionContext.getSchema()).thenReturn(SCHEMA);
    DatabricksSession session = new DatabricksSession(connectionContext, thriftClient);
    assertFalse(session.isOpen());
    session.open();
    assertTrue(session.isOpen());
    assertEquals(SESSION_ID, session.getSessionId());
    session.close();
    assertFalse(session.isOpen());
    assertNull(session.getSessionId());
  }

  @Test
  public void testSessionConstructorForAllPurposeCluster() throws DatabricksSQLException {
    DatabricksSession session =
        new DatabricksSession(
            DatabricksConnectionContext.parse(VALID_CLUSTER_URL, new Properties()));
    assertFalse(session.isOpen());
  }

  @Test
  public void testSessionConstructorForWarehouse() throws DatabricksSQLException {
    DatabricksSession session =
        new DatabricksSession(
            DatabricksConnectionContext.parse(VALID_WAREHOUSE_URL, new Properties()));
    assertFalse(session.isOpen());
  }

  @Test
  public void testOpenSession_invalidWarehouseUrl() {
    assertThrows(
        DatabricksParsingException.class,
        () ->
            new DatabricksSession(
                DatabricksConnectionContext.parse(JDBC_URL_INVALID, new Properties()), null));
  }

  @Test
  public void testCatalogAndSchema() throws DatabricksSQLException {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(VALID_WAREHOUSE_URL, new Properties());
    DatabricksSession session = new DatabricksSession(connectionContext);
    session.setCatalog(NEW_CATALOG);
    assertEquals(NEW_CATALOG, session.getCatalog());
    session.setSchema(NEW_SCHEMA);
    assertEquals(NEW_SCHEMA, session.getSchema());
    assertEquals(connectionContext, session.getConnectionContext());
  }

  @Test
  public void testSessionToString() throws DatabricksSQLException {
    DatabricksSession session =
        new DatabricksSession(
            DatabricksConnectionContext.parse(VALID_WAREHOUSE_URL, new Properties()));
    assertEquals(
        "DatabricksSession[compute='SQL Warehouse with warehouse ID {erg6767gg}', catalog='SPARK', schema='default']",
        session.toString());
  }
}
