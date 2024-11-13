package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.TestConstants.*;
import static com.databricks.jdbc.common.DatabricksDriverProperty.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.common.DatabricksJdbcUrlParams;
import com.databricks.jdbc.dbclient.impl.sqlexec.DatabricksMetadataSdkClient;
import com.databricks.jdbc.dbclient.impl.sqlexec.DatabricksSdkClient;
import com.databricks.jdbc.dbclient.impl.thrift.DatabricksThriftServiceClient;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.client.thrift.generated.TSessionHandle;
import java.sql.DriverPropertyInfo;
import java.util.List;
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

  private DatabricksSession createSession(String jdbcUrl) throws DatabricksSQLException {
    Properties properties = new Properties();
    return new DatabricksSession(DatabricksConnectionContext.parse(jdbcUrl, properties), sdkClient);
  }

  private void assertMissingProperties(DatabricksSession session, String... expectedProperties)
      throws DatabricksSQLException {
    List<DriverPropertyInfo> missingProperties = session.checkProperties();
    for (String prop : expectedProperties) {
      assertTrue(
          missingProperties.stream().anyMatch(p -> p.name.equals(prop)),
          "Missing property: " + prop);
    }
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
        "DatabricksSession[compute='SQL Warehouse with warehouse ID {warehouse_id}']",
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

  @Test
  public void testSetClientInfoProperty_AuthAccessToken() throws DatabricksSQLException {
    DatabricksSession session =
        new DatabricksSession(
            DatabricksConnectionContext.parse(VALID_CLUSTER_URL, new Properties()), sdkClient);
    session.setClientInfoProperty(
        DatabricksJdbcUrlParams.AUTH_ACCESS_TOKEN.getParamName(), "token");
    verify(sdkClient).resetAccessToken("token");
  }

  @Test
  public void testSetClientInfoProperty_AuthAccessTokenThrift() throws DatabricksSQLException {
    DatabricksSession session =
        new DatabricksSession(
            DatabricksConnectionContext.parse(VALID_CLUSTER_URL, new Properties()), thriftClient);
    session.setClientInfoProperty(
        DatabricksJdbcUrlParams.AUTH_ACCESS_TOKEN.getParamName(), "token");
    verify(thriftClient).resetAccessToken("token");
  }

  @Test
  public void testCheckProperties() throws DatabricksSQLException {
    String jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=3";
    Properties properties = new Properties();
    DatabricksSession session = createSession(jdbcUrl);
    assertMissingProperties(session, USER.getName(), PASSWORD.getName());

    // log-level properties
    jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=3;logLevel=DEBUG;";
    session = createSession(jdbcUrl);
    assertMissingProperties(
        session, LOG_PATH.getName(), LOG_FILE_SIZE.getName(), LOG_FILE_COUNT.getName());

    // auth-flow missing OAUTH auth mech
    jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=11";
    session = createSession(jdbcUrl);
    assertMissingProperties(session, AUTH_FLOW.getName());

    // TOKEN_PASSTHROUGH with auth-access token.
    jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=11;Auth_Flow=0";
    session = createSession(jdbcUrl);
    assertMissingProperties(session, OAUTH_REFRESH_TOKEN.getName(), AUTH_ACCESS_TOKEN.getName());

    // TOKEN_PASSTHROUGH with refresh token.
    jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=11;Auth_Flow=0;OAuthRefreshToken=token;OAuthDiscoveryMode=0";
    session = createSession(jdbcUrl);
    assertMissingProperties(
        session,
        CLIENT_ID.getName(),
        CLIENT_SECRET.getName(),
        TOKEN_ENDPOINT.getName(),
        DISCOVERY_MODE.getName(),
        DISCOVERY_URL.getName());

    // TOKEN_PASSTHROUGH with discovery mode and refresh token.
    jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=11;Auth_Flow=0;OAuthRefreshToken=token;";
    session = createSession(jdbcUrl);
    assertMissingProperties(
        session, CLIENT_ID.getName(), CLIENT_SECRET.getName(), DISCOVERY_URL.getName());

    // client credentials auth flow
    jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=11;Auth_Flow=1;";
    session = createSession(jdbcUrl);
    assertMissingProperties(
        session, CLIENT_ID.getName(), CLIENT_SECRET.getName(), USE_JWT_ASSERTION.getName());

    // client credentials auth flow with jwt assertion
    jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=11;Auth_Flow=1;UseJWTAssertion=1";
    session = createSession(jdbcUrl);
    assertMissingProperties(
        session,
        CLIENT_ID.getName(),
        CLIENT_SECRET.getName(),
        JWT_KEY_FILE.getName(),
        JWT_ALGORITHM.getName(),
        JWT_PASS_PHRASE.getName(),
        JWT_KID.getName());

    // browser-based auth flow
    jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=11;Auth_Flow=2;";
    session = createSession(jdbcUrl);
    assertMissingProperties(
        session, CLIENT_ID.getName(), CLIENT_SECRET.getName(), AUTH_SCOPE.getName());

    // proxy based connection
    jdbcUrl =
        "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=11;Auth_Flow=2;useproxy=1";
    session = createSession(jdbcUrl);
    assertMissingProperties(
        session,
        PROXY_HOST.getName(),
        PROXY_USER.getName(),
        PROXY_PWD.getName(),
        PROXY_PORT.getName());
  }
}
