package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.api.impl.DatabricksConnectionContext.getLogLevel;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.TestConstants;
import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.common.LogLevel;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.sdk.core.ProxyConfig;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class DatabricksConnectionContextTest {

  private static final Properties properties = new Properties();
  private static final Properties properties_with_pwd = new Properties();

  @BeforeAll
  public static void setUp() {
    properties.setProperty("password", "passwd");
    properties_with_pwd.setProperty("pwd", "passwd2");
  }

  @Test
  public void testParseInvalid() {
    assertThrows(
        DatabricksParsingException.class,
        () -> DatabricksConnectionContext.parse(TestConstants.INVALID_URL_1, properties));
    assertThrows(
        DatabricksParsingException.class,
        () -> DatabricksConnectionContext.parse(TestConstants.INVALID_URL_2, properties));
  }

  @Test
  public void testParseValid() throws DatabricksSQLException {
    // test provided port
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_1, properties);
    assertEquals(
        "https://adb-565757575.18.azuredatabricks.net:4423", connectionContext.getHostUrl());
    assertEquals(TestConstants.VALID_URL_1, connectionContext.getConnectionURL());
    assertEquals("/sql/1.0/warehouses/erg6767gg", connectionContext.getHttpPath());
    assertEquals("passwd", connectionContext.getToken());
    assertTrue(connectionContext.isOAuthDiscoveryModeEnabled());
    assertFalse(connectionContext.useJWTAssertion());
    assertEquals(
        connectionContext.getAuthFlow(),
        IDatabricksConnectionContext.AuthFlow.BROWSER_BASED_AUTHENTICATION);
    assertEquals(7, connectionContext.parameters.size());
    assertEquals(CompressionCodec.LZ4_FRAME, connectionContext.getCompressionCodec());
    assertEquals(LogLevel.DEBUG, connectionContext.getLogLevel());
    assertNull(connectionContext.getClientSecret());
    assertEquals("./test1", connectionContext.getLogPathString());
    assertNull(connectionContext.getOAuthScopesForU2M());
    assertFalse(connectionContext.isAllPurposeCluster());
    assertEquals(DatabricksClientType.SQL_EXEC, connectionContext.getClientType());

    // test default port
    connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_2, properties_with_pwd);
    assertEquals("https://adb-565656.azuredatabricks.net:443", connectionContext.getHostUrl());
    assertEquals("/sql/1.0/warehouses/fgff575757", connectionContext.getHttpPath());
    assertEquals("passwd2", connectionContext.getToken());
    assertEquals("96eecda7-19ea-49cc-abb5-240097d554f5", connectionContext.getClientId());
    assertEquals(7, connectionContext.parameters.size());
    assertEquals(CompressionCodec.LZ4_FRAME, connectionContext.getCompressionCodec());
    assertEquals(LogLevel.OFF, connectionContext.getLogLevel());
    assertEquals(System.getProperty("user.dir"), connectionContext.getLogPathString());
    assertEquals("3", connectionContext.parameters.get("authmech"));
    assertNull(connectionContext.getOAuthScopesForU2M());
    assertFalse(connectionContext.isAllPurposeCluster());
    assertEquals(DatabricksClientType.SQL_EXEC, connectionContext.getClientType());

    // test aws port
    connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_3, properties);
    List<String> expected_scopes = Arrays.asList("sql", "offline_access");
    assertEquals(
        "http://e2-dogfood.staging.cloud.databricks.com:443", connectionContext.getHostUrl());
    assertEquals("/sql/1.0/warehouses/5c89f447c476a5a8", connectionContext.getHttpPath());
    assertEquals("passwd", connectionContext.getToken());
    assertEquals("databricks-sql-jdbc", connectionContext.getClientId());
    assertEquals("e2-dogfood.staging.cloud.databricks.com", connectionContext.getHostForOAuth());
    assertEquals(
        IDatabricksConnectionContext.AuthFlow.TOKEN_PASSTHROUGH, connectionContext.getAuthFlow());
    assertEquals(IDatabricksConnectionContext.AuthMech.PAT, connectionContext.getAuthMech());
    assertEquals(CompressionCodec.NONE, connectionContext.getCompressionCodec());
    assertEquals(8, connectionContext.parameters.size());
    assertEquals(LogLevel.OFF, connectionContext.getLogLevel());
    assertEquals(connectionContext.getOAuthScopesForU2M(), expected_scopes);
    assertFalse(connectionContext.isAllPurposeCluster());
    assertEquals(DatabricksClientType.THRIFT, connectionContext.getClientType());
  }

  @Test
  public void testPortStringAndAuthEndpointsThroughConnectionParameters()
      throws DatabricksSQLException {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_8, properties);
    assertEquals(123, connectionContext.port);
    assertEquals("tokenEndpoint", connectionContext.getTokenEndpoint());
    assertEquals("authEndpoint", connectionContext.getAuthEndpoint());
    assertEquals("test_kid", connectionContext.getKID());
    assertEquals("test_algo", connectionContext.getJWTAlgorithm());
    assertEquals("test_phrase", connectionContext.getJWTPassphrase());
    assertEquals("test_key_file", connectionContext.getJWTKeyFile());
    assertTrue(connectionContext.useJWTAssertion());
    assertThrows(
        DatabricksSQLException.class,
        () -> DatabricksConnectionContext.parse(TestConstants.INVALID_URL_3, properties));
  }

  @Test
  public void testCompressionTypeParsing() throws DatabricksSQLException {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_4, properties);
    assertEquals(CompressionCodec.LZ4_FRAME, connectionContext.getCompressionCodec());
    connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(
                TestConstants.VALID_URL_WITH_INVALID_COMPRESSION_TYPE, properties);
    assertEquals(CompressionCodec.LZ4_FRAME, connectionContext.getCompressionCodec());
  }

  @Test
  public void AuthFlowParsing() {
    assertEquals(
        IDatabricksConnectionContext.AuthMech.PAT,
        IDatabricksConnectionContext.AuthMech.parseAuthMech("3"),
        "Parsing '3' should return PAT");
    assertEquals(
        IDatabricksConnectionContext.AuthMech.OAUTH,
        IDatabricksConnectionContext.AuthMech.parseAuthMech("11"),
        "Parsing '11' should return OAUTH");
    assertThrows(
        UnsupportedOperationException.class,
        () -> IDatabricksConnectionContext.AuthMech.parseAuthMech("1"),
        "Parsing unsupported value should throw exception");
    assertThrows(
        NumberFormatException.class,
        () -> IDatabricksConnectionContext.AuthMech.parseAuthMech("non-numeric"),
        "Parsing non-numeric value should throw NumberFormatException");
  }

  @Test
  public void testFetchSchemaType() throws DatabricksSQLException {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_5, properties);
    assertNull(connectionContext.getSchema());

    connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_6, properties);
    assertEquals("schemaName", connectionContext.getSchema());
  }

  @Test
  public void testEndpointHttpPathParsing() throws DatabricksSQLException {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_7, properties);
    assertEquals("/sql/1.0/endpoints/erg6767gg", connectionContext.getHttpPath());
  }

  @Test
  public void testEndpointURL() throws DatabricksSQLException {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_CLUSTER_URL, properties);
    assertEquals(
        "https://e2-dogfood.staging.cloud.databricks.com:443/sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv",
        connectionContext.getEndpointURL());
  }

  @Test
  public void testFetchCatalog() throws DatabricksSQLException {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_5, properties);
    assertNull(connectionContext.getCatalog());

    connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_6, properties);
    assertEquals("catalogName", connectionContext.getCatalog());
  }

  @Test
  public void testEnableCloudFetch() throws DatabricksSQLException {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_5, properties);
    assertTrue(connectionContext.shouldEnableArrow());
    connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_7, properties);
    assertFalse(connectionContext.shouldEnableArrow());
  }

  @Test
  public void testAllPurposeClusterParsing() throws DatabricksSQLException {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_CLUSTER_URL, properties);
    assertEquals(
        "https://e2-dogfood.staging.cloud.databricks.com:443", connectionContext.getHostUrl());
    assertEquals(
        "sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv", connectionContext.getHttpPath());
    assertEquals("passwd", connectionContext.getToken());
    assertEquals(CompressionCodec.LZ4_FRAME, connectionContext.getCompressionCodec());
    assertEquals(5, connectionContext.parameters.size());
    assertEquals(LogLevel.WARN, connectionContext.getLogLevel());
    assertTrue(connectionContext.isAllPurposeCluster());
    assertEquals(DatabricksClientType.THRIFT, connectionContext.getClientType());
  }

  @Test
  public void testParsingOfUrlWithoutDefault() throws DatabricksSQLException {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_5, properties);
    assertEquals("/sql/1.0/warehouses/5c89f447c476a5a8", connectionContext.getHttpPath());
    assertEquals("passwd", connectionContext.getToken());
    assertEquals(CompressionCodec.LZ4_FRAME, connectionContext.getCompressionCodec());
    assertEquals(6, connectionContext.parameters.size());
    assertEquals(
        "http://e2-dogfood.staging.cloud.databricks.com:4473", connectionContext.getHostUrl());
    assertEquals(LogLevel.OFF, connectionContext.getLogLevel());
  }

  @Test
  public void testPollingInterval() throws DatabricksSQLException {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_5, properties);
    assertEquals(200, connectionContext.getAsyncExecPollInterval());

    DatabricksConnectionContext connectionContextWithPoll =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_POLLING, properties);
    assertEquals(500, connectionContextWithPoll.getAsyncExecPollInterval());
  }

  @Test
  public void testParsingOfUrlWithEnableDirectResultsFlag() throws DatabricksSQLException {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_5, properties);
    assertEquals(false, connectionContext.getDirectResultMode());
    DatabricksConnectionContext connectionContext2 =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_4, properties);
    assertEquals(true, connectionContext2.getDirectResultMode());
  }

  @Test
  public void testWithNoEnableDirectResultsFlag() throws DatabricksSQLException {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_3, properties);
    assertEquals(true, connectionContext.getDirectResultMode());
  }

  @Test
  public void testParsingOfUrlWithProxy() throws DatabricksSQLException {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(TestConstants.VALID_URL_WITH_PROXY, properties);
    assertTrue(connectionContext.getUseProxy());
    assertEquals("127.0.0.1", connectionContext.getProxyHost());
    assertEquals(8080, connectionContext.getProxyPort());
    assertEquals(ProxyConfig.ProxyAuthType.BASIC, connectionContext.getProxyAuthType());
    assertEquals("proxyUser", connectionContext.getProxyUser());
    assertEquals("proxyPassword", connectionContext.getProxyPassword());

    System.setProperty("https.proxyHost", "localhost");
    System.setProperty("https.proxyPort", "8080");
    IDatabricksConnectionContext connectionContextWithCFProxy =
        DatabricksConnectionContext.parse(
            TestConstants.VALID_URL_WITH_PROXY_AND_CF_PROXY, properties);
    assertTrue(connectionContextWithCFProxy.getUseSystemProxy());
    assertTrue(connectionContextWithCFProxy.getUseProxy());
    assertEquals("127.0.1.2", connectionContextWithCFProxy.getCloudFetchProxyHost());
    assertEquals(8081, connectionContextWithCFProxy.getCloudFetchProxyPort());
    assertEquals(
        ProxyConfig.ProxyAuthType.SPNEGO,
        connectionContextWithCFProxy.getCloudFetchProxyAuthType());
    assertEquals("cfProxyUser", connectionContextWithCFProxy.getCloudFetchProxyUser());
    assertEquals("cfProxyPassword", connectionContextWithCFProxy.getCloudFetchProxyPassword());
  }

  @Test
  void testLogLevels() {
    assertEquals(getLogLevel(123), LogLevel.OFF);
    assertEquals(getLogLevel(0), LogLevel.OFF);
    assertEquals(getLogLevel(1), LogLevel.FATAL);
    assertEquals(getLogLevel(2), LogLevel.ERROR);
    assertEquals(getLogLevel(3), LogLevel.WARN);
    assertEquals(getLogLevel(4), LogLevel.INFO);
    assertEquals(getLogLevel(5), LogLevel.DEBUG);
    assertEquals(getLogLevel(6), LogLevel.TRACE);
  }
}
