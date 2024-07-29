package com.databricks.jdbc.driver;

import static com.databricks.jdbc.driver.DatabricksConnectionContext.getLogLevel;
import static com.databricks.jdbc.driver.DatabricksJdbcConstants.DEFAULT_CATALOG;
import static com.databricks.jdbc.driver.DatabricksJdbcConstants.DEFAULT_SCHEMA;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.client.DatabricksClientType;
import com.databricks.jdbc.commons.LogLevel;
import com.databricks.jdbc.core.DatabricksParsingException;
import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.jdbc.core.types.CompressionType;
import com.databricks.sdk.core.ProxyConfig;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class DatabricksConnectionContextTest {

  private static final String VALID_URL_1 =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;LogLevel=debug;LogPath=test1/application.log;auth_flow=2";
  private static final String VALID_URL_2 =
      "jdbc:databricks://azuredatabricks.net/default;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/fgff575757;LogLevel=invalid;EnableQueryResultLZ4Compression=1;UseThriftClient=0";
  private static final String VALID_URL_3 =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=0;AuthMech=3;httpPath=/sql/1.0/warehouses/5c89f447c476a5a8;EnableQueryResultLZ4Compression=0;UseThriftClient=1;LogLevel=1234";
  private static final String VALID_URL_4 =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;AuthMech=3;httpPath=/sql/1.0/warehouses/5c89f447c476a5a8;QueryResultCompressionType=1;EnableDirectResults=1;";
  private static final String VALID_URL_5 =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:4473;ssl=0;AuthMech=3;httpPath=/sql/1.0/warehouses/5c89f447c476a5a8;QueryResultCompressionType=1;EnableDirectResults=0";

  private static final String VALID_URL_6 =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:4473/schemaName;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/5c89f447c476a5a8;ConnCatalog=catalogName;QueryResultCompressionType=1";

  private static final String VALID_URL_7 =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;ssl=1;AuthMech=3;httpPath=/sql/1.0/endpoints/erg6767gg;LogLevel=debug;LogPath=test1/application.log;auth_flow=2;enablearrow=0";

  private static final String VALID_URL_8 =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;ssl=1;port=123;AuthMech=3;httpPath=/sql/1.0/endpoints/erg6767gg;LogLevel=debug;LogPath=test1/application.log;auth_flow=2;enablearrow=0";

  private static final String VALID_URL_WITH_INVALID_COMPRESSION_TYPE =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/5c89f447c476a5a8;QueryResultCompressionType=234";

  private static final String INVALID_URL_1 =
      "jdbc:oracle://azuredatabricks.net/default;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/fgff575757;";
  private static final String INVALID_URL_2 =
      "http:databricks://azuredatabricks.net/default;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/fgff575757;";

  private static final String INVALID_URL_3 =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;ssl=1;port=alphabetical;AuthMech=3;httpPath=/sql/1.0/endpoints/erg6767gg;LogLevel=debug;LogPath=test1/application.log;auth_flow=2;enablearrow=0";

  private static final String VALID_TEST_URL = "jdbc:databricks://test";

  private static final String VALID_CLUSTER_URL =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;ssl=1;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=3;loglevel=3";
  private static final String INVALID_CLUSTER_URL =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;ssl=1;httpPath=sql/protocolv1/oo/6051921418418893/1115-130834-ms4m0yv;AuthMech=3";
  private static final String VALID_BASE_URL_1 =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;";
  private static final String VALID_BASE_URL_2 =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default";
  private static final String VALID_BASE_URL_3 =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443";
  private static final String VALID_URL_WITH_PROXY =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/5c89f447c476a5a8;UseProxy=1;ProxyHost=127.0.0.1;ProxyPort=8080;ProxyAuth=1;ProxyUID=proxyUser;ProxyPwd=proxyPassword;";
  private static final String VALID_URL_WITH_PROXY_AND_CF_PROXY =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/5c89f447c476a5a8;UseSystemProxy=1;UseProxy=1;ProxyHost=127.0.0.1;ProxyPort=8080;ProxyAuth=1;ProxyUID=proxyUser;ProxyPwd=proxyPassword;UseCFProxy=1;CFProxyHost=127.0.1.2;CFProxyPort=8081;CFProxyAuth=2;CFProxyUID=cfProxyUser;CFProxyPwd=cfProxyPassword;";

  private static Properties properties = new Properties();

  private static final String VALID_URL_POLLING =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:4473;ssl=1;asyncexecpollinterval=500;AuthMech=3;httpPath=/sql/1.0/warehouses/5c89f447c476a5a8;QueryResultCompressionType=1";

  private static Properties properties_with_pwd = new Properties();

  @BeforeAll
  public static void setUp() {
    properties.setProperty("password", "passwd");
    properties_with_pwd.setProperty("pwd", "passwd2");
  }

  @Test
  public void testIsValid() {
    assertTrue(DatabricksConnectionContext.isValid(VALID_URL_1));
    assertTrue(DatabricksConnectionContext.isValid(VALID_BASE_URL_1));
    assertTrue(DatabricksConnectionContext.isValid(VALID_BASE_URL_2));
    assertTrue(DatabricksConnectionContext.isValid(VALID_BASE_URL_3));
    assertTrue(DatabricksConnectionContext.isValid(VALID_TEST_URL));
    assertTrue(DatabricksConnectionContext.isValid(VALID_URL_2));
    assertTrue(DatabricksConnectionContext.isValid(VALID_URL_3));
    assertTrue(DatabricksConnectionContext.isValid(VALID_URL_4));
    assertTrue(DatabricksConnectionContext.isValid(VALID_URL_5));
    assertTrue(DatabricksConnectionContext.isValid(VALID_URL_6));
    assertTrue(DatabricksConnectionContext.isValid(VALID_URL_7));
    assertTrue(DatabricksConnectionContext.isValid(VALID_URL_WITH_INVALID_COMPRESSION_TYPE));
    assertFalse(DatabricksConnectionContext.isValid(INVALID_URL_1));
    assertFalse(DatabricksConnectionContext.isValid(INVALID_URL_2));
    assertTrue(DatabricksConnectionContext.isValid(VALID_CLUSTER_URL));
    assertFalse(DatabricksConnectionContext.isValid(INVALID_CLUSTER_URL));
  }

  @Test
  public void testParseInvalid() {
    assertThrows(
        DatabricksParsingException.class,
        () -> DatabricksConnectionContext.parse(INVALID_URL_1, properties));
    assertThrows(
        DatabricksParsingException.class,
        () -> DatabricksConnectionContext.parse(INVALID_URL_2, properties));
  }

  @Test
  public void testParseValid() throws DatabricksSQLException {
    // test provided port
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(VALID_URL_1, properties);
    assertEquals(
        "https://adb-565757575.18.azuredatabricks.net:4423", connectionContext.getHostUrl());
    assertEquals("/sql/1.0/warehouses/erg6767gg", connectionContext.getHttpPath());
    assertEquals("passwd", connectionContext.getToken());
    assertEquals(
        connectionContext.getAuthFlow(),
        IDatabricksConnectionContext.AuthFlow.BROWSER_BASED_AUTHENTICATION);
    assertEquals(7, connectionContext.parameters.size());
    assertEquals(CompressionType.NONE, connectionContext.getCompressionType());
    assertEquals(LogLevel.DEBUG, connectionContext.getLogLevel());
    assertNull(connectionContext.getClientSecret());
    assertEquals("test1/application.log", connectionContext.getLogPathString());
    assertNull(connectionContext.getOAuthScopesForU2M());
    assertFalse(connectionContext.isAllPurposeCluster());
    assertEquals(DatabricksClientType.SQL_EXEC, connectionContext.getClientType());

    // test default port
    connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(VALID_URL_2, properties_with_pwd);
    assertEquals("https://azuredatabricks.net:443", connectionContext.getHostUrl());
    assertEquals("/sql/1.0/warehouses/fgff575757", connectionContext.getHttpPath());
    assertEquals("passwd2", connectionContext.getToken());
    assertEquals("96eecda7-19ea-49cc-abb5-240097d554f5", connectionContext.getClientId());
    assertEquals(7, connectionContext.parameters.size());
    assertEquals(CompressionType.LZ4_COMPRESSION, connectionContext.getCompressionType());
    assertEquals(LogLevel.INFO, connectionContext.getLogLevel());
    assertTrue(connectionContext.getLogPathString().contains("logs/application.log"));
    assertEquals("3", connectionContext.parameters.get("authmech"));
    assertNull(connectionContext.getOAuthScopesForU2M());
    assertFalse(connectionContext.isAllPurposeCluster());
    assertEquals(DatabricksClientType.SQL_EXEC, connectionContext.getClientType());

    // test aws port
    connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(VALID_URL_3, properties);
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
    assertEquals(CompressionType.NONE, connectionContext.getCompressionType());
    assertEquals(8, connectionContext.parameters.size());
    assertEquals(LogLevel.INFO, connectionContext.getLogLevel());
    assertEquals(connectionContext.getOAuthScopesForU2M(), expected_scopes);
    assertFalse(connectionContext.isAllPurposeCluster());
    assertEquals(DatabricksClientType.THRIFT, connectionContext.getClientType());
  }

  @Test
  public void testPortStringThroughConnectionParameters() throws DatabricksSQLException {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(VALID_URL_8, properties);
    assertEquals(123, connectionContext.port);
    assertThrows(
        DatabricksSQLException.class,
        () -> DatabricksConnectionContext.parse(INVALID_URL_3, properties));
  }

  @Test
  public void testCompressionTypeParsing() throws DatabricksSQLException {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(VALID_URL_4, properties);
    assertEquals(CompressionType.LZ4_COMPRESSION, connectionContext.getCompressionType());
    connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(VALID_URL_WITH_INVALID_COMPRESSION_TYPE, properties);
    assertEquals(CompressionType.NONE, connectionContext.getCompressionType());
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
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(VALID_URL_5, properties);
    assertEquals(DEFAULT_SCHEMA, connectionContext.getSchema());

    connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(VALID_URL_6, properties);
    assertEquals("default", connectionContext.getSchema());
  }

  @Test
  public void testEndpointHttpPathParsing() throws DatabricksSQLException {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(VALID_URL_7, properties);
    assertEquals("/sql/1.0/endpoints/erg6767gg", connectionContext.getHttpPath());
  }

  @Test
  public void testEndpointURL() throws DatabricksSQLException {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(VALID_CLUSTER_URL, properties);
    //  assertEquals(connectionContext.getLogLevel(), LogLevel.WARN);
    assertEquals(
        "https://e2-dogfood.staging.cloud.databricks.com:443/sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv",
        connectionContext.getEndpointURL());
  }

  @Test
  public void testFetchCatalog() throws DatabricksSQLException {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(VALID_URL_5, properties);
    assertEquals(DEFAULT_CATALOG, connectionContext.getCatalog());

    connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(VALID_URL_6, properties);
    assertEquals("catalogName", connectionContext.getCatalog());
  }

  @Test
  public void testEnableCloudFetch() throws DatabricksSQLException {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(VALID_URL_5, properties);
    assertTrue(connectionContext.shouldEnableArrow());
    connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(VALID_URL_7, properties);
    assertFalse(connectionContext.shouldEnableArrow());
  }

  @Test
  public void testAllPurposeClusterParsing() throws DatabricksSQLException {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(VALID_CLUSTER_URL, properties);
    assertEquals(
        "https://e2-dogfood.staging.cloud.databricks.com:443", connectionContext.getHostUrl());
    assertEquals(
        "sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv", connectionContext.getHttpPath());
    assertEquals("passwd", connectionContext.getToken());
    assertEquals(CompressionType.NONE, connectionContext.getCompressionType());
    assertEquals(5, connectionContext.parameters.size());
    assertEquals(LogLevel.WARN, connectionContext.getLogLevel());
    assertTrue(connectionContext.isAllPurposeCluster());
    assertEquals(DatabricksClientType.THRIFT, connectionContext.getClientType());
  }

  @Test
  public void testParsingOfUrlWithoutDefault() throws DatabricksSQLException {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(VALID_URL_5, properties);
    assertEquals("/sql/1.0/warehouses/5c89f447c476a5a8", connectionContext.getHttpPath());
    assertEquals("passwd", connectionContext.getToken());
    assertEquals(CompressionType.LZ4_COMPRESSION, connectionContext.getCompressionType());
    assertEquals(6, connectionContext.parameters.size());
    assertEquals(
        "http://e2-dogfood.staging.cloud.databricks.com:4473", connectionContext.getHostUrl());
    assertEquals(LogLevel.INFO, connectionContext.getLogLevel());
  }

  @Test
  public void testPollingInterval() throws DatabricksSQLException {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(VALID_URL_5, properties);
    assertEquals(200, connectionContext.getAsyncExecPollInterval());

    DatabricksConnectionContext connectionContextWithPoll =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(VALID_URL_POLLING, properties);
    assertEquals(500, connectionContextWithPoll.getAsyncExecPollInterval());
  }

  @Test
  public void testParsingOfUrlWithEnableDirectResultsFlag() throws DatabricksSQLException {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(VALID_URL_5, properties);
    assertEquals(false, connectionContext.getDirectResultMode());
    DatabricksConnectionContext connectionContext2 =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(VALID_URL_4, properties);
    assertEquals(true, connectionContext2.getDirectResultMode());
  }

  @Test
  public void testWithNoEnableDirectResultsFlag() throws DatabricksSQLException {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(VALID_URL_3, properties);
    assertEquals(true, connectionContext.getDirectResultMode());
  }

  @Test
  public void testParsingOfUrlWithProxy() throws DatabricksSQLException {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(VALID_URL_WITH_PROXY, properties);
    assertTrue(connectionContext.getUseProxy());
    assertEquals("127.0.0.1", connectionContext.getProxyHost());
    assertEquals(8080, connectionContext.getProxyPort());
    assertEquals(ProxyConfig.ProxyAuthType.BASIC, connectionContext.getProxyAuthType());
    assertEquals("proxyUser", connectionContext.getProxyUser());
    assertEquals("proxyPassword", connectionContext.getProxyPassword());

    System.setProperty("https.proxyHost", "localhost");
    System.setProperty("https.proxyPort", "8080");
    IDatabricksConnectionContext connectionContextWithCFProxy =
        DatabricksConnectionContext.parse(VALID_URL_WITH_PROXY_AND_CF_PROXY, properties);
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
    assertEquals(getLogLevel(123), LogLevel.INFO);
    assertEquals(getLogLevel(0), LogLevel.OFF);
    assertEquals(getLogLevel(1), LogLevel.FATAL);
    assertEquals(getLogLevel(2), LogLevel.ERROR);
    assertEquals(getLogLevel(3), LogLevel.WARN);
    assertEquals(getLogLevel(4), LogLevel.INFO);
    assertEquals(getLogLevel(5), LogLevel.DEBUG);
    assertEquals(getLogLevel(6), LogLevel.TRACE);
  }
}
