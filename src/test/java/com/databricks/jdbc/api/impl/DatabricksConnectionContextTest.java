package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.api.impl.DatabricksConnectionContext.buildPropertiesMap;
import static com.databricks.jdbc.api.impl.DatabricksConnectionContext.getLogLevel;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.GCP_GOOGLE_CREDENTIALS_AUTH_TYPE;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.GCP_GOOGLE_ID_AUTH_TYPE;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.M2M_AUTH_TYPE;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.TestConstants;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.*;
import com.databricks.jdbc.exception.DatabricksDriverException;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.sdk.core.ProxyConfig;
import com.google.common.collect.ImmutableMap;
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
  public void testBuildPropertiesMap() {
    String connectionParamString = "param1=value1;param2=value2";
    Properties properties = new Properties();
    properties.setProperty("param3", "value3");

    ImmutableMap<String, String> propertiesMap =
        buildPropertiesMap(connectionParamString, properties);
    assertNotNull(propertiesMap);
    assertEquals(3, propertiesMap.size());
    assertEquals("value1", propertiesMap.get("param1"));
    assertEquals("value2", propertiesMap.get("param2"));
    assertEquals("value3", propertiesMap.get("param3"));
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
    assertEquals("https://sample-host.18.azuredatabricks.net:9999", connectionContext.getHostUrl());
    assertEquals(TestConstants.VALID_URL_1, connectionContext.getConnectionURL());
    assertEquals("/sql/1.0/warehouses/999999999", connectionContext.getHttpPath());
    assertEquals("passwd", connectionContext.getToken());
    assertTrue(connectionContext.isOAuthDiscoveryModeEnabled());
    assertFalse(connectionContext.useJWTAssertion());
    assertEquals(connectionContext.getAuthFlow(), AuthFlow.BROWSER_BASED_AUTHENTICATION);
    assertEquals(7, connectionContext.parameters.size());
    assertEquals(CompressionCodec.LZ4_FRAME, connectionContext.getCompressionCodec());
    assertEquals(LogLevel.DEBUG, connectionContext.getLogLevel());
    assertNull(connectionContext.getClientSecret());
    assertEquals("./test1", connectionContext.getLogPathString());
    assertNull(connectionContext.getOAuthScopesForU2M());
    assertFalse(connectionContext.isAllPurposeCluster());
    assertEquals(DatabricksClientType.THRIFT, connectionContext.getClientType());

    // test default port
    connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_2, properties_with_pwd);
    assertEquals("https://sample-host.18.azuredatabricks.net:9999", connectionContext.getHostUrl());
    assertEquals("/sql/1.0/warehouses/9999999999", connectionContext.getHttpPath());
    assertEquals("passwd2", connectionContext.getToken());
    assertEquals("96eecda7-19ea-49cc-abb5-240097d554f5", connectionContext.getClientId());
    assertEquals(7, connectionContext.parameters.size());
    assertEquals(CompressionCodec.LZ4_FRAME, connectionContext.getCompressionCodec());
    assertEquals(LogLevel.OFF, connectionContext.getLogLevel());
    assertEquals(System.getProperty("user.dir"), connectionContext.getLogPathString());
    assertEquals("3", connectionContext.parameters.get("authmech"));
    assertNull(connectionContext.getOAuthScopesForU2M());
    assertFalse(connectionContext.isAllPurposeCluster());
    assertEquals(DatabricksClientType.SEA, connectionContext.getClientType());

    // test aws port
    connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_3, properties);
    List<String> expected_scopes = List.of("sql", "offline_access");
    assertEquals("http://sample-host.cloud.databricks.com:9999", connectionContext.getHostUrl());
    assertEquals("/sql/1.0/warehouses/9999999999999999", connectionContext.getHttpPath());
    assertEquals("passwd", connectionContext.getToken());
    assertEquals("databricks-sql-jdbc", connectionContext.getClientId());
    assertEquals("sample-host.cloud.databricks.com", connectionContext.getHostForOAuth());
    assertEquals(AuthFlow.TOKEN_PASSTHROUGH, connectionContext.getAuthFlow());
    assertEquals(AuthMech.PAT, connectionContext.getAuthMech());
    assertEquals(CompressionCodec.NONE, connectionContext.getCompressionCodec());
    assertEquals(8, connectionContext.parameters.size());
    assertEquals(LogLevel.OFF, connectionContext.getLogLevel());
    assertEquals(connectionContext.getOAuthScopesForU2M(), expected_scopes);
    assertFalse(connectionContext.isAllPurposeCluster());
    assertEquals(DatabricksClientType.THRIFT, connectionContext.getClientType());

    // test gcp port
    Properties p1 = new Properties();
    p1.setProperty("GoogleServiceAccount", "abc-compute@developer.gserviceaccount.com");
    connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.GCP_TEST_URL, p1);
    assertEquals("https://sample-host.7.gcp.databricks.com:9999", connectionContext.getHostUrl());
    assertEquals("/sql/1.0/warehouses/9999999999999999", connectionContext.getHttpPath());
    assertEquals("databricks-sql-jdbc", connectionContext.getClientId());
    assertEquals("sample-host.7.gcp.databricks.com", connectionContext.getHostForOAuth());
    assertEquals(AuthMech.OAUTH, connectionContext.getAuthMech());
    assertEquals(AuthFlow.CLIENT_CREDENTIALS, connectionContext.getAuthFlow());
    assertEquals(connectionContext.getOAuthScopesForU2M(), expected_scopes);
    assertFalse(connectionContext.isAllPurposeCluster());
    assertEquals(5, connectionContext.parameters.size());
    assertEquals(DatabricksClientType.THRIFT, connectionContext.getClientType());
    assertEquals(
        "abc-compute@developer.gserviceaccount.com", connectionContext.getGoogleServiceAccount());
    assertNull(connectionContext.getGoogleCredentials());
    assertEquals(GCP_GOOGLE_ID_AUTH_TYPE, connectionContext.getGcpAuthType());

    // test gcp port with google credentials file
    Properties p2 = new Properties();
    p2.setProperty("GoogleCredentialsFile", "/path/to/credentials.json");
    connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.GCP_TEST_URL, p2);
    assertEquals(GCP_GOOGLE_CREDENTIALS_AUTH_TYPE, connectionContext.getGcpAuthType());

    // test gcp with Client Secret
    Properties p3 = new Properties();
    p3.setProperty("OAuth2Secret", "client_secret");
    connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.GCP_TEST_URL, p3);
    assertEquals(M2M_AUTH_TYPE, connectionContext.getGcpAuthType());
  }

  @Test
  public void testParseValidBasicUrl() throws DatabricksSQLException {
    // test default AuthMech
    Properties props = new Properties();
    String httpPath = "/sql/1.0/warehouses/fgff575757";
    props.put("password", "passwd");
    props.put("httpPath", httpPath);
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_BASE_URL_1, props);
    assertEquals(AuthMech.PAT, connectionContext.getAuthMech());
    assertEquals("passwd", connectionContext.getToken());
    assertEquals(httpPath, connectionContext.getHttpPath());
    assertEquals(2, connectionContext.parameters.size());

    // test url without <;>
    connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_BASE_URL_3, props);
    assertEquals(AuthMech.PAT, connectionContext.getAuthMech());
    assertEquals("passwd", connectionContext.getToken());
    assertEquals(httpPath, connectionContext.getHttpPath());
    assertEquals(2, connectionContext.parameters.size());
  }

  @Test
  public void testParseWithDefaultStringColumnLength() throws DatabricksSQLException {
    // Test case 1: Valid DefaultStringColumnLength
    String validJdbcUrl = TestConstants.VALID_URL_1;
    Properties properties = new Properties();
    properties.put("DefaultStringColumnLength", 500);
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(validJdbcUrl, properties);
    assertEquals(500, connectionContext.getDefaultStringColumnLength());

    // Test case 2: Out of bounds DefaultStringColumnLength
    properties.put("DefaultStringColumnLength", 400000);
    connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(validJdbcUrl, properties);
    assertEquals(255, connectionContext.getDefaultStringColumnLength());

    // Test case 3: Negative DefaultStringColumnLength
    properties.put("DefaultStringColumnLength", -1);
    connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(validJdbcUrl, properties);
    assertEquals(255, connectionContext.getDefaultStringColumnLength());

    // Test case 4: Invalid format DefaultStringColumnLength
    properties.put("DefaultStringColumnLength", "invalid");
    connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(validJdbcUrl, properties);
    assertEquals(255, connectionContext.getDefaultStringColumnLength());
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
    assertEquals(AuthMech.PAT, AuthMech.parseAuthMech("3"), "Parsing '3' should return PAT");
    assertEquals(AuthMech.OAUTH, AuthMech.parseAuthMech("11"), "Parsing '11' should return OAUTH");
    assertThrows(
        DatabricksDriverException.class,
        () -> AuthMech.parseAuthMech("1"),
        "Parsing unsupported value should throw exception");
    assertThrows(
        DatabricksDriverException.class,
        () -> AuthMech.parseAuthMech("non-numeric"),
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
    assertEquals("/sql/1.0/endpoints/999999999", connectionContext.getHttpPath());
  }

  @Test
  public void testEndpointURL() throws DatabricksSQLException {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_CLUSTER_URL, properties);
    assertEquals(
        "https://sample-host.cloud.databricks.com:9999/sql/protocolv1/o/9999999999999999/9999999999999999999",
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
    assertEquals("https://sample-host.cloud.databricks.com:9999", connectionContext.getHostUrl());
    assertEquals(
        "sql/protocolv1/o/9999999999999999/9999999999999999999", connectionContext.getHttpPath());
    assertEquals("passwd", connectionContext.getToken());
    assertEquals(CompressionCodec.LZ4_FRAME, connectionContext.getCompressionCodec());
    assertEquals(5, connectionContext.parameters.size());
    assertEquals(LogLevel.WARN, connectionContext.getLogLevel());
    assertTrue(connectionContext.isAllPurposeCluster());
    assertEquals(DatabricksClientType.THRIFT, connectionContext.getClientType());
  }

  @Test
  public void testRowsFetchedPerBlock() throws DatabricksSQLException {
    // Test with default value
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_CLUSTER_URL, properties);
    assertEquals(2000000, connectionContext.getRowsFetchedPerBlock());

    // Test with custom value
    Properties properties = new Properties();
    properties.setProperty("password", "passwd");
    properties.setProperty("RowsFetchedPerBlock", "500000");

    connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_CLUSTER_URL, properties);
    assertEquals(500000, connectionContext.getRowsFetchedPerBlock());

    // Test with invalid value (should fall back to default)
    properties.setProperty("RowsFetchedPerBlock", "invalid");

    connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_CLUSTER_URL, properties);
    assertEquals(2000000, connectionContext.getRowsFetchedPerBlock());
  }

  @Test
  public void testParsingOfUrlWithoutDefault() throws DatabricksSQLException {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_5, properties);
    assertEquals("/sql/1.0/warehouses/9999999999999999", connectionContext.getHttpPath());
    assertEquals("passwd", connectionContext.getToken());
    assertEquals(CompressionCodec.LZ4_FRAME, connectionContext.getCompressionCodec());
    assertEquals(6, connectionContext.parameters.size());
    assertEquals("http://sample-host.cloud.databricks.com:9999", connectionContext.getHostUrl());
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
  public void testParsingOfCustomHeaders() throws DatabricksSQLException {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(
                TestConstants.VALID_URL_WITH_CUSTOM_HEADERS, properties);
    assertEquals("headerValue1", connectionContext.getCustomHeaders().get("HEADER_KEY_1"));
    assertEquals("headerValue2", connectionContext.getCustomHeaders().get("headerKey2"));
  }

  @Test
  public void testGetVolumeOperationPathsFlag() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(
            TestConstants.VALID_URL_WITH_VOLUME_ALLOWED_PATH, properties);
    assertEquals("/tmp2", connectionContext.getVolumeOperationAllowedPaths());
    assertEquals(List.of(429, 503, 504), connectionContext.getUCIngestionRetriableHttpCodes());
    assertEquals(600, connectionContext.getUCIngestionRetryTimeoutSeconds());

    connectionContext =
        DatabricksConnectionContext.parse(
            TestConstants.VALID_URL_WITH_STAGING_ALLOWED_PATH, properties);
    assertEquals("/tmp", connectionContext.getVolumeOperationAllowedPaths());
    assertEquals(List.of(503, 504), connectionContext.getUCIngestionRetriableHttpCodes());
    assertEquals(720, connectionContext.getUCIngestionRetryTimeoutSeconds());

    connectionContext = DatabricksConnectionContext.parse(TestConstants.VALID_URL_1, properties);
    assertEquals("", connectionContext.getVolumeOperationAllowedPaths());
    assertEquals(List.of(408, 502, 503, 504), connectionContext.getUCIngestionRetriableHttpCodes());
    assertEquals(900, connectionContext.getUCIngestionRetryTimeoutSeconds());
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
  public void testParsingOfUrlWithSpecifiedCatalogAndSchema() throws DatabricksSQLException {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(
            TestConstants.VALID_URL_WITH_CONN_CATALOG_CONN_SCHEMA_PROVIDED, properties);
    assertEquals("sampleCatalog", connectionContext.getCatalog());
    assertEquals("sampleSchema", connectionContext.getSchema());
    IDatabricksConnectionContext connectionContext2 =
        DatabricksConnectionContext.parse(
            TestConstants.VALID_URL_WITH_CONN_CATALOG_CONN_SCHEMA_NOT_PROVIDED, properties);
    assertEquals("default", connectionContext2.getSchema());
    assertEquals(null, connectionContext2.getCatalog());
    IDatabricksConnectionContext connectionContext3 =
        DatabricksConnectionContext.parse(
            TestConstants.VALID_URL_WITH_CONN_CATALOG_CONN_SCHEMA_NOT_PROVIDED_WITHOUT_SCHEMA,
            properties);
    assertEquals(null, connectionContext3.getSchema());
    assertEquals(null, connectionContext3.getCatalog());
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

  @Test
  public void testGetOAuth2RedirectUrlPorts() throws DatabricksSQLException {
    // Test default value
    Properties props = new Properties();
    DatabricksConnectionContext context =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_1, props);
    List<Integer> ports = context.getOAuth2RedirectUrlPorts();
    assertEquals(1, ports.size());
    assertEquals(8020, ports.get(0)); // Default value

    // Test single port
    props = new Properties();
    props.setProperty("OAuth2RedirectUrlPort", "9090");
    context =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_1, props);
    ports = context.getOAuth2RedirectUrlPorts();
    assertEquals(1, ports.size());
    assertEquals(9090, ports.get(0));

    // Test multiple ports
    props = new Properties();
    props.setProperty("OAuth2RedirectUrlPort", "9090,9091,9092");
    context =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_1, props);
    ports = context.getOAuth2RedirectUrlPorts();
    assertEquals(3, ports.size());
    assertEquals(9090, ports.get(0));
    assertEquals(9091, ports.get(1));
    assertEquals(9092, ports.get(2));

    // Test invalid format
    props = new Properties();
    props.setProperty("OAuth2RedirectUrlPort", "invalid");
    context =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_1, props);
    assertThrows(IllegalArgumentException.class, context::getOAuth2RedirectUrlPorts);
  }
}
