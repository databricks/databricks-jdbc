package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.TestConstants.TEST_SCOPE_STRING;
import static com.databricks.jdbc.api.impl.DatabricksConnectionContext.buildPropertiesMap;
import static com.databricks.jdbc.api.impl.DatabricksConnectionContext.getLogLevel;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.GCP_GOOGLE_CREDENTIALS_AUTH_TYPE;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.GCP_GOOGLE_ID_AUTH_TYPE;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.M2M_AUTH_TYPE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.TestConstants;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.*;
import com.databricks.jdbc.common.safe.DatabricksDriverFeatureFlagsContextFactory;
import com.databricks.jdbc.exception.DatabricksDriverException;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksVendorCode;
import com.databricks.sdk.core.ProxyConfig;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

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
    assertEquals(
        Arrays.asList(
            DatabricksJdbcConstants.SQL_SCOPE, DatabricksJdbcConstants.OFFLINE_ACCESS_SCOPE),
        connectionContext.getOAuthScopesForU2M());
    assertFalse(connectionContext.isAllPurposeCluster());
    assertEquals(DatabricksClientType.THRIFT, connectionContext.getClientType());

    // test default port
    connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_2, properties_with_pwd);
    assertEquals("https://sample-host.18.azuredatabricks.net:9999", connectionContext.getHostUrl());
    assertEquals("/sql/1.0/warehouses/9999999999", connectionContext.getHttpPath());
    assertEquals("passwd2", connectionContext.getToken());
    assertEquals("databricks-sql-jdbc", connectionContext.getClientId());
    assertEquals(7, connectionContext.parameters.size());
    assertEquals(CompressionCodec.LZ4_FRAME, connectionContext.getCompressionCodec());
    assertEquals(LogLevel.OFF, connectionContext.getLogLevel());
    assertEquals(System.getProperty("user.dir"), connectionContext.getLogPathString());
    assertEquals("3", connectionContext.parameters.get("authmech"));
    assertEquals(
        Arrays.asList(
            DatabricksJdbcConstants.SQL_SCOPE, DatabricksJdbcConstants.OFFLINE_ACCESS_SCOPE),
        connectionContext.getOAuthScopesForU2M());
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
    assertEquals(9, connectionContext.parameters.size());
    assertEquals(LogLevel.OFF, connectionContext.getLogLevel());
    assertEquals(
        connectionContext.getOAuthScopesForU2M(), Collections.singletonList(TEST_SCOPE_STRING));
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
  public void testEmptySchemaConvertedToNull() throws DatabricksSQLException {
    String urlWithEmptySchema =
        "jdbc:databricks://sample-host.18.azuredatabricks.net:9999/;ssl=1;AuthMech=3;"
            + "httpPath=/sql/1.0/warehouses/999999999;LogLevel=debug;LogPath=./test1;auth_flow=2";
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(urlWithEmptySchema, properties);
    assertNull(connectionContext.getSchema());
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
  public void testAllPurposeClusterAlwaysUsesThriftClient() throws DatabricksSQLException {
    // Test that all-purpose clusters always use THRIFT client type regardless of feature flags
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_CLUSTER_URL, properties);

    // Verify it's an all-purpose cluster
    assertTrue(connectionContext.isAllPurposeCluster());

    // Should use THRIFT client type
    assertEquals(DatabricksClientType.THRIFT, connectionContext.getClientType());

    // Even if we set feature flag to enable SEA, all-purpose cluster should still use THRIFT
    Map<String, String> flags = new HashMap<>();
    flags.put("databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbc", "true");
    DatabricksDriverFeatureFlagsContextFactory.setFeatureFlagsContext(connectionContext, flags);

    // Client type should still be THRIFT for all-purpose cluster
    assertEquals(DatabricksClientType.THRIFT, connectionContext.getClientType());
  }

  @Test
  public void testRowsFetchedPerBlockDefault() throws DatabricksSQLException {
    // Test with default value
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_CLUSTER_URL, properties);
    assertEquals(100000, connectionContext.getRowsFetchedPerBlock());
  }

  @ParameterizedTest
  @MethodSource("rowsFetchedPerBlockTestCases")
  public void testRowsFetchedPerBlock(String value, Integer expectedResult)
      throws DatabricksSQLException {
    Properties testProperties = new Properties();
    testProperties.setProperty("password", "passwd");
    testProperties.setProperty("RowsFetchedPerBlock", value);

    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_CLUSTER_URL, testProperties);
    assertEquals(expectedResult, connectionContext.getRowsFetchedPerBlock());
  }

  private static Stream<Arguments> rowsFetchedPerBlockTestCases() {
    return Stream.of(
        Arguments.of("500000", 500000), // Valid positive value
        Arguments.of("1", 1), // Valid minimum positive value
        Arguments.of("0", 2000000), // Zero returns default
        Arguments.of("-100", 2000000)); // Negative returns default
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
    assertEquals(
        List.of(408, 429, 500, 502, 503, 504),
        connectionContext.getUCIngestionRetriableHttpCodes());
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

  @ParameterizedTest
  @CsvSource(
      value = {
        "<NULL>, DEBUG",
        "'', DEBUG",
        "6, TRACE",
        "0, OFF",
        "'  trace  ', TRACE",
        "nope, DEBUG"
      },
      nullValues = "<NULL>")
  void testTelemetryLogLevelParameterized(String input, TelemetryLogLevel expected)
      throws DatabricksSQLException {
    String baseUrl = TestConstants.VALID_URL_1;
    Properties props = new Properties();
    if (input != null) {
      props.setProperty("telemetryLogLevel", input);
    }
    DatabricksConnectionContext ctx =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(baseUrl, props);
    assertEquals(expected, ctx.getTelemetryLogLevel());
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
    assertThrows(DatabricksDriverException.class, context::getOAuth2RedirectUrlPorts);
  }

  @Test
  public void testTokenCacheSettings() throws DatabricksSQLException {
    // Test with token cache disabled (default)
    String jdbcUrl =
        "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;EnableTokenCache=0";
    Properties properties = new Properties();
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(jdbcUrl, properties);
    assertFalse(connectionContext.isTokenCacheEnabled());
    assertNull(connectionContext.getTokenCachePassPhrase());

    // Test with token cache enabled but no passphrase
    jdbcUrl =
        "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;EnableTokenCache=1";
    connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(jdbcUrl, properties);
    assertTrue(connectionContext.isTokenCacheEnabled());
    assertNull(connectionContext.getTokenCachePassPhrase());

    // Test with token cache enabled and passphrase specified
    jdbcUrl =
        "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;EnableTokenCache=1;TokenCachePassPhrase=testpass";
    connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(jdbcUrl, properties);
    assertTrue(connectionContext.isTokenCacheEnabled());
    assertEquals("testpass", connectionContext.getTokenCachePassPhrase());

    // Test with token cache enabled via properties
    jdbcUrl =
        "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg";
    properties.setProperty("EnableTokenCache", "1");
    properties.setProperty("TokenCachePassPhrase", "proppass");
    connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(jdbcUrl, properties);
    assertTrue(connectionContext.isTokenCacheEnabled());
    assertEquals("proppass", connectionContext.getTokenCachePassPhrase());
  }

  @Test
  public void testSSLKeystoreParameters() throws DatabricksSQLException {
    // Test case 1: Default settings (all null)
    String validJdbcUrl = TestConstants.VALID_URL_1;
    Properties properties = new Properties();
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(validJdbcUrl, properties);
    assertNull(connectionContext.getSSLKeyStore());
    assertNull(connectionContext.getSSLKeyStorePassword());
    assertEquals("JKS", connectionContext.getSSLKeyStoreType());
    assertNull(connectionContext.getSSLKeyStoreProvider());

    // Test case 2: With keystore parameters
    properties.put("SSLKeyStore", "/path/to/keystore.jks");
    properties.put("SSLKeyStorePwd", "keystorepassword");
    properties.put("SSLKeyStoreType", "PKCS12");
    properties.put("SSLKeyStoreProvider", "SunJSSE");
    connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(validJdbcUrl, properties);
    assertEquals("/path/to/keystore.jks", connectionContext.getSSLKeyStore());
    assertEquals("keystorepassword", connectionContext.getSSLKeyStorePassword());
    assertEquals("PKCS12", connectionContext.getSSLKeyStoreType());
    assertEquals("SunJSSE", connectionContext.getSSLKeyStoreProvider());
  }

  @Test
  public void testSSLTrustStoreParameters() throws DatabricksSQLException {
    // Test case 1: Default settings (all null)
    String validJdbcUrl = TestConstants.VALID_URL_1;
    Properties properties = new Properties();
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(validJdbcUrl, properties);
    assertNull(connectionContext.getSSLTrustStore());

    // Test case 2: With truststore parameters
    properties.put("SSLTrustStore", "/path/to/truststore.jks");
    properties.put("SSLTrustStorePwd", "truststorepassword");
    properties.put("SSLTrustStoreType", "PKCS12");
    properties.put("SSLTrustStoreProvider", "SunJSSE");
    connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(validJdbcUrl, properties);
  }

  @Test
  public void testUidValidation_ValidToken() throws DatabricksSQLException {
    // Test that UID=token is valid
    String urlWithValidUid =
        "jdbc:databricks://sample-host.18.azuredatabricks.net:9999/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/999999999;UID=token";
    Properties properties = new Properties();
    properties.setProperty("password", "passwd");

    // Should not throw exception
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(urlWithValidUid, properties);
    assertNotNull(connectionContext);
  }

  @Test
  public void testUidValidation_NoUidProvided() throws DatabricksSQLException {
    // Test that missing UID is valid (backward compatibility)
    String urlWithoutUid =
        "jdbc:databricks://sample-host.18.azuredatabricks.net:9999/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/999999999";
    Properties properties = new Properties();
    properties.setProperty("password", "passwd");

    // Should not throw exception
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(urlWithoutUid, properties);
    assertNotNull(connectionContext);
  }

  @Test
  public void testUidValidation_EmptyUid() {
    // Test that UID= (empty) is invalid
    String urlWithEmptyUid =
        "jdbc:databricks://sample-host.18.azuredatabricks.net:9999/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/999999999;UID=";
    Properties properties = new Properties();
    properties.setProperty("password", "passwd");

    // Should throw DatabricksValidationException with vendor code 500174
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> DatabricksConnectionContext.parse(urlWithEmptyUid, properties));
    assertTrue(exception.getMessage().contains("Invalid UID parameter"));
    assertEquals(DatabricksVendorCode.INCORRECT_UID.getCode(), exception.getErrorCode());
  }

  @Test
  public void testUidValidation_InvalidUidValue() {
    // Test that UID=user is invalid
    String urlWithInvalidUid =
        "jdbc:databricks://sample-host.18.azuredatabricks.net:9999/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/999999999;UID=user";
    Properties properties = new Properties();
    properties.setProperty("password", "passwd");

    // Should throw DatabricksValidationException with vendor code 500174
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> DatabricksConnectionContext.parse(urlWithInvalidUid, properties));
    assertTrue(exception.getMessage().contains("Invalid UID parameter"));
    assertEquals(DatabricksVendorCode.INCORRECT_UID.getCode(), exception.getErrorCode());
  }

  @Test
  public void testUidValidation_InvalidUidInProperties() {
    // Test UID validation when provided via Properties instead of URL
    String baseUrl =
        "jdbc:databricks://sample-host.18.azuredatabricks.net:9999/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/999999999";
    Properties properties = new Properties();
    properties.setProperty("password", "passwd");
    properties.setProperty("UID", "admin"); // Invalid UID value

    // Should throw DatabricksValidationException
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () -> DatabricksConnectionContext.parse(baseUrl, properties));
    assertEquals(DatabricksVendorCode.INCORRECT_UID.getCode(), exception.getErrorCode());
    assertTrue(exception.getMessage().contains("Expected 'token' or omit UID parameter entirely"));
  }

  @Test
  public void testUidValidation_ValidUidInProperties() throws DatabricksSQLException {
    // Test that UID=token in Properties is valid
    String baseUrl =
        "jdbc:databricks://sample-host.18.azuredatabricks.net:9999/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/999999999";
    Properties properties = new Properties();
    properties.setProperty("password", "passwd");
    properties.setProperty("UID", "token"); // Valid UID value

    // Should not throw exception
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(baseUrl, properties);
    assertNotNull(connectionContext);
  }

  @Test
  public void testSqlExecDirectResultsEnabled() throws DatabricksSQLException {
    // Test default value (should be true)
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_1, properties);
    assertTrue(connectionContext.isSqlExecDirectResultsEnabled());

    // Test when EnableSQLExecDirectResults=1
    String urlWithDirectResults =
        "jdbc:databricks://sample-host.cloud.databricks.com:9999/default;AuthMech=3;"
            + "httpPath=/sql/1.0/warehouses/9999999999999999;EnableSQLExecDirectResults=1";
    connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(urlWithDirectResults, properties);
    assertTrue(connectionContext.isSqlExecDirectResultsEnabled());

    // Test when EnableSQLExecDirectResults=0
    String urlWithoutDirectResults =
        "jdbc:databricks://sample-host.cloud.databricks.com:9999/default;AuthMech=3;"
            + "httpPath=/sql/1.0/warehouses/9999999999999999;EnableSQLExecDirectResults=0";
    connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(urlWithoutDirectResults, properties);
    assertFalse(connectionContext.isSqlExecDirectResultsEnabled());
  }

  @Test
  public void testEnableMultipleCatalogSupport() throws DatabricksSQLException {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_1, properties);
    assertTrue(connectionContext.getEnableMultipleCatalogSupport());

    String urlWithMultipleCatalogEnabled =
        "jdbc:databricks://sample-host.cloud.databricks.com:9999/default;AuthMech=3;"
            + "httpPath=/sql/1.0/warehouses/9999999999999999;enableMultipleCatalogSupport=1";
    connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(urlWithMultipleCatalogEnabled, properties);
    assertTrue(connectionContext.getEnableMultipleCatalogSupport());

    String urlWithMultipleCatalogDisabled =
        "jdbc:databricks://sample-host.cloud.databricks.com:9999/default;AuthMech=3;"
            + "httpPath=/sql/1.0/warehouses/9999999999999999;enableMultipleCatalogSupport=0";
    connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(urlWithMultipleCatalogDisabled, properties);
    assertFalse(connectionContext.getEnableMultipleCatalogSupport());

    Properties propsWithParam = new Properties();
    propsWithParam.setProperty("password", "passwd");
    propsWithParam.setProperty("enableMultipleCatalogSupport", "0");
    connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_1, propsWithParam);
    assertFalse(connectionContext.getEnableMultipleCatalogSupport());

    Properties propsWithInvalidParam = new Properties();
    propsWithInvalidParam.setProperty("password", "passwd");
    propsWithInvalidParam.setProperty("enableMultipleCatalogSupport", "invalid");
    connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_1, propsWithInvalidParam);
    assertFalse(connectionContext.getEnableMultipleCatalogSupport());
  }

  // ===== Lazy Initialization Tests =====

  @Test
  public void testClientTypeIsCachedAfterFirstAccess() throws DatabricksSQLException {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_2, properties_with_pwd);

    // First call should compute the client type
    DatabricksClientType firstCall = connectionContext.getClientType();
    // Second call should return cached value
    DatabricksClientType secondCall = connectionContext.getClientType();

    assertEquals(firstCall, secondCall);
    assertEquals(DatabricksClientType.SEA, firstCall);
  }

  @Test
  public void testClientTypeThreadSafety() throws Exception {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_2, properties_with_pwd);

    int numThreads = 10;
    Thread[] threads = new Thread[numThreads];
    DatabricksClientType[] results = new DatabricksClientType[numThreads];

    for (int i = 0; i < numThreads; i++) {
      final int index = i;
      threads[i] = new Thread(() -> results[index] = connectionContext.getClientType());
    }

    // Start all threads at once
    for (Thread thread : threads) {
      thread.start();
    }

    // Wait for all threads to complete
    for (Thread thread : threads) {
      thread.join();
    }

    // All threads should get the same result
    DatabricksClientType expected = results[0];
    for (DatabricksClientType result : results) {
      assertEquals(expected, result);
    }
  }

  // ===== Client Type Selection Logic Tests =====

  @Test
  public void testClientTypeWithExplicitUseThriftClientEnabled() throws DatabricksSQLException {
    String urlWithThrift =
        "jdbc:databricks://sample-host.cloud.databricks.com:9999/default;AuthMech=3;"
            + "httpPath=/sql/1.0/warehouses/9999999999999999;UseThriftClient=1;EnableArrow=1;EnableQueryResultDownload=1";
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(urlWithThrift, properties);
    assertEquals(DatabricksClientType.THRIFT, connectionContext.getClientType());
  }

  @Test
  public void testClientTypeWithExplicitUseThriftClientDisabled() throws DatabricksSQLException {
    String urlWithSea =
        "jdbc:databricks://sample-host.cloud.databricks.com:9999/default;AuthMech=3;"
            + "httpPath=/sql/1.0/warehouses/9999999999999999;UseThriftClient=0";
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(urlWithSea, properties);
    assertEquals(DatabricksClientType.SEA, connectionContext.getClientType());
  }

  @Test
  public void testClientTypeWhenArrowDisabled() throws DatabricksSQLException {
    String urlWithArrowDisabled =
        "jdbc:databricks://sample-host.cloud.databricks.com:9999/default;AuthMech=3;"
            + "httpPath=/sql/1.0/warehouses/9999999999999999;EnableArrow=0";
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(urlWithArrowDisabled, properties);
    assertEquals(DatabricksClientType.THRIFT, connectionContext.getClientType());
  }

  @Test
  public void testClientTypeWhenCloudFetchDisabled() throws DatabricksSQLException {
    String urlWithCloudFetchDisabled =
        "jdbc:databricks://sample-host.cloud.databricks.com:9999/default;AuthMech=3;"
            + "httpPath=/sql/1.0/warehouses/9999999999999999;EnableQueryResultDownload=0";
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(urlWithCloudFetchDisabled, properties);
    assertEquals(DatabricksClientType.THRIFT, connectionContext.getClientType());
  }

  @Test
  public void testClientTypeWhenBothArrowAndCloudFetchDisabled() throws DatabricksSQLException {
    String urlWithBothDisabled =
        "jdbc:databricks://sample-host.cloud.databricks.com:9999/default;AuthMech=3;"
            + "httpPath=/sql/1.0/warehouses/9999999999999999;EnableArrow=0;EnableQueryResultDownload=0";
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(urlWithBothDisabled, properties);
    assertEquals(DatabricksClientType.THRIFT, connectionContext.getClientType());
  }

  // ===== setClientType Override Tests =====

  @Test
  public void testSetClientTypeBeforeFirstAccess() throws DatabricksSQLException {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_3, properties);

    // Set client type before accessing
    connectionContext.setClientType(DatabricksClientType.SEA);

    // Should return the overridden value
    assertEquals(DatabricksClientType.SEA, connectionContext.getClientType());
  }

  @Test
  public void testSetClientTypeAfterFirstAccess() throws DatabricksSQLException {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_3, properties);

    // First access computes THRIFT
    assertEquals(DatabricksClientType.THRIFT, connectionContext.getClientType());

    // Override to SEA
    connectionContext.setClientType(DatabricksClientType.SEA);

    // Should return the new overridden value
    assertEquals(DatabricksClientType.SEA, connectionContext.getClientType());
  }

  // ===== Feature Flag Value Tests =====

  @Test
  public void testClientTypeWithFeatureFlagEnabled() throws DatabricksSQLException {
    String url =
        "jdbc:databricks://sample-host.cloud.databricks.com:9999/default;AuthMech=3;"
            + "httpPath=/sql/1.0/warehouses/9999999999999999;EnableArrow=1;EnableQueryResultDownload=1";
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(url, properties);

    // Mock feature flag to return true
    Map<String, String> flags = new HashMap<>();
    flags.put("databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbc", "true");
    DatabricksDriverFeatureFlagsContextFactory.setFeatureFlagsContext(connectionContext, flags);

    assertEquals(DatabricksClientType.SEA, connectionContext.getClientType());
  }

  @Test
  public void testClientTypeWithFeatureFlagDisabled() throws DatabricksSQLException {
    String url =
        "jdbc:databricks://sample-host.cloud.databricks.com:9999/default;AuthMech=3;"
            + "httpPath=/sql/1.0/warehouses/9999999999999999;EnableArrow=1;EnableQueryResultDownload=1";
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(url, properties);

    // Mock feature flag to return false
    Map<String, String> flags = new HashMap<>();
    flags.put("databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbc", "false");
    DatabricksDriverFeatureFlagsContextFactory.setFeatureFlagsContext(connectionContext, flags);

    assertEquals(DatabricksClientType.THRIFT, connectionContext.getClientType());
  }

  @Test
  public void testClientTypeWithInvalidFeatureFlagValue() throws DatabricksSQLException {
    String url =
        "jdbc:databricks://sample-host.cloud.databricks.com:9999/default;AuthMech=3;"
            + "httpPath=/sql/1.0/warehouses/9999999999999999;EnableArrow=1;EnableQueryResultDownload=1";
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(url, properties);

    // Mock feature flag to return invalid value
    Map<String, String> flags = new HashMap<>();
    flags.put(
        "databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbc", "invalid");
    DatabricksDriverFeatureFlagsContextFactory.setFeatureFlagsContext(connectionContext, flags);

    // Should default to THRIFT
    assertEquals(DatabricksClientType.THRIFT, connectionContext.getClientType());
  }

  @Test
  public void testClientTypeWithEmptyFeatureFlagValue() throws DatabricksSQLException {
    String url =
        "jdbc:databricks://sample-host.cloud.databricks.com:9999/default;AuthMech=3;"
            + "httpPath=/sql/1.0/warehouses/9999999999999999;EnableArrow=1;EnableQueryResultDownload=1";
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(url, properties);

    // Mock feature flag to return empty value
    Map<String, String> flags = new HashMap<>();
    flags.put("databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbc", "");
    DatabricksDriverFeatureFlagsContextFactory.setFeatureFlagsContext(connectionContext, flags);

    // Should default to THRIFT
    assertEquals(DatabricksClientType.THRIFT, connectionContext.getClientType());
  }

  @Test
  public void testClientTypeWhenFeatureFlagNotFound() throws DatabricksSQLException {
    String url =
        "jdbc:databricks://sample-host.cloud.databricks.com:9999/default;AuthMech=3;"
            + "httpPath=/sql/1.0/warehouses/9999999999999999;EnableArrow=1;EnableQueryResultDownload=1";
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(url, properties);

    // Mock feature flag context with no matching flag
    Map<String, String> flags = new HashMap<>();
    flags.put("some.other.flag", "true");
    DatabricksDriverFeatureFlagsContextFactory.setFeatureFlagsContext(connectionContext, flags);

    // Should default to THRIFT
    assertEquals(DatabricksClientType.THRIFT, connectionContext.getClientType());
  }

  // ===== Parameterized Decision Matrix Test =====

  @ParameterizedTest
  @CsvSource({
    "true, null, 1, 1, true, THRIFT", // AllPurposeCluster always returns THRIFT
    "true, null, 1, 1, false, THRIFT", // AllPurposeCluster always returns THRIFT
    "false, 1, 1, 1, true, THRIFT", // Explicit useThriftClient=1 returns THRIFT
    "false, 0, 1, 1, true, SEA", // Explicit useThriftClient=0 returns SEA
    "false, 0, 1, 1, false, SEA", // Explicit useThriftClient=0 returns SEA (ignores flag)
    "false, null, 0, 1, true, THRIFT", // Arrow disabled returns THRIFT
    "false, null, 1, 0, true, THRIFT", // CloudFetch disabled returns THRIFT
    "false, null, 1, 1, true, SEA", // All enabled + flag=true returns SEA
    "false, null, 1, 1, false, THRIFT", // All enabled + flag=false returns THRIFT
    "false, null, 0, 0, true, THRIFT", // Both Arrow and CloudFetch disabled returns THRIFT
  })
  public void testClientTypeDecisionMatrix(
      boolean isCluster,
      String useThriftClient,
      int enableArrow,
      int enableCloudFetch,
      boolean featureFlagEnabled,
      DatabricksClientType expectedClientType)
      throws DatabricksSQLException {

    String httpPath =
        isCluster
            ? "sql/protocolv1/o/9999999999999999/9999999999999999999"
            : "/sql/1.0/warehouses/9999999999999999";

    StringBuilder urlBuilder =
        new StringBuilder(
            "jdbc:databricks://sample-host.cloud.databricks.com:9999/default;AuthMech=3;");
    urlBuilder.append("httpPath=").append(httpPath).append(";");

    if (useThriftClient != null && !useThriftClient.equals("null")) {
      urlBuilder.append("UseThriftClient=").append(useThriftClient).append(";");
    }
    urlBuilder.append("EnableArrow=").append(enableArrow).append(";");
    urlBuilder.append("EnableQueryResultDownload=").append(enableCloudFetch).append(";");

    String url = urlBuilder.toString();
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(url, properties);

    // Set up feature flag
    Map<String, String> flags = new HashMap<>();
    flags.put(
        "databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbc",
        String.valueOf(featureFlagEnabled));
    DatabricksDriverFeatureFlagsContextFactory.setFeatureFlagsContext(connectionContext, flags);

    assertEquals(expectedClientType, connectionContext.getClientType());
  }

  @Test
  public void testDisableOauthRefreshTokenParam() throws DatabricksSQLException {
    // Default should be true (offline_access not requested by default)
    DatabricksConnectionContext ctx =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_1, properties);
    assertTrue(ctx.getDisableOauthRefreshToken());

    // Explicitly disable = 0 via URL
    String urlWithDisableOff =
        "jdbc:databricks://sample-host.cloud.databricks.com:9999/default;AuthMech=3;"
            + "httpPath=/sql/1.0/warehouses/9999999999999999;DisableOauthRefreshToken=0";
    ctx =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(urlWithDisableOff, properties);
    assertFalse(ctx.getDisableOauthRefreshToken());

    // Explicitly enable = 1 via URL
    String urlWithDisableOn =
        "jdbc:databricks://sample-host.cloud.databricks.com:9999/default;AuthMech=3;"
            + "httpPath=/sql/1.0/warehouses/9999999999999999;DisableOauthRefreshToken=1";
    ctx =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(urlWithDisableOn, properties);
    assertTrue(ctx.getDisableOauthRefreshToken());

    // Via Properties
    Properties props = new Properties();
    props.setProperty("password", "passwd");
    props.setProperty("DisableOauthRefreshToken", "0");
    ctx =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_1, props);
    assertFalse(ctx.getDisableOauthRefreshToken());
  }

  @Test
  public void testEnableTokenFederation() throws DatabricksSQLException {
    // Test default value (should be enabled by default)
    DatabricksConnectionContext ctx =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_1, properties);
    assertTrue(ctx.isTokenFederationEnabled()); // Default should be true

    // Test via URL parameter - enabled
    String urlWithTokenFederationEnabled =
        "jdbc:databricks://sample-host.18.azuredatabricks.net:9999/default;httpPath=/sql/1.0/warehouses/999999999;EnableTokenFederation=1";
    ctx =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(urlWithTokenFederationEnabled, properties);
    assertTrue(ctx.isTokenFederationEnabled());

    // Test via URL parameter - disabled
    String urlWithTokenFederationDisabled =
        "jdbc:databricks://sample-host.18.azuredatabricks.net:9999/default;httpPath=/sql/1.0/warehouses/999999999;EnableTokenFederation=0";
    ctx =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(urlWithTokenFederationDisabled, properties);
    assertFalse(ctx.isTokenFederationEnabled());

    // Test via Properties - enabled
    Properties props = new Properties();
    props.setProperty("password", "passwd");
    props.setProperty("EnableTokenFederation", "1");
    ctx =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_1, props);
    assertTrue(ctx.isTokenFederationEnabled());

    // Test via Properties - disabled
    props = new Properties();
    props.setProperty("password", "passwd");
    props.setProperty("EnableTokenFederation", "0");
    ctx =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_1, props);
    assertFalse(ctx.isTokenFederationEnabled());
  }

  @Test
  public void testIsCloudFetchEnabled() throws DatabricksSQLException {
    // Test default value (should be enabled by default)
    DatabricksConnectionContext ctx =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_1, properties);
    assertTrue(ctx.isCloudFetchEnabled()); // Default should be true

    // Test via URL parameter - enabled
    String urlWithCloudFetchEnabled =
        "jdbc:databricks://sample-host.18.azuredatabricks.net:9999/default;httpPath=/sql/1.0/warehouses/999999999;EnableQueryResultDownload=1";
    ctx =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(urlWithCloudFetchEnabled, properties);
    assertTrue(ctx.isCloudFetchEnabled());

    // Test via URL parameter - disabled
    String urlWithCloudFetchDisabled =
        "jdbc:databricks://sample-host.18.azuredatabricks.net:9999/default;httpPath=/sql/1.0/warehouses/999999999;EnableQueryResultDownload=0";
    ctx =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(urlWithCloudFetchDisabled, properties);
    assertFalse(ctx.isCloudFetchEnabled());

    // Test via Properties - enabled
    Properties props = new Properties();
    props.setProperty("password", "passwd");
    props.setProperty("EnableQueryResultDownload", "1");
    ctx =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_1, props);
    assertTrue(ctx.isCloudFetchEnabled());

    // Test via Properties - disabled
    props = new Properties();
    props.setProperty("password", "passwd");
    props.setProperty("EnableQueryResultDownload", "0");
    ctx =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_1, props);
    assertFalse(ctx.isCloudFetchEnabled());
  }

  // ===== Inline Streaming Configuration Tests =====

  @Test
  public void testIsInlineStreamingEnabledDefault() throws DatabricksSQLException {
    // Test default value (should be enabled by default - "1")
    DatabricksConnectionContext ctx =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_1, properties);
    assertTrue(ctx.isInlineStreamingEnabled()); // Default should be true
  }

  @Test
  public void testIsInlineStreamingEnabledDisabled() throws DatabricksSQLException {
    // Test via URL parameter - disabled
    String urlWithStreamingDisabled =
        "jdbc:databricks://sample-host.18.azuredatabricks.net:9999/default;httpPath=/sql/1.0/warehouses/999999999;EnableInlineStreaming=0";
    DatabricksConnectionContext ctx =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(urlWithStreamingDisabled, properties);
    assertFalse(ctx.isInlineStreamingEnabled());

    // Test via Properties - disabled
    Properties props = new Properties();
    props.setProperty("password", "passwd");
    props.setProperty("EnableInlineStreaming", "0");
    ctx =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_1, props);
    assertFalse(ctx.isInlineStreamingEnabled());
  }

  @Test
  public void testGetThriftMaxBatchesInMemoryDefault() throws DatabricksSQLException {
    // Test default value (should be 3)
    DatabricksConnectionContext ctx =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_1, properties);
    assertEquals(3, ctx.getThriftMaxBatchesInMemory());
  }

  @Test
  public void testGetThriftMaxBatchesInMemoryCustom() throws DatabricksSQLException {
    // Test custom value via URL
    String urlWithCustomBatches =
        "jdbc:databricks://sample-host.18.azuredatabricks.net:9999/default;httpPath=/sql/1.0/warehouses/999999999;ThriftMaxBatchesInMemory=5";
    DatabricksConnectionContext ctx =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(urlWithCustomBatches, properties);
    assertEquals(5, ctx.getThriftMaxBatchesInMemory());

    // Test custom value via Properties
    Properties props = new Properties();
    props.setProperty("password", "passwd");
    props.setProperty("ThriftMaxBatchesInMemory", "10");
    ctx =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_1, props);
    assertEquals(10, ctx.getThriftMaxBatchesInMemory());
  }

  @Test
  public void testGetThriftMaxBatchesInMemoryInvalidFallback() throws DatabricksSQLException {
    // Test invalid value falls back to default (3)
    Properties props = new Properties();
    props.setProperty("password", "passwd");
    props.setProperty("ThriftMaxBatchesInMemory", "invalid");
    DatabricksConnectionContext ctx =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(TestConstants.VALID_URL_1, props);
    assertEquals(3, ctx.getThriftMaxBatchesInMemory()); // Should fall back to default
  }
}
