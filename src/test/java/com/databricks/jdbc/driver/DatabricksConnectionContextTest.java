package com.databricks.jdbc.driver;

import static com.databricks.jdbc.driver.DatabricksJdbcConstants.DEFAULT_CATALOG;
import static com.databricks.jdbc.driver.DatabricksJdbcConstants.DEFAULT_SCHEMA;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.core.DatabricksParsingException;
import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.jdbc.core.types.CompressionType;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class DatabricksConnectionContextTest {

  private static final String VALID_URL_1 =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;LogLevel=debug;LogPath=test1/application.log;";
  private static final String VALID_URL_2 =
      "jdbc:databricks://azuredatabricks.net/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/fgff575757;LogLevel=invalid;EnableQueryResultLZ4Compression=1";
  private static final String VALID_URL_3 =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/5c89f447c476a5a8;EnableQueryResultLZ4Compression=0";

  private static final String VALID_URL_4 =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/5c89f447c476a5a8;QueryResultCompressionType=1";
  private static final String VALID_URL_5 =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:4473;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/5c89f447c476a5a8;QueryResultCompressionType=1";

  private static final String VALID_URL_6 =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:4473/schemaName;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/5c89f447c476a5a8;ConnCatalog=catalogName;QueryResultCompressionType=1";

  private static final String VALID_URL_WITH_INVALID_COMPRESSION_TYPE =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/5c89f447c476a5a8;QueryResultCompressionType=234";

  private static final String INVALID_URL_1 =
      "jdbc:oracle://azuredatabricks.net/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/fgff575757;";
  private static final String INVALID_URL_2 =
      "http:databricks://azuredatabricks.net/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/fgff575757;";
  private static final String VALID_TEST_URL = "jdbc:databricks://test";

  private static final String VALID_CLUSTER_URL =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=3";
  private static final String INVALID_CLUSTER_URL =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/oo/6051921418418893/1115-130834-ms4m0yv;AuthMech=3";

  private static final String VALID_URL_WITH_PROXY =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/5c89f447c476a5a8;UseProxy=1;ProxyHost=127.0.0.1;ProxyPort=8080;ProxyAuth=1;ProxyUID=proxyUser;ProxyPwd=proxyPassword;";
  private static final String VALID_URL_WITH_PROXY_AND_CF_PROXY =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/5c89f447c476a5a8;UseSystemProxy=1;UseProxy=1;ProxyHost=127.0.0.1;ProxyPort=8080;ProxyAuth=1;ProxyUID=proxyUser;ProxyPwd=proxyPassword;UseCFProxy=1;CFProxyHost=127.0.1.2;CFProxyPort=8081;CFProxyAuth=1;CFProxyUID=cfProxyUser;CFProxyPwd=cfProxyPassword;";

  private static Properties properties = new Properties();

  @BeforeAll
  public static void setUp() {
    properties.setProperty("password", "passwd");
  }

  @Test
  public void testIsValid() {
    assertTrue(DatabricksConnectionContext.isValid(VALID_URL_1));
    assertTrue(DatabricksConnectionContext.isValid(VALID_TEST_URL));
    assertTrue(DatabricksConnectionContext.isValid(VALID_URL_2));
    assertTrue(DatabricksConnectionContext.isValid(VALID_URL_3));
    assertTrue(DatabricksConnectionContext.isValid(VALID_URL_4));
    assertTrue(DatabricksConnectionContext.isValid(VALID_URL_5));
    assertTrue(DatabricksConnectionContext.isValid(VALID_URL_6));
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
    assertEquals(7, connectionContext.parameters.size());
    assertEquals(CompressionType.NONE, connectionContext.getCompressionType());
    assertEquals("DEBUG", connectionContext.getLogLevelString());
    assertEquals("test1/application.log", connectionContext.getLogPathString());
    assertNull(connectionContext.getOAuthScopesForU2M());
    assertFalse(connectionContext.isAllPurposeCluster());

    // test default port
    connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(VALID_URL_2, properties);
    assertEquals("https://azuredatabricks.net:443", connectionContext.getHostUrl());
    assertEquals("/sql/1.0/warehouses/fgff575757", connectionContext.getHttpPath());
    assertEquals("passwd", connectionContext.getToken());
    assertEquals(7, connectionContext.parameters.size());
    assertEquals(CompressionType.LZ4_COMPRESSION, connectionContext.getCompressionType());
    assertEquals("INFO", connectionContext.getLogLevelString());
    assertNull(connectionContext.getLogPathString());
    assertEquals("3", connectionContext.parameters.get("authmech"));
    assertNull(connectionContext.getOAuthScopesForU2M());
    assertFalse(connectionContext.isAllPurposeCluster());

    // test aws port
    connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(VALID_URL_3, properties);
    List<String> expected_scopes = List.of("sql", "offline_access");
    assertEquals(
        "https://e2-dogfood.staging.cloud.databricks.com:443", connectionContext.getHostUrl());
    assertEquals("/sql/1.0/warehouses/5c89f447c476a5a8", connectionContext.getHttpPath());
    assertEquals("passwd", connectionContext.getToken());
    assertEquals(CompressionType.NONE, connectionContext.getCompressionType());
    assertEquals(6, connectionContext.parameters.size());
    assertEquals("INFO", connectionContext.getLogLevelString());
    assertEquals(connectionContext.getOAuthScopesForU2M(), expected_scopes);
    assertFalse(connectionContext.isAllPurposeCluster());
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
  public void testFetchSchemaType() throws DatabricksSQLException {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(VALID_URL_5, properties);
    assertEquals(DEFAULT_SCHEMA, connectionContext.getSchema());

    connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(VALID_URL_6, properties);
    assertEquals("schemaName", connectionContext.getSchema());
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
    assertEquals("INFO", connectionContext.getLogLevelString());
    assertTrue(connectionContext.isAllPurposeCluster());
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
        "https://e2-dogfood.staging.cloud.databricks.com:4473", connectionContext.getHostUrl());
    assertEquals("INFO", connectionContext.getLogLevelString());
  }

  @Test
  public void testParsingofUrlWithProxy() throws DatabricksSQLException {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(VALID_URL_WITH_PROXY, properties);
    assertTrue(connectionContext.getUseProxy());
    assertEquals("127.0.0.1", connectionContext.getProxyHost());
    assertEquals(8080, connectionContext.getProxyPort());
    assertTrue(connectionContext.getUseProxyAuth());
    assertEquals("proxyUser", connectionContext.getProxyUser());
    assertEquals("proxyPassword", connectionContext.getProxyPassword());

    IDatabricksConnectionContext connectionContextWithCFProxy =
        DatabricksConnectionContext.parse(VALID_URL_WITH_PROXY_AND_CF_PROXY, properties);
    assertTrue(connectionContextWithCFProxy.getUseSystemProxy());
    assertTrue(connectionContextWithCFProxy.getUseProxy());
    assertEquals("127.0.1.2", connectionContextWithCFProxy.getCloudFetchProxyHost());
    assertEquals(8081, connectionContextWithCFProxy.getCloudFetchProxyPort());
    assertTrue(connectionContextWithCFProxy.getUseCloudFetchProxyAuth());
    assertEquals("cfProxyUser", connectionContextWithCFProxy.getCloudFetchProxyUser());
    assertEquals("cfProxyPassword", connectionContextWithCFProxy.getCloudFetchProxyPassword());
  }
}
