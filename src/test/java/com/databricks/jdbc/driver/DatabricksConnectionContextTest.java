package com.databricks.jdbc.driver;

import static org.junit.jupiter.api.Assertions.*;

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

  private static final String VALID_URL_WITH_INVALID_COMPRESSION_TYPE =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/5c89f447c476a5a8;QueryResultCompressionType=234";

  private static final String INVALID_URL_1 =
      "jdbc:oracle://azuredatabricks.net/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/fgff575757;";
  private static final String INVALID_URL_2 =
      "http:databricks://azuredatabricks.net/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/fgff575757;";
  private static final String VALID_TEST_URL = "jdbc:databricks://test";

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
    assertTrue(DatabricksConnectionContext.isValid(VALID_URL_WITH_INVALID_COMPRESSION_TYPE));
    assertFalse(DatabricksConnectionContext.isValid(INVALID_URL_1));
    assertFalse(DatabricksConnectionContext.isValid(INVALID_URL_2));
  }

  @Test
  public void testParseInvalid() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DatabricksConnectionContext.parse(INVALID_URL_1, properties));
    assertThrows(
        IllegalArgumentException.class,
        () -> DatabricksConnectionContext.parse(INVALID_URL_2, properties));
  }

  @Test
  public void testParseValid() {
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
  }

  @Test
  public void testCompressionTypeParsing() {
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(VALID_URL_4, properties);
    assertEquals(CompressionType.LZ4_COMPRESSION, connectionContext.getCompressionType());
    connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(VALID_URL_WITH_INVALID_COMPRESSION_TYPE, properties);
    assertEquals(CompressionType.NONE, connectionContext.getCompressionType());
  }

  @Test
  public void testParsingOfUrlWithoutDefault() {
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
}
