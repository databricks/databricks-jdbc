package com.databricks.jdbc.driver;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Properties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class DatabricksConnectionContextTest {

  private static final String VALID_URL_1 =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;LogLevel=debug;LogPath=test1/application.log;";
  private static final String VALID_URL_2 =
      "jdbc:databricks://azuredatabricks.net/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/fgff575757;LogLevel=invalid;";

  private static final String VALID_THRIFT_URL =
      "jdbc:databricks://azuredatabricks.net/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/o/6051921418418893/0405-074109-omxf62hf;AuthMech=3;UID=token;PWD=pat;";

  private static final String INVALID_URL_1 =
      "jdbc:oracle://azuredatabricks.net/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/fgff575757;";
  private static final String INVALID_URL_2 =
      "http:databricks://azuredatabricks.net/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/fgff575757;";

  private static Properties properties = new Properties();

  @BeforeAll
  public static void setUp() {
    properties.setProperty("password", "passwd");
  }

  @Test
  public void testIsValid() throws Exception {
    assertTrue(DatabricksConnectionContext.isValid(VALID_URL_1));
    assertTrue(DatabricksConnectionContext.isValid(VALID_URL_2));
    assertFalse(DatabricksConnectionContext.isValid(INVALID_URL_1));
    assertFalse(DatabricksConnectionContext.isValid(INVALID_URL_2));
    assertTrue(DatabricksConnectionContext.isValid(VALID_THRIFT_URL));
  }

  @Test
  public void testParseInvalid() throws Exception {
    assertThrows(
        IllegalArgumentException.class,
        () -> DatabricksConnectionContext.parse(INVALID_URL_1, properties));
    assertThrows(
        IllegalArgumentException.class,
        () -> DatabricksConnectionContext.parse(INVALID_URL_2, properties));
  }

  @Test
  public void testParseValid() throws Exception {
    // test provided port
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(VALID_URL_1, properties);
    assertEquals(
        "https://adb-565757575.18.azuredatabricks.net:4423", connectionContext.getHostUrl());
    assertEquals("/sql/1.0/warehouses/erg6767gg", connectionContext.getHttpPath());
    assertEquals("passwd", connectionContext.getToken());
    assertEquals(7, connectionContext.parameters.size());
    assertEquals("DEBUG", connectionContext.getLogLevelString());
    assertEquals("test1/application.log", connectionContext.getLogPathString());
    assertEquals(connectionContext.getClientType(), ClientType.SQL_EXEC);

    // test default port
    connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(VALID_URL_2, properties);
    assertEquals("https://azuredatabricks.net:443", connectionContext.getHostUrl());
    assertEquals("/sql/1.0/warehouses/fgff575757", connectionContext.getHttpPath());
    assertEquals("passwd", connectionContext.getToken());
    assertEquals(6, connectionContext.parameters.size());
    assertEquals("INFO", connectionContext.getLogLevelString());
    assertNull(connectionContext.getLogPathString());
    assertEquals("3", connectionContext.parameters.get("authmech"));
    assertEquals(connectionContext.getClientType(), ClientType.SQL_EXEC);

    // test thrift context
    connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(VALID_THRIFT_URL, properties);
    assertEquals("https://azuredatabricks.net:443", connectionContext.getHostUrl());
    assertEquals(
        "sql/protocolv1/o/6051921418418893/0405-074109-omxf62hf", connectionContext.getHttpPath());
    assertEquals("pat", connectionContext.getToken());
    assertEquals(7, connectionContext.parameters.size());
    assertEquals("INFO", connectionContext.getLogLevelString());
    assertEquals("0405-074109-omxf62hf", connectionContext.getWarehouse());
    assertNull(connectionContext.getLogPathString());
    assertEquals("3", connectionContext.parameters.get("authmech"));
    assertEquals(connectionContext.getClientType(), ClientType.THRIFT);
  }
}
