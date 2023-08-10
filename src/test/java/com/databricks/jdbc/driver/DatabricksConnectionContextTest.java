package com.databricks.jdbc.driver;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;

class DatabricksConnectionContestTest {

  private static final String VALID_URL_1 = "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;";
  private static final String VALID_URL_2 = "jdbc:databricks://azuredatabricks.net/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/fgff575757;";
  private static final String INVALID_URL_1 = "jdbc:oracle://azuredatabricks.net/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/fgff575757;";
  private static final String INVALID_URL_2 = "http:databricks://azuredatabricks.net/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/fgff575757;";

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
  }

  @Test
  public void testParseInvalid() throws Exception {
    assertThrows(IllegalArgumentException.class, () -> DatabricksConnectionContext.parse(INVALID_URL_1, properties));
    assertThrows(IllegalArgumentException.class, () -> DatabricksConnectionContext.parse(INVALID_URL_2, properties));
  }

  @Test
  public void testParseValid() throws Exception {
    // test provided port
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext) DatabricksConnectionContext.parse(VALID_URL_1, properties);
    assertEquals("https://adb-565757575.18.azuredatabricks.net:4423", connectionContext.getHostUrl());
    assertEquals("/sql/1.0/warehouses/erg6767gg", connectionContext.getHttpPath());
    assertEquals("passwd", connectionContext.getToken());
    assertEquals(5, connectionContext.parameters.size());

    // test default port
    connectionContext = (DatabricksConnectionContext) DatabricksConnectionContext.parse(VALID_URL_2, properties);
    assertEquals("https://azuredatabricks.net:443", connectionContext.getHostUrl());
    assertEquals("/sql/1.0/warehouses/fgff575757", connectionContext.getHttpPath());
    assertEquals("passwd", connectionContext.getToken());
    assertEquals(5, connectionContext.parameters.size());
    assertEquals("3", connectionContext.parameters.get("AuthMech"));
  }
}
