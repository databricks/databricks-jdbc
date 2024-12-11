package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.common.DatabricksJdbcUrlParams.*;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.exception.DatabricksSQLException;
import com.google.common.collect.ImmutableMap;
import java.sql.DriverPropertyInfo;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class DatabricksDriverPropertyUtilTest {

  private static final String test_host = "e2-dogfood.staging.cloud.databricks.com";
  private static final String gcp_host = "4371047901336987.7.gcp.databricks.com";

  private void assertMissingProperties(
      String host, String connectionParamString, String... expectedProperties)
      throws DatabricksSQLException {
    List<DriverPropertyInfo> missingProperties =
        DatabricksDriverPropertyUtil.getMissingProperties(
            host, connectionParamString, new Properties());
    for (String expectedProperty : expectedProperties) {
      assertTrue(
          missingProperties.stream().anyMatch(p -> p.name.equals(expectedProperty)),
          "Missing property: " + expectedProperty);
    }
  }

  @Test
  public void testBuildPropertiesMap() {
    String connectionParamString = ";param1=value1;param2=value2";
    Properties properties = new Properties();
    properties.setProperty("param3", "value3");

    ImmutableMap<String, String> propertiesMap =
        DatabricksDriverPropertyUtil.buildPropertiesMap(connectionParamString, properties);
    assertNotNull(propertiesMap);
    assertEquals(3, propertiesMap.size());
    assertEquals("value1", propertiesMap.get("param1"));
    assertEquals("value2", propertiesMap.get("param2"));
    assertEquals("value3", propertiesMap.get("param3"));
  }

  @Test
  public void testGetMissingProperties() throws DatabricksSQLException {
    String connectionParamString =
        ";transportMode=http;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=3";
    assertMissingProperties(test_host, connectionParamString, PWD.getParamName());

    // log-level properties
    connectionParamString =
        ";transportMode=http;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=3;logLevel=DEBUG;";
    assertMissingProperties(
        test_host,
        connectionParamString,
        LOG_PATH.getParamName(),
        LOG_FILE_SIZE.getParamName(),
        LOG_FILE_COUNT.getParamName());

    // auth-flow missing OAUTH auth mech
    connectionParamString =
        ";transportMode=http;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=11";
    assertMissingProperties(test_host, connectionParamString, AUTH_FLOW.getParamName());

    // TOKEN_PASSTHROUGH with auth-access token.
    connectionParamString =
        ";transportMode=http;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=11;Auth_Flow=0";
    assertMissingProperties(
        test_host,
        connectionParamString,
        OAUTH_REFRESH_TOKEN.getParamName(),
        AUTH_ACCESS_TOKEN.getParamName());

    // TOKEN_PASSTHROUGH with refresh token.
    connectionParamString =
        ";transportMode=http;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=11;Auth_Flow=0;OAuthRefreshToken=token;OAuthDiscoveryMode=0";
    assertMissingProperties(
        test_host,
        connectionParamString,
        CLIENT_ID.getParamName(),
        CLIENT_SECRET.getParamName(),
        TOKEN_ENDPOINT.getParamName(),
        DISCOVERY_URL.getParamName());

    // TOKEN_PASSTHROUGH with discovery mode and refresh token.
    connectionParamString =
        ";transportMode=http;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=11;Auth_Flow=0;OAuthRefreshToken=token;";
    assertMissingProperties(
        test_host,
        connectionParamString,
        CLIENT_ID.getParamName(),
        CLIENT_SECRET.getParamName(),
        DISCOVERY_URL.getParamName());

    // client credentials auth flow
    connectionParamString =
        ";transportMode=http;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=11;Auth_Flow=1;";
    assertMissingProperties(
        test_host,
        connectionParamString,
        CLIENT_ID.getParamName(),
        CLIENT_SECRET.getParamName(),
        USE_JWT_ASSERTION.getParamName());

    assertMissingProperties(
        gcp_host,
        connectionParamString,
        GOOGLE_SERVICE_ACCOUNT.getParamName(),
        GOOGLE_CREDENTIALS_FILE.getParamName());

    // client credentials auth flow with jwt assertion
    connectionParamString =
        ";transportMode=http;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=11;Auth_Flow=1;UseJWTAssertion=1";
    assertMissingProperties(
        test_host,
        connectionParamString,
        CLIENT_ID.getParamName(),
        CLIENT_SECRET.getParamName(),
        JWT_KEY_FILE.getParamName(),
        JWT_ALGORITHM.getParamName(),
        JWT_PASS_PHRASE.getParamName(),
        JWT_KID.getParamName());

    // browser-based auth flow
    connectionParamString =
        ";transportMode=http;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=11;Auth_Flow=2;";
    assertMissingProperties(
        test_host,
        connectionParamString,
        CLIENT_ID.getParamName(),
        CLIENT_SECRET.getParamName(),
        AUTH_SCOPE.getParamName());

    // proxy based connection
    connectionParamString =
        ";transportMode=http;httpPath=sql/protocolv1/o/6051921418418893/1115-130834-ms4m0yv;AuthMech=11;Auth_Flow=2;useproxy=1";
    assertMissingProperties(
        test_host,
        connectionParamString,
        PROXY_HOST.getParamName(),
        PROXY_USER.getParamName(),
        PROXY_PWD.getParamName(),
        PROXY_PORT.getParamName());
  }
}
