package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.common.DatabricksJdbcUrlParams.*;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.sql.DriverPropertyInfo;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class DatabricksDriverPropertyUtilTest {

  private static final String test_host =
      "jdbc:databricks://sample-host.cloud.databricks.com:9999;transportMode=http;httpPath=sql/protocolv1/o/9999999999999999/9999999999999999;";
  private static final String gcp_host =
      "jdbc:databricks://sample-host.7.gcp.databricks.com:443;transportMode=http;httpPath=sql/protocolv1/o/9999999999999999/9999999999999999;";

  private void assertMissingProperties(String jdbcUrl, String... expectedProperties)
      throws DatabricksSQLException {
    List<DriverPropertyInfo> missingProperties =
        DatabricksDriverPropertyUtil.getMissingProperties(jdbcUrl, new Properties());

    for (String expectedProperty : expectedProperties) {
      assertTrue(
          missingProperties.stream().anyMatch(p -> p.name.equals(expectedProperty)),
          "Missing property: " + expectedProperty);
    }
  }

  @Test
  public void testGetMissingProperties() throws DatabricksSQLException {
    assertMissingProperties(null, "host");

    String jdbcUrl = test_host + "AuthMech=3";
    assertMissingProperties(jdbcUrl, PWD.getParamName());

    // log-level properties
    jdbcUrl = test_host + "AuthMech=3;logLevel=DEBUG;";
    assertMissingProperties(
        jdbcUrl,
        LOG_PATH.getParamName(),
        LOG_FILE_SIZE.getParamName(),
        LOG_FILE_COUNT.getParamName());

    // auth-flow missing OAUTH auth mech
    jdbcUrl = test_host + "AuthMech=11";
    assertMissingProperties(jdbcUrl, AUTH_FLOW.getParamName());

    // TOKEN_PASSTHROUGH with auth-access token.
    jdbcUrl = test_host + "AuthMech=11;Auth_Flow=0";
    assertMissingProperties(
        jdbcUrl, OAUTH_REFRESH_TOKEN.getParamName(), AUTH_ACCESS_TOKEN.getParamName());

    // TOKEN_PASSTHROUGH with refresh token.
    jdbcUrl = test_host + "AuthMech=11;Auth_Flow=0;OAuthRefreshToken=token;OAuthDiscoveryMode=0";
    assertMissingProperties(
        jdbcUrl,
        CLIENT_ID.getParamName(),
        CLIENT_SECRET.getParamName(),
        TOKEN_ENDPOINT.getParamName(),
        DISCOVERY_URL.getParamName());

    // TOKEN_PASSTHROUGH with discovery mode and refresh token.
    jdbcUrl = test_host + "AuthMech=11;Auth_Flow=0;OAuthRefreshToken=token;";
    assertMissingProperties(
        jdbcUrl,
        CLIENT_ID.getParamName(),
        CLIENT_SECRET.getParamName(),
        DISCOVERY_URL.getParamName());

    // client credentials auth flow
    jdbcUrl = test_host + "AuthMech=11;Auth_Flow=1;";
    assertMissingProperties(
        jdbcUrl,
        CLIENT_ID.getParamName(),
        CLIENT_SECRET.getParamName(),
        USE_JWT_ASSERTION.getParamName());

    jdbcUrl = gcp_host + "AuthMech=11;Auth_Flow=1;";
    assertMissingProperties(
        jdbcUrl, GOOGLE_SERVICE_ACCOUNT.getParamName(), GOOGLE_CREDENTIALS_FILE.getParamName());

    // client credentials auth flow with jwt assertion
    jdbcUrl = test_host + "AuthMech=11;Auth_Flow=1;UseJWTAssertion=1";
    assertMissingProperties(
        jdbcUrl,
        CLIENT_ID.getParamName(),
        CLIENT_SECRET.getParamName(),
        JWT_KEY_FILE.getParamName(),
        JWT_ALGORITHM.getParamName(),
        JWT_PASS_PHRASE.getParamName(),
        JWT_KID.getParamName());

    // browser-based auth flow
    jdbcUrl = test_host + "AuthMech=11;Auth_Flow=2;";
    assertMissingProperties(
        jdbcUrl, CLIENT_ID.getParamName(), CLIENT_SECRET.getParamName(), AUTH_SCOPE.getParamName());

    // proxy based connection
    jdbcUrl = test_host + "AuthMech=11;Auth_Flow=2;useproxy=1";
    assertMissingProperties(
        jdbcUrl,
        PROXY_HOST.getParamName(),
        PROXY_USER.getParamName(),
        PROXY_PWD.getParamName(),
        PROXY_PORT.getParamName());
  }
}
