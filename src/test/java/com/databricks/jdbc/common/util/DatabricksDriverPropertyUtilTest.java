package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.common.DatabricksJdbcUrlParams.*;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.sql.DriverPropertyInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

public class DatabricksDriverPropertyUtilTest {

  private static final String HOST_ONLY_URL = "jdbc:databricks://sample-host.cloud.databricks.com";
  private static final List<String> OPTIONAL_PROPERTY_NAMES =
      Arrays.asList(
          "ssl",
          "loglevel",
          "useproxy",
          "usethriftclient",
          "EnableArrow",
          "EnableDirectResults",
          "QueryResultCompressionType",
          "EnableQueryResultLZ4Compression",
          "useragententry",
          "EnableSQLExecHybridResults",
          "EnableSQLExecDirectResults",
          "EnableComplexDatatypeSupport",
          "RowsFetchedPerBlock",
          "DefaultStringColumnLength",
          "socketTimeout",
          "EnableTokenCache",
          "EnableSQLValidationForIsValid",
          "DisableOauthRefreshToken",
          "enableMultipleCatalogSupport",
          "IgnoreTransactions");

  @ParameterizedTest
  @MethodSource("patDiscoveryCases")
  void reportsEveryRequiredPropertyForEffectivePat(String options, List<String> requiredNames)
      throws DatabricksSQLException {
    assertPropertyInfos(
        HOST_ONLY_URL + options, new Properties(), requiredNames, OPTIONAL_PROPERTY_NAMES);
  }

  private static Stream<Arguments> patDiscoveryCases() {
    return Stream.of(
        Arguments.of("", Arrays.asList("authmech", "httppath", "pwd")),
        Arguments.of("/", Arrays.asList("authmech", "httppath", "pwd")),
        Arguments.of(";httpPath=/warehouse", Arrays.asList("authmech", "pwd")),
        Arguments.of(";AuthMech=3", Arrays.asList("httppath", "pwd")),
        Arguments.of(";AuthMech=3;httpPath=/warehouse", Collections.singletonList("pwd")),
        Arguments.of(";AuthMech=3;httpPath=/warehouse;PWD=local-test", Collections.emptyList()),
        Arguments.of(";httpPath=", Arrays.asList("authmech", "httppath", "pwd")),
        Arguments.of(";httpPath= \t", Arrays.asList("authmech", "httppath", "pwd")),
        Arguments.of(";httpPath=\u2003", Arrays.asList("authmech", "httppath", "pwd")),
        Arguments.of(";AuthMech=3;PWD= \t", Arrays.asList("httppath", "pwd")),
        Arguments.of(";AuthMech=", Arrays.asList("authmech", "httppath", "pwd")),
        Arguments.of(";AuthMech= \t", Arrays.asList("authmech", "httppath", "pwd")));
  }

  @ParameterizedTest
  @ValueSource(strings = {"99", "invalid"})
  void reportsUnsupportedAuthMechInsteadOfThrowing(String authMech) throws DatabricksSQLException {
    assertPropertyInfos(
        HOST_ONLY_URL + ";AuthMech=" + authMech,
        new Properties(),
        Arrays.asList("httppath", "authmech"),
        OPTIONAL_PROPERTY_NAMES);
    assertPropertyInfos(
        HOST_ONLY_URL + ";httpPath=/warehouse;AuthMech=" + authMech,
        new Properties(),
        Collections.singletonList("authmech"),
        OPTIONAL_PROPERTY_NAMES);
  }

  @ParameterizedTest
  @MethodSource("oauthDiscoveryCases")
  void reportsOAuthRequirementsWithoutPwd(
      String options, List<String> requiredNames, List<String> additionalOptionalNames)
      throws DatabricksSQLException {
    List<String> optionalNames = new ArrayList<>(OPTIONAL_PROPERTY_NAMES);
    optionalNames.add("Identity_Federation_Client_Id");
    optionalNames.addAll(additionalOptionalNames);
    assertPropertyInfos(HOST_ONLY_URL + options, new Properties(), requiredNames, optionalNames);
  }

  private static Stream<Arguments> oauthDiscoveryCases() {
    List<String> browserOptionalNames =
        Arrays.asList(
            "OAuth2ClientId",
            "OAuth2Secret",
            "Auth_Scope",
            "OAuth2RedirectUrlPort",
            "EnableTokenCache");
    return Stream.of(
        Arguments.of(
            ";AuthMech=11", Arrays.asList("httppath", "auth_flow"), Collections.emptyList()),
        Arguments.of(
            ";AuthMech=11;Auth_Flow=",
            Arrays.asList("httppath", "auth_flow"),
            Collections.emptyList()),
        Arguments.of(
            ";AuthMech=11;Auth_Flow= \t",
            Arrays.asList("httppath", "auth_flow"),
            Collections.emptyList()),
        Arguments.of(
            ";AuthMech=11;Auth_Flow=0",
            Arrays.asList("httppath", "Auth_AccessToken"),
            Collections.singletonList("Auth_RefreshToken")),
        Arguments.of(
            ";AuthMech=11;Auth_Flow=0;OAuthRefreshToken= ",
            Arrays.asList("httppath", "Auth_AccessToken"),
            Collections.singletonList("Auth_RefreshToken")),
        Arguments.of(
            ";AuthMech=11;Auth_Flow=1",
            Arrays.asList("httppath", "OAuth2Secret", "OAuth2ClientId"),
            Collections.singletonList("UseJWTAssertion")),
        Arguments.of(
            ";AuthMech=11;Auth_Flow=1;OAuth2ClientId= ;OAuth2Secret= ",
            Arrays.asList("httppath", "OAuth2Secret", "OAuth2ClientId"),
            Collections.singletonList("UseJWTAssertion")),
        Arguments.of(
            ";AuthMech=11;Auth_Flow=2",
            Collections.singletonList("httppath"),
            browserOptionalNames),
        Arguments.of(
            ";httpPath=/warehouse;AuthMech=11;Auth_Flow=2",
            Collections.emptyList(),
            browserOptionalNames),
        Arguments.of(
            ";AuthMech=11;Auth_Flow=3",
            Arrays.asList("httppath", "azure_workspace_resource_id"),
            Collections.emptyList()),
        Arguments.of(
            ";AuthMech=11;Auth_Flow=0;OAuthRefreshToken=refresh",
            Arrays.asList("httppath", "OAuth2ClientId", "OAuth2Secret", "OAuthDiscoveryURL"),
            Collections.emptyList()),
        Arguments.of(
            ";AuthMech=11;Auth_Flow=0;OAuthRefreshToken=refresh;OAuth2TokenEndpoint= ;OAuthDiscoveryURL= ",
            Arrays.asList("httppath", "OAuth2ClientId", "OAuth2Secret", "OAuthDiscoveryURL"),
            Collections.emptyList()));
  }

  @Test
  void honorsCaseInsensitivePropertiesAndUrlPrecedence() throws DatabricksSQLException {
    Properties properties = new Properties();
    properties.setProperty("HTTPPATH", "");
    properties.setProperty("AUTHMECH", "3");
    assertPropertyInfos(
        HOST_ONLY_URL, properties, Arrays.asList("httppath", "pwd"), OPTIONAL_PROPERTY_NAMES);
    assertPropertyInfos(
        HOST_ONLY_URL + ";httpPath=/warehouse;AuthMech=11;Auth_Flow=0",
        properties,
        Collections.singletonList("Auth_AccessToken"),
        Stream.concat(
                OPTIONAL_PROPERTY_NAMES.stream(),
                Stream.of("Identity_Federation_Client_Id", "Auth_RefreshToken"))
            .collect(Collectors.toList()));
  }

  @Test
  void reportsRequiredProxyPropertiesBeforeOptionalEntries() throws DatabricksSQLException {
    List<String> optionalNames = new ArrayList<>(OPTIONAL_PROPERTY_NAMES);
    optionalNames.remove("useproxy");
    optionalNames.addAll(Arrays.asList("proxyuid", "proxypwd"));
    assertPropertyInfos(
        HOST_ONLY_URL + ";AuthMech=3;UseProxy=1",
        new Properties(),
        Arrays.asList("httppath", "pwd", "proxyhost", "proxyport"),
        optionalNames);
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {"not-a-jdbc-url", "jdbc:other://localhost:8080"})
  void preservesUnparseableUrlHostPrompt(String url) throws DatabricksSQLException {
    assertPropertyInfos(
        url, new Properties(), Collections.singletonList("host"), Collections.emptyList());
  }

  private static void assertPropertyInfos(
      String url, Properties properties, List<String> requiredNames, List<String> optionalNames)
      throws DatabricksSQLException {
    List<String> expected = new ArrayList<>();
    requiredNames.forEach(name -> expected.add("required:" + name));
    optionalNames.forEach(name -> expected.add("optional:" + name));
    List<DriverPropertyInfo> actual =
        DatabricksDriverPropertyUtil.getMissingProperties(url, properties);

    assertEquals(
        expected,
        actual.stream()
            .map(property -> (property.required ? "required:" : "optional:") + property.name)
            .collect(Collectors.toList()));
    actual.forEach(property -> assertNotNull(property.description));
  }

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

  private void assertNotMissingProperties(
      String jdbcUrl, Properties info, String... notExpectedProperties)
      throws DatabricksSQLException {
    List<DriverPropertyInfo> missingProperties =
        DatabricksDriverPropertyUtil.getMissingProperties(jdbcUrl, info);
    for (String notExpected : notExpectedProperties) {
      assertFalse(
          missingProperties.stream().anyMatch(p -> p.name.equals(notExpected)),
          "Should not report missing property: " + notExpected);
    }
  }

  @Test
  public void testM2MSecretFromPasswordNotReportedMissing() throws DatabricksSQLException {
    // When user/password supply the OAuth client id/secret, they must not be flagged as missing
    // (issue #1132).
    String jdbcUrl = test_host + "AuthMech=11;Auth_Flow=1;";
    Properties info = new Properties();
    info.setProperty("user", "my-client-id");
    info.setProperty("password", "my-oauth-secret");
    assertNotMissingProperties(
        jdbcUrl, info, CLIENT_ID.getParamName(), CLIENT_SECRET.getParamName());
  }

  @Test
  public void testM2MMissingSecretStillReportedWithoutPassword() throws DatabricksSQLException {
    // Without user/password and without explicit OAuth params, both are still reported missing.
    String jdbcUrl = test_host + "AuthMech=11;Auth_Flow=1;";
    assertMissingProperties(jdbcUrl, CLIENT_ID.getParamName(), CLIENT_SECRET.getParamName());
  }
}
