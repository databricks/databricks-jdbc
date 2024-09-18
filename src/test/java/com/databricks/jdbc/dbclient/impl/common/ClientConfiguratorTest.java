package com.databricks.jdbc.dbclient.impl.common;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.auth.PrivateKeyClientCredentialProvider;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.CredentialsProvider;
import com.databricks.sdk.core.DatabricksConfig;
import java.util.List;
import java.util.Properties;
import org.apache.http.config.Registry;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ClientConfiguratorTest {

  @Mock private IDatabricksConnectionContext mockContext;
  private ClientConfigurator configurator;

  @Test
  void getWorkspaceClient_PAT_AuthenticatesWithAccessToken() throws DatabricksParsingException {
    when(mockContext.getAuthMech()).thenReturn(IDatabricksConnectionContext.AuthMech.PAT);
    when(mockContext.getHostUrl()).thenReturn("https://pat.databricks.com");
    when(mockContext.getToken()).thenReturn("pat-token");
    configurator = new ClientConfigurator(mockContext);

    WorkspaceClient client = configurator.getWorkspaceClient();
    assertNotNull(client);
    DatabricksConfig config = client.config();

    assertEquals("https://pat.databricks.com", config.getHost());
    assertEquals("pat-token", config.getToken());
    assertEquals(DatabricksJdbcConstants.ACCESS_TOKEN_AUTH_TYPE, config.getAuthType());
  }

  @Test
  void getWorkspaceClient_OAuthWithTokenPassthrough_AuthenticatesCorrectly()
      throws DatabricksParsingException {
    when(mockContext.getAuthMech()).thenReturn(IDatabricksConnectionContext.AuthMech.OAUTH);
    when(mockContext.getAuthFlow())
        .thenReturn(IDatabricksConnectionContext.AuthFlow.TOKEN_PASSTHROUGH);
    when(mockContext.getHostUrl()).thenReturn("https://oauth-token.databricks.com");
    when(mockContext.getPassThroughAccessToken()).thenReturn("oauth-token");
    configurator = new ClientConfigurator(mockContext);

    WorkspaceClient client = configurator.getWorkspaceClient();
    assertNotNull(client);
    DatabricksConfig config = client.config();

    assertEquals("https://oauth-token.databricks.com", config.getHost());
    assertEquals("oauth-token", config.getToken());
    assertEquals(DatabricksJdbcConstants.ACCESS_TOKEN_AUTH_TYPE, config.getAuthType());
  }

  @Test
  void getWorkspaceClient_OAuthWithClientCredentials_AuthenticatesCorrectly()
      throws DatabricksParsingException {
    when(mockContext.getAuthMech()).thenReturn(IDatabricksConnectionContext.AuthMech.OAUTH);
    when(mockContext.getAuthFlow())
        .thenReturn(IDatabricksConnectionContext.AuthFlow.CLIENT_CREDENTIALS);
    when(mockContext.getHostForOAuth()).thenReturn("https://oauth-client.databricks.com");
    when(mockContext.getClientId()).thenReturn("client-id");
    when(mockContext.getClientSecret()).thenReturn("client-secret");
    configurator = new ClientConfigurator(mockContext);

    WorkspaceClient client = configurator.getWorkspaceClient();
    assertNotNull(client);
    DatabricksConfig config = client.config();

    assertEquals("https://oauth-client.databricks.com", config.getHost());
    assertEquals("client-id", config.getClientId());
    assertEquals("client-secret", config.getClientSecret());
    assertEquals(DatabricksJdbcConstants.M2M_AUTH_TYPE, config.getAuthType());
  }

  @Test
  void testM2MWithJWT() throws DatabricksSQLException {
    String jdbcUrl =
        "jdbc:databricks://adb-565757575.18.azuredatabricks.net:123/default;ssl=1;port=123;AuthMech=11;"
            + "httpPath=/sql/1.0/endpoints/erg6767gg;auth_flow=1;UseJWTAssertion=1;auth_scope=test_scope;"
            + "OAuth2ClientId=test-client;auth_kid=test_kid;Auth_JWT_Key_Passphrase=test_phrase;Auth_JWT_Key_File=test_key_file;"
            + "Auth_JWT_Alg=test_algo;Oauth2TokenEndpoint=token_endpoint";
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(jdbcUrl, new Properties());
    configurator = new ClientConfigurator(connectionContext);
    DatabricksConfig config = configurator.getDatabricksConfig();
    CredentialsProvider provider = config.getCredentialsProvider();
    assertEquals("https://adb-565757575.18.azuredatabricks.net", config.getHost());
    assertEquals("test-client", config.getClientId());
    assertEquals("custom-oauth-m2m", provider.authType());
    assertEquals(DatabricksJdbcConstants.M2M_AUTH_TYPE, config.getAuthType());
    assertEquals(PrivateKeyClientCredentialProvider.class, provider.getClass());
  }

  @Test
  void getWorkspaceClient_OAuthWithBrowserBasedAuthentication_AuthenticatesCorrectly()
      throws DatabricksParsingException {
    when(mockContext.getAuthMech()).thenReturn(IDatabricksConnectionContext.AuthMech.OAUTH);
    when(mockContext.getAuthFlow())
        .thenReturn(IDatabricksConnectionContext.AuthFlow.BROWSER_BASED_AUTHENTICATION);
    when(mockContext.getHostForOAuth()).thenReturn("https://oauth-browser.databricks.com");
    when(mockContext.getClientId()).thenReturn("browser-client-id");
    when(mockContext.getClientSecret()).thenReturn("browser-client-secret");
    when(mockContext.getOAuthScopesForU2M()).thenReturn(List.of(new String[] {"scope1", "scope2"}));
    configurator = new ClientConfigurator(mockContext);
    WorkspaceClient client = configurator.getWorkspaceClient();
    assertNotNull(client);
    DatabricksConfig config = client.config();

    assertEquals("https://oauth-browser.databricks.com", config.getHost());
    assertEquals("browser-client-id", config.getClientId());
    assertEquals("browser-client-secret", config.getClientSecret());
    assertEquals(List.of(new String[] {"scope1", "scope2"}), config.getScopes());
    assertEquals(DatabricksJdbcConstants.U2M_AUTH_REDIRECT_URL, config.getOAuthRedirectUrl());
    assertEquals(DatabricksJdbcConstants.U2M_AUTH_TYPE, config.getAuthType());
  }

  @Test
  void testNonOauth() {
    when(mockContext.getAuthMech()).thenReturn(IDatabricksConnectionContext.AuthMech.OTHER);
    configurator = new ClientConfigurator(mockContext);
    DatabricksConfig config = configurator.getDatabricksConfig();
    assertEquals(DatabricksJdbcConstants.ACCESS_TOKEN_AUTH_TYPE, config.getAuthType());
  }

  @Test
  void testNonProxyHostsFormatConversion() {
    String nonProxyHostsInput = ".example.com,.blabla.net,.xyz.abc";
    assertEquals(
        "*.example.com|*.blabla.net|*.xyz.abc",
        ClientConfigurator.convertNonProxyHostConfigToBeSystemPropertyCompliant(
            nonProxyHostsInput));

    String nonProxyHostsInput2 = "example.com,.blabla.net,123.xyz.abc";
    assertEquals(
        "example.com|*.blabla.net|123.xyz.abc",
        ClientConfigurator.convertNonProxyHostConfigToBeSystemPropertyCompliant(
            nonProxyHostsInput2));

    String nonProxyHostsInput3 = "staging.example.*|blabla.net|*.xyz.abc";
    assertEquals(
        "staging.example.*|blabla.net|*.xyz.abc",
        ClientConfigurator.convertNonProxyHostConfigToBeSystemPropertyCompliant(
            nonProxyHostsInput3));
  }

  @Test
  void testGetConnectionSocketFactoryRegistry() {
    when(mockContext.getSSLTrustStore())
        .thenReturn("src/test/resources/ssltruststore/empty-truststore.jks");
    when(mockContext.getSSLTrustStorePassword()).thenReturn("changeit");
    when(mockContext.getSSLTrustStoreType()).thenReturn("PKCS12");
    Registry<ConnectionSocketFactory> registry =
        ClientConfigurator.getConnectionSocketFactoryRegistry(mockContext);
    assertInstanceOf(SSLConnectionSocketFactory.class, registry.lookup("https"));
    assertInstanceOf(PlainConnectionSocketFactory.class, registry.lookup("http"));
  }
}
