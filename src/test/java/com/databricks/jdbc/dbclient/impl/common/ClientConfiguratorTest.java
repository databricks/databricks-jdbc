package com.databricks.jdbc.dbclient.impl.common;

import static com.databricks.jdbc.TestConstants.TEST_DISCOVERY_URL;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.GCP_GOOGLE_CREDENTIALS_AUTH_TYPE;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.M2M_AZURE_CLIENT_SECRET_AUTH_TYPE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.auth.PrivateKeyClientCredentialProvider;
import com.databricks.jdbc.common.AuthFlow;
import com.databricks.jdbc.common.AuthMech;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.CredentialsProvider;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.ProxyConfig;
import com.databricks.sdk.core.commons.CommonsHttpClient;
import com.databricks.sdk.core.utils.Cloud;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ClientConfiguratorTest {
  @Mock private IDatabricksConnectionContext mockContext;
  private ClientConfigurator configurator;

  @Test
  void getWorkspaceClient_PAT_AuthenticatesWithAccessToken() throws DatabricksParsingException {
    when(mockContext.getAuthMech()).thenReturn(AuthMech.PAT);
    when(mockContext.getHostUrl()).thenReturn("https://pat.databricks.com");
    when(mockContext.getToken()).thenReturn("pat-token");
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
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
    when(mockContext.getAuthMech()).thenReturn(AuthMech.OAUTH);
    when(mockContext.getAuthFlow()).thenReturn(AuthFlow.TOKEN_PASSTHROUGH);
    when(mockContext.getHostUrl()).thenReturn("https://oauth-token.databricks.com");
    when(mockContext.getPassThroughAccessToken()).thenReturn("oauth-token");
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
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
    when(mockContext.getAuthMech()).thenReturn(AuthMech.OAUTH);
    when(mockContext.getAuthFlow()).thenReturn(AuthFlow.CLIENT_CREDENTIALS);
    when(mockContext.getHostForOAuth()).thenReturn("https://oauth-client.databricks.com");
    when(mockContext.getClientId()).thenReturn("client-id");
    when(mockContext.getClientSecret()).thenReturn("client-secret");
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
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
  void getWorkspaceClient_OAuthWithClientCredentials_AuthenticatesCorrectlyGCP()
      throws DatabricksParsingException {
    when(mockContext.getAuthMech()).thenReturn(AuthMech.OAUTH);
    when(mockContext.getAuthFlow()).thenReturn(AuthFlow.CLIENT_CREDENTIALS);
    when(mockContext.getHostForOAuth()).thenReturn("https://oauth-client.databricks.com");
    when(mockContext.getCloud()).thenReturn(Cloud.GCP);
    when(mockContext.getGcpAuthType()).thenReturn(GCP_GOOGLE_CREDENTIALS_AUTH_TYPE);
    when(mockContext.getGoogleCredentials()).thenReturn("google-credentials");
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
    configurator = new ClientConfigurator(mockContext);
    WorkspaceClient client = configurator.getWorkspaceClient();
    assertNotNull(client);
    DatabricksConfig config = client.config();

    assertEquals("https://oauth-client.databricks.com", config.getHost());
    assertEquals("google-credentials", config.getGoogleCredentials());
    assertEquals(GCP_GOOGLE_CREDENTIALS_AUTH_TYPE, config.getAuthType());
  }

  @Test
  void getWorkspaceClient_OAuthWithClientCredentials_AuthenticatesCorrectlyWithJWT()
      throws DatabricksParsingException {
    when(mockContext.getConnectionUuid()).thenReturn("connection-uuid");
    when(mockContext.getAuthMech()).thenReturn(AuthMech.OAUTH);
    when(mockContext.getAuthFlow()).thenReturn(AuthFlow.CLIENT_CREDENTIALS);
    when(mockContext.getHostForOAuth()).thenReturn("https://jwt-auth.databricks.com");
    when(mockContext.getClientId()).thenReturn("client-id");
    when(mockContext.getClientSecret()).thenReturn("client-secret");
    when(mockContext.useJWTAssertion()).thenReturn(true);
    when(mockContext.getTokenEndpoint()).thenReturn("token-endpoint");
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
    configurator = new ClientConfigurator(mockContext);

    WorkspaceClient client = configurator.getWorkspaceClient();
    assertNotNull(client);
    DatabricksConfig config = client.config();

    assertEquals("https://jwt-auth.databricks.com", config.getHost());
    assertEquals("client-id", config.getClientId());
    assertEquals("client-secret", config.getClientSecret());
    assertEquals(DatabricksJdbcConstants.M2M_AUTH_TYPE, config.getAuthType());
    CredentialsProvider provider = config.getCredentialsProvider();
    assertNotNull(provider);
    assertEquals(PrivateKeyClientCredentialProvider.class, provider.getClass());
    assertEquals("custom-oauth-m2m", provider.authType());
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
    when(mockContext.getAuthMech()).thenReturn(AuthMech.OAUTH);
    when(mockContext.getAuthFlow()).thenReturn(AuthFlow.BROWSER_BASED_AUTHENTICATION);
    when(mockContext.getHostForOAuth()).thenReturn("https://oauth-browser.databricks.com");
    when(mockContext.getClientId()).thenReturn("browser-client-id");
    when(mockContext.getClientSecret()).thenReturn("browser-client-secret");
    when(mockContext.getOAuthScopesForU2M()).thenReturn(List.of(new String[] {"scope1", "scope2"}));
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
    when(mockContext.getOAuth2RedirectUrlPorts()).thenReturn(List.of(8020));
    configurator = new ClientConfigurator(mockContext);
    WorkspaceClient client = configurator.getWorkspaceClient();
    assertNotNull(client);
    DatabricksConfig config = client.config();

    assertEquals("https://oauth-browser.databricks.com", config.getHost());
    assertEquals("browser-client-id", config.getClientId());
    assertEquals("browser-client-secret", config.getClientSecret());
    assertEquals(List.of(new String[] {"scope1", "scope2"}), config.getScopes());
    assertEquals("http://localhost:8020", config.getOAuthRedirectUrl());
    assertEquals(DatabricksJdbcConstants.U2M_AUTH_TYPE, config.getAuthType());
  }

  @Test
  void
      getWorkspaceClient_OAuthWithBrowserBasedAuthentication_WithDiscoveryURL_AuthenticatesCorrectly()
          throws DatabricksParsingException, IOException {
    when(mockContext.getAuthMech()).thenReturn(AuthMech.OAUTH);
    when(mockContext.getAuthFlow()).thenReturn(AuthFlow.BROWSER_BASED_AUTHENTICATION);
    when(mockContext.getHostForOAuth()).thenReturn("https://oauth-browser.databricks.com");
    when(mockContext.getClientId()).thenReturn("browser-client-id");
    when(mockContext.getClientSecret()).thenReturn("browser-client-secret");
    when(mockContext.getOAuthScopesForU2M()).thenReturn(List.of(new String[] {"scope1", "scope2"}));
    when(mockContext.isOAuthDiscoveryModeEnabled()).thenReturn(true);
    when(mockContext.getOAuthDiscoveryURL()).thenReturn(TEST_DISCOVERY_URL);
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
    when(mockContext.getOAuth2RedirectUrlPorts()).thenReturn(List.of(8020));
    configurator = new ClientConfigurator(mockContext);
    WorkspaceClient client = configurator.getWorkspaceClient();
    assertNotNull(client);
    DatabricksConfig config = client.config();

    assertEquals(TEST_DISCOVERY_URL, config.getDiscoveryUrl());
    assertEquals("https://oauth-browser.databricks.com", config.getHost());
    assertEquals("browser-client-id", config.getClientId());
    assertEquals("browser-client-secret", config.getClientSecret());
    assertEquals(List.of(new String[] {"scope1", "scope2"}), config.getScopes());
    assertEquals("http://localhost:8020", config.getOAuthRedirectUrl());
    assertEquals(DatabricksJdbcConstants.U2M_AUTH_TYPE, config.getAuthType());
  }

  @Test
  void testNonOauth() {
    when(mockContext.getAuthMech()).thenReturn(AuthMech.OTHER);
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
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
  void testSetupProxyConfig() {
    when(mockContext.getAuthMech()).thenReturn(AuthMech.PAT);
    when(mockContext.getUseProxy()).thenReturn(true);
    when(mockContext.getProxyHost()).thenReturn("proxy.host.com");
    when(mockContext.getProxyPort()).thenReturn(3128);
    when(mockContext.getProxyUser()).thenReturn("proxyUser");
    when(mockContext.getProxyPassword()).thenReturn("proxyPass");
    when(mockContext.getProxyAuthType()).thenReturn(ProxyConfig.ProxyAuthType.values()[0]);
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
    // For non-proxy hosts conversion, an input of ".example.com,localhost"
    // is expected to be converted to "*.example.com|localhost"
    when(mockContext.getNonProxyHosts()).thenReturn(".example.com,localhost");
    configurator = new ClientConfigurator(mockContext);
    CommonsHttpClient.Builder builder = mock(CommonsHttpClient.Builder.class);

    configurator.setupProxyConfig(builder);

    ArgumentCaptor<ProxyConfig> captor = ArgumentCaptor.forClass(ProxyConfig.class);
    verify(builder).withProxyConfig(captor.capture());
    ProxyConfig proxyConfig = captor.getValue();

    // Verify that the ProxyConfig is set as expected.
    assertEquals("proxy.host.com", proxyConfig.getHost());
    assertEquals(3128, proxyConfig.getPort());
    assertEquals("proxyUser", proxyConfig.getUsername());
    assertEquals("proxyPass", proxyConfig.getPassword());
    assertEquals("*.example.com|localhost", proxyConfig.getNonProxyHosts());
  }

  @Test
  void setupM2MConfig_WithAzureTenantId_ConfiguresCorrectly() throws DatabricksParsingException {
    when(mockContext.getAuthMech()).thenReturn(AuthMech.OAUTH);
    when(mockContext.getAuthFlow()).thenReturn(AuthFlow.CLIENT_CREDENTIALS);
    when(mockContext.getHostForOAuth()).thenReturn("https://azure-oauth.databricks.com");
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
    when(mockContext.getAzureTenantId()).thenReturn("azure-tenant-id");
    when(mockContext.getCloud()).thenReturn(Cloud.AZURE);
    when(mockContext.getClientId()).thenReturn("azure-client-id");
    when(mockContext.getClientSecret()).thenReturn("azure-client-secret");

    configurator = new ClientConfigurator(mockContext);

    DatabricksConfig config = configurator.getDatabricksConfig();
    assertEquals("https://azure-oauth.databricks.com", config.getHost());
    assertEquals(M2M_AZURE_CLIENT_SECRET_AUTH_TYPE, config.getAuthType());
    assertEquals("azure-client-id", config.getAzureClientId());
    assertEquals("azure-client-secret", config.getAzureClientSecret());
    assertEquals("azure-tenant-id", config.getAzureTenantId());

    verify(mockContext, times(2)).getAzureTenantId();
    verify(mockContext, times(2)).getCloud();
    verify(mockContext).getClientId();
    verify(mockContext).getClientSecret();
  }

  @Test
  void setupM2MConfig_WithAzureTenantIdButNonAzureCloud_ThrowsException()
      throws DatabricksParsingException {
    when(mockContext.getAuthMech()).thenReturn(AuthMech.OAUTH);
    when(mockContext.getAuthFlow()).thenReturn(AuthFlow.CLIENT_CREDENTIALS);
    when(mockContext.getHostForOAuth()).thenReturn("https://azure-oauth.databricks.com");
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
    when(mockContext.getAzureTenantId()).thenReturn("azure-tenant-id");
    when(mockContext.getCloud()).thenReturn(Cloud.AWS);

    DatabricksException exception =
        assertThrows(DatabricksException.class, () -> new ClientConfigurator(mockContext));

    assertEquals(
        "Azure client credentials flow is only supported for Azure cloud",
        exception.getCause().getMessage());

    verify(mockContext).getAzureTenantId();
    verify(mockContext, times(2)).getCloud();
  }

  @Test
  void testFindAvailablePort() throws Exception {
    // Create a mockContext for the ClientConfigurator constructor
    when(mockContext.getAuthMech()).thenReturn(AuthMech.PAT);
    when(mockContext.getHostUrl()).thenReturn("https://test.databricks.com");
    when(mockContext.getToken()).thenReturn("test-token");
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
    configurator = new ClientConfigurator(mockContext);

    // Test with a single available port
    int availablePort = findFreePort();
    List<Integer> ports = List.of(availablePort);
    int result = configurator.findAvailablePort(ports);
    assertEquals(availablePort, result);

    // Test with multiple ports, first unavailable
    int secondAvailablePort = findFreePort();
    try (ServerSocket serverSocket = new ServerSocket(availablePort)) {
      serverSocket.setReuseAddress(true);
      ports = List.of(availablePort, secondAvailablePort);
      result = configurator.findAvailablePort(ports);
      assertEquals(secondAvailablePort, result);
    }

    // Test incremental search - first port unavailable, second available
    try (ServerSocket serverSocket = new ServerSocket(availablePort)) {
      serverSocket.setReuseAddress(true);
      ports = List.of(availablePort);
      result = configurator.findAvailablePort(ports);
      assertEquals(availablePort + 1, result);
    }
  }

  @Test
  void testFindAvailablePortThrowsExceptionWhenNoPortsAvailable() throws Exception {
    // Create a mockContext for the ClientConfigurator constructor
    when(mockContext.getAuthMech()).thenReturn(AuthMech.PAT);
    when(mockContext.getHostUrl()).thenReturn("https://test.databricks.com");
    when(mockContext.getToken()).thenReturn("test-token");
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
    configurator = new ClientConfigurator(mockContext);

    // Use a port that is likely to be available
    int port1 = findFreePort();
    int port2 = findFreePort();
    if (port1 == port2) {
      port2 = port1 + 1;
    }

    // Occupy the ports to make them unavailable
    try (ServerSocket socket1 = new ServerSocket(port1);
        ServerSocket socket2 = new ServerSocket(port2)) {
      socket1.setReuseAddress(true);
      socket2.setReuseAddress(true);

      // First test with multiple specified ports
      List<Integer> unavailablePorts = List.of(port1, port2);
      DatabricksException exception =
          assertThrows(
              DatabricksException.class, () -> configurator.findAvailablePort(unavailablePorts));
      assertTrue(exception.getMessage().contains("No available port found"));

      // Now test with single port and verify it tries incremental ports
      // We need to create a subclass to control isPortAvailable behavior
      ClientConfigurator testConfigurator =
          new ClientConfigurator(mockContext) {
            @Override
            protected boolean isPortAvailable(int port) {
              return false; // All ports are unavailable
            }
          };

      exception =
          assertThrows(
              DatabricksException.class, () -> testConfigurator.findAvailablePort(List.of(port1)));
      assertTrue(exception.getMessage().contains("No available port found"));
    }
  }

  /** Utility method to find a free port */
  private int findFreePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(true);
      return socket.getLocalPort();
    } catch (IOException e) {
      throw new RuntimeException("Failed to find free port", e);
    }
  }

  @Test
  void getWorkspaceClient_OAuthWithBrowserBasedAuthentication_SetsCustomRedirectUrl()
      throws Exception {
    // We'll mock getOAuth2RedirectUrlPorts to return a predefined list
    int testPort = findFreePort();
    when(mockContext.getAuthMech()).thenReturn(AuthMech.OAUTH);
    when(mockContext.getAuthFlow()).thenReturn(AuthFlow.BROWSER_BASED_AUTHENTICATION);
    when(mockContext.getHostForOAuth()).thenReturn("https://oauth-browser.databricks.com");
    when(mockContext.getClientId()).thenReturn("browser-client-id");
    when(mockContext.getClientSecret()).thenReturn("browser-client-secret");
    when(mockContext.getOAuthScopesForU2M()).thenReturn(List.of(new String[] {"scope1", "scope2"}));
    when(mockContext.getOAuth2RedirectUrlPorts()).thenReturn(List.of(testPort));
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);

    configurator = new ClientConfigurator(mockContext);
    WorkspaceClient client = configurator.getWorkspaceClient();
    assertNotNull(client);
    DatabricksConfig config = client.config();

    assertEquals("https://oauth-browser.databricks.com", config.getHost());
    assertEquals("browser-client-id", config.getClientId());
    assertEquals("browser-client-secret", config.getClientSecret());
    assertEquals(List.of(new String[] {"scope1", "scope2"}), config.getScopes());
    assertEquals("http://localhost:" + testPort, config.getOAuthRedirectUrl());
    assertEquals(DatabricksJdbcConstants.U2M_AUTH_TYPE, config.getAuthType());
  }
}
