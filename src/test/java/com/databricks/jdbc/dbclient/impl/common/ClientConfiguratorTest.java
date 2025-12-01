package com.databricks.jdbc.dbclient.impl.common;

import static com.databricks.jdbc.TestConstants.TEST_DISCOVERY_URL;
import static com.databricks.jdbc.TestConstants.TEST_SCOPE_STRING;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.GCP_GOOGLE_CREDENTIALS_AUTH_TYPE;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.M2M_AZURE_CLIENT_SECRET_AUTH_TYPE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.auth.DatabricksTokenFederationProvider;
import com.databricks.jdbc.auth.PrivateKeyClientCredentialProvider;
import com.databricks.jdbc.common.AuthFlow;
import com.databricks.jdbc.common.AuthMech;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksSSLException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.ProxyConfig;
import com.databricks.sdk.core.commons.CommonsHttpClient;
import com.databricks.sdk.core.oauth.ExternalBrowserCredentialsProvider;
import com.databricks.sdk.core.utils.Cloud;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Collections;
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
  void getWorkspaceClient_PAT_AuthenticatesWithAccessToken()
      throws DatabricksParsingException, DatabricksSSLException, DatabricksValidationException {
    when(mockContext.getAuthMech()).thenReturn(AuthMech.PAT);
    when(mockContext.getHostUrl()).thenReturn("https://pat.databricks.com");
    when(mockContext.getToken()).thenReturn("pat-token");
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
    when(mockContext.getHttpMaxConnectionsPerRoute()).thenReturn(100);
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
      throws DatabricksParsingException, DatabricksSSLException, DatabricksValidationException {
    when(mockContext.getAuthMech()).thenReturn(AuthMech.OAUTH);
    when(mockContext.getAuthFlow()).thenReturn(AuthFlow.TOKEN_PASSTHROUGH);
    when(mockContext.getHostUrl()).thenReturn("https://oauth-token.databricks.com");
    when(mockContext.getPassThroughAccessToken()).thenReturn("oauth-token");
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
    when(mockContext.getHttpMaxConnectionsPerRoute()).thenReturn(100);
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
      throws DatabricksParsingException, DatabricksSSLException, DatabricksValidationException {
    when(mockContext.getAuthMech()).thenReturn(AuthMech.OAUTH);
    when(mockContext.getAuthFlow()).thenReturn(AuthFlow.CLIENT_CREDENTIALS);
    when(mockContext.getHostForOAuth()).thenReturn("https://oauth-client.databricks.com");
    when(mockContext.getClientId()).thenReturn("client-id");
    when(mockContext.getClientSecret()).thenReturn("client-secret");
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
    when(mockContext.getHttpMaxConnectionsPerRoute()).thenReturn(100);
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
      throws DatabricksParsingException, DatabricksSSLException, DatabricksValidationException {
    when(mockContext.getAuthMech()).thenReturn(AuthMech.OAUTH);
    when(mockContext.getAuthFlow()).thenReturn(AuthFlow.CLIENT_CREDENTIALS);
    when(mockContext.getHostForOAuth()).thenReturn("https://oauth-client.databricks.com");
    when(mockContext.getCloud()).thenReturn(Cloud.GCP);
    when(mockContext.getGcpAuthType()).thenReturn(GCP_GOOGLE_CREDENTIALS_AUTH_TYPE);
    when(mockContext.getGoogleCredentials()).thenReturn("google-credentials");
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
    when(mockContext.getHttpMaxConnectionsPerRoute()).thenReturn(100);
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
      throws DatabricksParsingException, DatabricksSSLException, DatabricksValidationException {
    when(mockContext.getConnectionUuid()).thenReturn("connection-uuid");
    when(mockContext.getAuthMech()).thenReturn(AuthMech.OAUTH);
    when(mockContext.getAuthFlow()).thenReturn(AuthFlow.CLIENT_CREDENTIALS);
    when(mockContext.getHostForOAuth()).thenReturn("https://jwt-auth.databricks.com");
    when(mockContext.getClientId()).thenReturn("client-id");
    when(mockContext.getClientSecret()).thenReturn("client-secret");
    when(mockContext.useJWTAssertion()).thenReturn(true);
    when(mockContext.getTokenEndpoint()).thenReturn("token-endpoint");
    when(mockContext.isTokenFederationEnabled()).thenReturn(true);
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
    when(mockContext.getHttpMaxConnectionsPerRoute()).thenReturn(100);
    configurator = new ClientConfigurator(mockContext);

    WorkspaceClient client = configurator.getWorkspaceClient();
    assertNotNull(client);
    DatabricksConfig config = client.config();

    assertEquals("https://jwt-auth.databricks.com", config.getHost());
    assertEquals("client-id", config.getClientId());
    assertEquals("client-secret", config.getClientSecret());
    assertEquals("custom-oauth-m2m", config.getAuthType());
    DatabricksTokenFederationProvider provider =
        (DatabricksTokenFederationProvider) config.getCredentialsProvider();
    assertNotNull(provider);
    assertEquals(
        PrivateKeyClientCredentialProvider.class, provider.getCredentialsProvider().getClass());
    assertEquals("custom-oauth-m2m", provider.authType());
  }

  @Test
  void testM2MWithJWT() throws DatabricksSQLException {
    String jdbcUrl =
        "jdbc:databricks://sample-host.18.azuredatabricks.net:123/default;ssl=1;port=123;AuthMech=11;"
            + "httpPath=/sql/1.0/endpoints/99999999;auth_flow=1;UseJWTAssertion=1;auth_scope=test_scope;"
            + "OAuth2ClientId=test-client;auth_kid=test_kid;Auth_JWT_Key_Passphrase=test_phrase;Auth_JWT_Key_File=test_key_file;"
            + "Auth_JWT_Alg=test_algo;Oauth2TokenEndpoint=token_endpoint";
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(jdbcUrl, new Properties());
    configurator = new ClientConfigurator(connectionContext);
    DatabricksConfig config = configurator.getDatabricksConfig();
    DatabricksTokenFederationProvider provider =
        (DatabricksTokenFederationProvider) config.getCredentialsProvider();
    assertEquals("https://sample-host.18.azuredatabricks.net", config.getHost());
    assertEquals("test-client", config.getClientId());
    assertEquals("custom-oauth-m2m", provider.authType());
    assertEquals(provider.authType(), config.getAuthType());
    assertEquals(
        PrivateKeyClientCredentialProvider.class, provider.getCredentialsProvider().getClass());
  }

  @Test
  void getWorkspaceClient_OAuthWithBrowserBasedAuthentication_AuthenticatesCorrectly()
      throws DatabricksParsingException, DatabricksSSLException, DatabricksValidationException {
    when(mockContext.getAuthMech()).thenReturn(AuthMech.OAUTH);
    when(mockContext.getAuthFlow()).thenReturn(AuthFlow.BROWSER_BASED_AUTHENTICATION);
    when(mockContext.getHostForOAuth()).thenReturn("https://oauth-browser.databricks.com");
    when(mockContext.getClientId()).thenReturn("browser-client-id");
    when(mockContext.getClientSecret()).thenReturn("browser-client-secret");
    when(mockContext.getOAuthScopesForU2M()).thenReturn(List.of("scope1", "scope2"));
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
    when(mockContext.getOAuth2RedirectUrlPorts()).thenReturn(List.of(8020));
    when(mockContext.getHttpMaxConnectionsPerRoute()).thenReturn(100);
    when(mockContext.getDisableOauthRefreshToken()).thenReturn(true);
    configurator = new ClientConfigurator(mockContext);
    WorkspaceClient client = configurator.getWorkspaceClient();
    assertNotNull(client);
    DatabricksConfig config = client.config();

    assertEquals("https://oauth-browser.databricks.com", config.getHost());
    assertEquals("browser-client-id", config.getClientId());
    assertEquals("browser-client-secret", config.getClientSecret());
    assertEquals(List.of("scope1", "scope2"), config.getScopes());
    assertEquals("http://localhost:8020", config.getOAuthRedirectUrl());
    assertEquals(DatabricksJdbcConstants.U2M_AUTH_TYPE, config.getAuthType());
  }

  @Test
  void getWorkspaceClient_OAuthWithBrowserBasedAuthentication_ScopesExcludeOfflineAccess()
      throws DatabricksParsingException, DatabricksSSLException, DatabricksValidationException {
    when(mockContext.getAuthMech()).thenReturn(AuthMech.OAUTH);
    when(mockContext.getAuthFlow()).thenReturn(AuthFlow.BROWSER_BASED_AUTHENTICATION);
    when(mockContext.getHostForOAuth()).thenReturn("https://oauth-browser.databricks.com");
    when(mockContext.getClientId()).thenReturn("browser-client-id");
    when(mockContext.getClientSecret()).thenReturn("browser-client-secret");
    when(mockContext.getOAuthScopesForU2M()).thenReturn(List.of("scope.read", "scope.write"));
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
    when(mockContext.getOAuth2RedirectUrlPorts()).thenReturn(List.of(8030));
    when(mockContext.getHttpMaxConnectionsPerRoute()).thenReturn(100);
    when(mockContext.getDisableOauthRefreshToken()).thenReturn(true);

    configurator = new ClientConfigurator(mockContext);
    DatabricksConfig config = configurator.getDatabricksConfig();

    // Driver disables refresh tokens for OAuth, so SDK must not include offline_access
    assertTrue(config.getDisableOauthRefreshToken());
    assertFalse(config.getScopes().contains("offline_access"));
  }

  @Test
  void
      getWorkspaceClient_OAuthWithBrowserBasedAuthentication_WithDiscoveryURL_AuthenticatesCorrectly()
          throws DatabricksParsingException,
              IOException,
              DatabricksSSLException,
              DatabricksValidationException {
    when(mockContext.getAuthMech()).thenReturn(AuthMech.OAUTH);
    when(mockContext.getAuthFlow()).thenReturn(AuthFlow.BROWSER_BASED_AUTHENTICATION);
    when(mockContext.getHostForOAuth()).thenReturn("https://oauth-browser.databricks.com");
    when(mockContext.getClientId()).thenReturn("browser-client-id");
    when(mockContext.getClientSecret()).thenReturn("browser-client-secret");
    when(mockContext.isOAuthDiscoveryModeEnabled()).thenReturn(true);
    when(mockContext.getOAuthDiscoveryURL()).thenReturn(TEST_DISCOVERY_URL);
    when(mockContext.getOAuthScopesForU2M())
        .thenReturn(Collections.singletonList(TEST_SCOPE_STRING));
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
    when(mockContext.getOAuth2RedirectUrlPorts()).thenReturn(List.of(8020));
    when(mockContext.getHttpMaxConnectionsPerRoute()).thenReturn(100);
    when(mockContext.getDisableOauthRefreshToken()).thenReturn(true);
    configurator = new ClientConfigurator(mockContext);
    WorkspaceClient client = configurator.getWorkspaceClient();
    assertNotNull(client);
    DatabricksConfig config = client.config();

    assertEquals(TEST_DISCOVERY_URL, config.getDiscoveryUrl());
    assertEquals("https://oauth-browser.databricks.com", config.getHost());
    assertEquals("browser-client-id", config.getClientId());
    assertEquals("browser-client-secret", config.getClientSecret());
    assertEquals(List.of(TEST_SCOPE_STRING), config.getScopes());
    assertEquals("http://localhost:8020", config.getOAuthRedirectUrl());
    assertEquals(DatabricksJdbcConstants.U2M_AUTH_TYPE, config.getAuthType());
  }

  @Test
  void testNonOauth() throws DatabricksSSLException, DatabricksValidationException {
    when(mockContext.getAuthMech()).thenReturn(AuthMech.OTHER);
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
    when(mockContext.getHttpMaxConnectionsPerRoute()).thenReturn(100);
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
  void testSetupProxyConfig() throws DatabricksSSLException, DatabricksValidationException {
    when(mockContext.getAuthMech()).thenReturn(AuthMech.PAT);
    when(mockContext.getUseProxy()).thenReturn(true);
    when(mockContext.getProxyHost()).thenReturn("proxy.host.com");
    when(mockContext.getProxyPort()).thenReturn(3128);
    when(mockContext.getProxyUser()).thenReturn("proxyUser");
    when(mockContext.getProxyPassword()).thenReturn("proxyPass");
    when(mockContext.getProxyAuthType()).thenReturn(ProxyConfig.ProxyAuthType.values()[0]);
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
    when(mockContext.getHttpMaxConnectionsPerRoute()).thenReturn(100);
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
  void setupM2MConfig_WithAzureTenantId_ConfiguresCorrectly()
      throws DatabricksParsingException, DatabricksSSLException, DatabricksValidationException {
    when(mockContext.getAuthMech()).thenReturn(AuthMech.OAUTH);
    when(mockContext.getAuthFlow()).thenReturn(AuthFlow.CLIENT_CREDENTIALS);
    when(mockContext.getHostForOAuth()).thenReturn("https://azure-oauth.databricks.com");
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
    when(mockContext.getAzureTenantId()).thenReturn("azure-tenant-id");
    when(mockContext.getCloud()).thenReturn(Cloud.AZURE);
    when(mockContext.getClientId()).thenReturn("azure-client-id");
    when(mockContext.getClientSecret()).thenReturn("azure-client-secret");
    when(mockContext.getHttpMaxConnectionsPerRoute()).thenReturn(100);

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
      throws DatabricksParsingException, DatabricksValidationException {
    when(mockContext.getAuthMech()).thenReturn(AuthMech.OAUTH);
    when(mockContext.getAuthFlow()).thenReturn(AuthFlow.CLIENT_CREDENTIALS);
    when(mockContext.getHostForOAuth()).thenReturn("https://azure-oauth.databricks.com");
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
    when(mockContext.getAzureTenantId()).thenReturn("azure-tenant-id");
    when(mockContext.getCloud()).thenReturn(Cloud.AWS);
    when(mockContext.getHttpMaxConnectionsPerRoute()).thenReturn(100);

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
    when(mockContext.getHttpMaxConnectionsPerRoute()).thenReturn(100);
    configurator = new ClientConfigurator(mockContext);

    // Test with a single available port
    int availablePort = findFreePort();
    List<Integer> ports = List.of(availablePort);
    int result = configurator.findAvailablePort(ports);
    assertEquals(availablePort, result);

    // Test with multiple ports, first unavailable
    int secondAvailablePort = findFreePort();
    try (ServerSocket serverSocket = new ServerSocket()) {
      serverSocket.setReuseAddress(true);
      serverSocket.bind(new InetSocketAddress(availablePort));
      ports = List.of(availablePort, secondAvailablePort);
      result = configurator.findAvailablePort(ports);
      assertEquals(secondAvailablePort, result);
    }

    // Test incremental search - first port unavailable, finds next available in sequence
    try (ServerSocket serverSocket2 = new ServerSocket()) {
      serverSocket2.setReuseAddress(true);
      serverSocket2.bind(new InetSocketAddress(availablePort));
      ports = List.of(availablePort);
      result = configurator.findAvailablePort(ports);
      assertTrue(
          result > availablePort,
          String.format("Expected port > %d, but got %d", availablePort, result));
      // 3. The returned port should actually be available
      try (ServerSocket testSocket = new ServerSocket()) {
        testSocket.setReuseAddress(true);
        testSocket.bind(new InetSocketAddress(result));
        assertNotNull(testSocket, "Returned port should be available for binding");
      }
    }
  }

  @Test
  void testFindAvailablePortThrowsExceptionWhenNoPortsAvailable() throws Exception {
    // Create a mockContext for the ClientConfigurator constructor
    when(mockContext.getAuthMech()).thenReturn(AuthMech.PAT);
    when(mockContext.getHostUrl()).thenReturn("https://test.databricks.com");
    when(mockContext.getToken()).thenReturn("test-token");
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
    when(mockContext.getHttpMaxConnectionsPerRoute()).thenReturn(100);
    configurator = new ClientConfigurator(mockContext);

    // Use a port that is likely to be available
    int port1 = findFreePort();
    int port2 = findFreePort();
    if (port1 == port2) {
      port2 = port1 + 1;
    }

    // Occupy the ports to make them unavailable
    try (ServerSocket socket1 = new ServerSocket();
        ServerSocket socket2 = new ServerSocket()) {
      socket1.setReuseAddress(true);
      socket1.bind(new InetSocketAddress(port1));
      socket2.setReuseAddress(true);
      socket2.bind(new InetSocketAddress(port2));

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
    try (ServerSocket socket = new ServerSocket()) {
      socket.setReuseAddress(true);
      socket.bind(new InetSocketAddress(0));
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
    when(mockContext.getOAuthScopesForU2M()).thenReturn(List.of("scope1", "scope2"));
    when(mockContext.getOAuth2RedirectUrlPorts()).thenReturn(List.of(testPort));
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
    when(mockContext.getHttpMaxConnectionsPerRoute()).thenReturn(100);
    when(mockContext.getDisableOauthRefreshToken()).thenReturn(true);

    configurator = new ClientConfigurator(mockContext);
    WorkspaceClient client = configurator.getWorkspaceClient();
    assertNotNull(client);
    DatabricksConfig config = client.config();

    assertEquals("https://oauth-browser.databricks.com", config.getHost());
    assertEquals("browser-client-id", config.getClientId());
    assertEquals("browser-client-secret", config.getClientSecret());
    assertEquals(List.of("scope1", "scope2"), config.getScopes());
    assertEquals(Duration.ofHours(1), config.getOAuthBrowserAuthTimeout());
    assertEquals("http://localhost:" + testPort, config.getOAuthRedirectUrl());
    assertEquals(DatabricksJdbcConstants.U2M_AUTH_TYPE, config.getAuthType());
  }

  @Test
  void testSetupU2MConfig_WithTokenCache()
      throws DatabricksParsingException, DatabricksSSLException, DatabricksValidationException {
    when(mockContext.getAuthMech()).thenReturn(AuthMech.OAUTH);
    when(mockContext.getAuthFlow()).thenReturn(AuthFlow.BROWSER_BASED_AUTHENTICATION);
    when(mockContext.getHostForOAuth()).thenReturn("https://oauth-browser.databricks.com");
    when(mockContext.getClientId()).thenReturn("browser-client-id");
    when(mockContext.getClientSecret()).thenReturn("browser-client-secret");
    when(mockContext.getOAuthScopesForU2M()).thenReturn(List.of("scope1", "scope2"));
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
    when(mockContext.getOAuth2RedirectUrlPorts()).thenReturn(List.of(8020));
    when(mockContext.isTokenCacheEnabled()).thenReturn(true);
    when(mockContext.getTokenCachePassPhrase()).thenReturn("testPassphrase");
    when(mockContext.getHttpMaxConnectionsPerRoute()).thenReturn(100);
    when(mockContext.getDisableOauthRefreshToken()).thenReturn(true);
    when(mockContext.isTokenFederationEnabled()).thenReturn(true);

    configurator = new ClientConfigurator(mockContext);
    WorkspaceClient client = configurator.getWorkspaceClient();
    assertNotNull(client);
    DatabricksConfig config = client.config();

    assertEquals("https://oauth-browser.databricks.com", config.getHost());
    assertEquals("browser-client-id", config.getClientId());
    assertEquals("browser-client-secret", config.getClientSecret());
    assertEquals(List.of("scope1", "scope2"), config.getScopes());
    assertEquals("http://localhost:8020", config.getOAuthRedirectUrl());
    assertEquals(DatabricksJdbcConstants.U2M_AUTH_TYPE, config.getAuthType());
    DatabricksTokenFederationProvider databricksTokenFederationProvider =
        (DatabricksTokenFederationProvider) config.getCredentialsProvider();
    assertInstanceOf(
        ExternalBrowserCredentialsProvider.class,
        databricksTokenFederationProvider.getCredentialsProvider());
  }

  @Test
  void testSetupU2MConfig_WithTokenCacheNoPassphrase()
      throws DatabricksParsingException, DatabricksValidationException {
    when(mockContext.getAuthMech()).thenReturn(AuthMech.OAUTH);
    when(mockContext.getAuthFlow()).thenReturn(AuthFlow.BROWSER_BASED_AUTHENTICATION);
    when(mockContext.getHostForOAuth()).thenReturn("https://oauth-browser.databricks.com");
    when(mockContext.getClientId()).thenReturn("browser-client-id");
    when(mockContext.getClientSecret()).thenReturn("browser-client-secret");
    when(mockContext.getOAuthScopesForU2M()).thenReturn(List.of("scope1", "scope2"));
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
    when(mockContext.getOAuth2RedirectUrlPorts()).thenReturn(List.of(8020));
    when(mockContext.isTokenCacheEnabled()).thenReturn(true);
    when(mockContext.getTokenCachePassPhrase()).thenReturn(null);
    when(mockContext.getHttpMaxConnectionsPerRoute()).thenReturn(100);
    when(mockContext.getDisableOauthRefreshToken()).thenReturn(true);

    assertThrows(DatabricksException.class, () -> new ClientConfigurator(mockContext));
  }

  @Test
  void testSetupU2MConfig_WithoutTokenCache()
      throws DatabricksParsingException, DatabricksSSLException, DatabricksValidationException {
    when(mockContext.getAuthMech()).thenReturn(AuthMech.OAUTH);
    when(mockContext.getAuthFlow()).thenReturn(AuthFlow.BROWSER_BASED_AUTHENTICATION);
    when(mockContext.getHostForOAuth()).thenReturn("https://oauth-browser.databricks.com");
    when(mockContext.getClientId()).thenReturn("browser-client-id");
    when(mockContext.getClientSecret()).thenReturn("browser-client-secret");
    when(mockContext.getOAuthScopesForU2M()).thenReturn(List.of("scope1", "scope2"));
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
    when(mockContext.getOAuth2RedirectUrlPorts()).thenReturn(List.of(8020));
    when(mockContext.isTokenCacheEnabled()).thenReturn(false);
    when(mockContext.getHttpMaxConnectionsPerRoute()).thenReturn(100);
    when(mockContext.getDisableOauthRefreshToken()).thenReturn(true);
    when(mockContext.isTokenFederationEnabled()).thenReturn(true);

    configurator = new ClientConfigurator(mockContext);
    WorkspaceClient client = configurator.getWorkspaceClient();
    assertNotNull(client);
    DatabricksConfig config = client.config();

    assertEquals("https://oauth-browser.databricks.com", config.getHost());
    assertEquals("browser-client-id", config.getClientId());
    assertEquals("browser-client-secret", config.getClientSecret());
    assertEquals(List.of("scope1", "scope2"), config.getScopes());
    assertEquals("http://localhost:8020", config.getOAuthRedirectUrl());
    assertEquals(DatabricksJdbcConstants.U2M_AUTH_TYPE, config.getAuthType());
    DatabricksTokenFederationProvider databricksTokenFederationProvider =
        (DatabricksTokenFederationProvider) config.getCredentialsProvider();
    assertInstanceOf(
        ExternalBrowserCredentialsProvider.class,
        databricksTokenFederationProvider.getCredentialsProvider());
  }

  @Test
  void testTokenFederationEnabled_WrapsCredentialsProvider()
      throws DatabricksParsingException, DatabricksSSLException, DatabricksValidationException {
    // Setup OAuth M2M with token federation enabled
    when(mockContext.getAuthMech()).thenReturn(AuthMech.OAUTH);
    when(mockContext.getAuthFlow()).thenReturn(AuthFlow.CLIENT_CREDENTIALS);
    when(mockContext.getHostForOAuth()).thenReturn("https://oauth-m2m.databricks.com");
    when(mockContext.getClientId()).thenReturn("m2m-client-id");
    when(mockContext.getClientSecret()).thenReturn("m2m-client-secret");
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
    when(mockContext.getHttpMaxConnectionsPerRoute()).thenReturn(100);
    when(mockContext.getDisableOauthRefreshToken()).thenReturn(true);
    when(mockContext.isTokenFederationEnabled()).thenReturn(true); // Token federation enabled
    when(mockContext.useJWTAssertion()).thenReturn(false);
    when(mockContext.getAzureTenantId()).thenReturn(null);
    when(mockContext.getCloud()).thenReturn(Cloud.AWS);

    configurator = new ClientConfigurator(mockContext);
    WorkspaceClient client = configurator.getWorkspaceClient();
    assertNotNull(client);
    DatabricksConfig config = client.config();

    // Verify that the credentials provider is wrapped with DatabricksTokenFederationProvider
    assertInstanceOf(DatabricksTokenFederationProvider.class, config.getCredentialsProvider());
    assertEquals(DatabricksJdbcConstants.M2M_AUTH_TYPE, config.getAuthType());
  }

  @Test
  void testTokenFederationDisabled_DoesNotWrapCredentialsProvider()
      throws DatabricksParsingException, DatabricksSSLException, DatabricksValidationException {
    // Setup OAuth M2M with token federation disabled
    when(mockContext.getAuthMech()).thenReturn(AuthMech.OAUTH);
    when(mockContext.getAuthFlow()).thenReturn(AuthFlow.CLIENT_CREDENTIALS);
    when(mockContext.getHostForOAuth()).thenReturn("https://oauth-m2m.databricks.com");
    when(mockContext.getClientId()).thenReturn("m2m-client-id");
    when(mockContext.getClientSecret()).thenReturn("m2m-client-secret");
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
    when(mockContext.getHttpMaxConnectionsPerRoute()).thenReturn(100);
    when(mockContext.getDisableOauthRefreshToken()).thenReturn(true);
    when(mockContext.isTokenFederationEnabled()).thenReturn(false); // Token federation disabled
    when(mockContext.useJWTAssertion()).thenReturn(false);
    when(mockContext.getAzureTenantId()).thenReturn(null);
    when(mockContext.getCloud()).thenReturn(Cloud.AWS);

    configurator = new ClientConfigurator(mockContext);
    WorkspaceClient client = configurator.getWorkspaceClient();
    assertNotNull(client);
    DatabricksConfig config = client.config();

    // Verify that the credentials provider is NOT wrapped with DatabricksTokenFederationProvider
    assertNotNull(config.getCredentialsProvider());
    // Should be the original OAuthM2MServicePrincipalCredentialsProvider, not wrapped
    assertFalse(config.getCredentialsProvider() instanceof DatabricksTokenFederationProvider);
  }

  @Test
  void testTokenFederationWithPATAuth_DoesNotAffectPATAuth()
      throws DatabricksParsingException, DatabricksSSLException, DatabricksValidationException {
    // Setup PAT auth with token federation disabled - should not affect PAT auth
    when(mockContext.getAuthMech()).thenReturn(AuthMech.PAT);
    when(mockContext.getHostUrl()).thenReturn("https://pat.databricks.com");
    when(mockContext.getToken()).thenReturn("pat-token");
    when(mockContext.getHttpConnectionPoolSize()).thenReturn(100);
    when(mockContext.getHttpMaxConnectionsPerRoute()).thenReturn(100);

    configurator = new ClientConfigurator(mockContext);
    WorkspaceClient client = configurator.getWorkspaceClient();
    assertNotNull(client);
    DatabricksConfig config = client.config();

    // PAT auth should work normally regardless of token federation setting
    assertEquals("https://pat.databricks.com", config.getHost());
    assertEquals("pat-token", config.getToken());
    assertEquals(DatabricksJdbcConstants.ACCESS_TOKEN_AUTH_TYPE, config.getAuthType());
    // PAT auth doesn't use Token federation provider, so it should be SDK default provider
    assertFalse(config.getCredentialsProvider() instanceof DatabricksTokenFederationProvider);
  }
}
