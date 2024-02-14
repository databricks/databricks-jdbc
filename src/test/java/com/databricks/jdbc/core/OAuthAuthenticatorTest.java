package com.databricks.jdbc.core;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.jdbc.driver.IDatabricksConnectionContext.AuthFlow;
import com.databricks.jdbc.driver.IDatabricksConnectionContext.AuthMech;
import com.databricks.sdk.WorkspaceClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class OAuthAuthenticatorTest {

  @Mock private IDatabricksConnectionContext connectionContext;

  private OAuthAuthenticator authenticator;

  @BeforeEach
  void setUp() {
    authenticator = new OAuthAuthenticator(connectionContext);
  }

  @Test
  void testGetWorkspaceClientWithPatAuthentication() {
    when(connectionContext.getAuthMech()).thenReturn(AuthMech.PAT);
    WorkspaceClient client = authenticator.getWorkspaceClient();
    assertNotNull(client, "WorkspaceClient should not be null when using PAT authentication");
  }

  @Test
  void testGetWorkspaceClientWithOAuthTokenPassthrough() {
    when(connectionContext.getAuthMech()).thenReturn(AuthMech.OAUTH);
    when(connectionContext.getAuthFlow()).thenReturn(AuthFlow.TOKEN_PASSTHROUGH);
    WorkspaceClient client = authenticator.getWorkspaceClient();
    assertNotNull(client, "WorkspaceClient should not be null for OAuth Token Passthrough");
  }

  @Test
  void testGetWorkspaceClientWithClientCredentials() {
    when(connectionContext.getAuthMech()).thenReturn(AuthMech.OAUTH);
    when(connectionContext.getAuthFlow()).thenReturn(AuthFlow.CLIENT_CREDENTIALS);
    WorkspaceClient client = authenticator.getWorkspaceClient();
    assertNotNull(client, "WorkspaceClient should not be null for Client Credentials flow");
  }

  @Test
  void testGetWorkspaceClientWithBrowserBasedAuthentication() {
    when(connectionContext.getAuthMech()).thenReturn(AuthMech.OAUTH);
    when(connectionContext.getAuthFlow()).thenReturn(AuthFlow.BROWSER_BASED_AUTHENTICATION);
    WorkspaceClient client = authenticator.getWorkspaceClient();
    assertNotNull(client, "WorkspaceClient should not be null for Browser-Based Authentication");
  }

  @Test
  void testGetWorkspaceClientWithSslEnabled() throws Exception {
    when(connectionContext.getAuthMech()).thenReturn(AuthMech.PAT);
    when(connectionContext.isSSLEnabled()).thenReturn(true);
    when(connectionContext.getSSLKeyStorePath()).thenReturn("/path/to/keystore");
    when(connectionContext.getSSLKeyStorePassword()).thenReturn("keystorePassword");

    WorkspaceClient client = authenticator.getWorkspaceClient();
    assertNotNull(client, "WorkspaceClient should not be null when SSL is enabled");
  }

  @Test
  void testGetWorkspaceClientWithSslDisabled() {
    when(connectionContext.getAuthMech()).thenReturn(AuthMech.PAT);
    when(connectionContext.isSSLEnabled()).thenReturn(false);

    WorkspaceClient client = authenticator.getWorkspaceClient();
    assertNotNull(client, "WorkspaceClient should not be null when SSL is disabled");
  }
}
