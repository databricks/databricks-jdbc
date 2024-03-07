package com.databricks.jdbc.core;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.driver.DatabricksJdbcConstants;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class OAuthAuthenticatorTest {

  @Mock private IDatabricksConnectionContext mockContext;

  private OAuthAuthenticator authenticator;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    authenticator = new OAuthAuthenticator(mockContext);
  }

  @Test
  void getWorkspaceClient_PAT_AuthenticatesWithAccessToken() {
    when(mockContext.getAuthMech()).thenReturn(IDatabricksConnectionContext.AuthMech.PAT);
    when(mockContext.getHostUrl()).thenReturn("https://pat.databricks.com");
    when(mockContext.getToken()).thenReturn("pat-token");

    WorkspaceClient client = authenticator.getWorkspaceClient();
    assertNotNull(client);
    DatabricksConfig config = client.config();

    assertEquals("https://pat.databricks.com", config.getHost());
    assertEquals("pat-token", config.getToken());
    assertEquals(DatabricksJdbcConstants.ACCESS_TOKEN_AUTH_TYPE, config.getAuthType());
  }

  @Test
  void getWorkspaceClient_OAuthWithTokenPassthrough_AuthenticatesCorrectly() {
    when(mockContext.getAuthMech()).thenReturn(IDatabricksConnectionContext.AuthMech.OAUTH);
    when(mockContext.getAuthFlow())
        .thenReturn(IDatabricksConnectionContext.AuthFlow.TOKEN_PASSTHROUGH);
    when(mockContext.getHostUrl()).thenReturn("https://oauth-token.databricks.com");
    when(mockContext.getToken()).thenReturn("oauth-token");

    WorkspaceClient client = authenticator.getWorkspaceClient();
    assertNotNull(client);
    DatabricksConfig config = client.config();

    assertEquals("https://oauth-token.databricks.com", config.getHost());
    assertEquals("oauth-token", config.getToken());
    assertEquals(DatabricksJdbcConstants.ACCESS_TOKEN_AUTH_TYPE, config.getAuthType());
  }

  @Test
  void getWorkspaceClient_OAuthWithClientCredentials_AuthenticatesCorrectly() {
    when(mockContext.getAuthMech()).thenReturn(IDatabricksConnectionContext.AuthMech.OAUTH);
    when(mockContext.getAuthFlow())
        .thenReturn(IDatabricksConnectionContext.AuthFlow.CLIENT_CREDENTIALS);
    when(mockContext.getHostForOAuth()).thenReturn("https://oauth-client.databricks.com");
    when(mockContext.getClientId()).thenReturn("client-id");
    when(mockContext.getClientSecret()).thenReturn("client-secret");

    WorkspaceClient client = authenticator.getWorkspaceClient();
    assertNotNull(client);
    DatabricksConfig config = client.config();

    assertEquals("https://oauth-client.databricks.com", config.getHost());
    assertEquals("client-id", config.getClientId());
    assertEquals("client-secret", config.getClientSecret());
    assertEquals(DatabricksJdbcConstants.M2M_AUTH_TYPE, config.getAuthType());
  }

  @Test
  void getWorkspaceClient_OAuthWithBrowserBasedAuthentication_AuthenticatesCorrectly() {
    when(mockContext.getAuthMech()).thenReturn(IDatabricksConnectionContext.AuthMech.OAUTH);
    when(mockContext.getAuthFlow())
        .thenReturn(IDatabricksConnectionContext.AuthFlow.BROWSER_BASED_AUTHENTICATION);
    when(mockContext.getHostForOAuth()).thenReturn("https://oauth-browser.databricks.com");
    when(mockContext.getClientId()).thenReturn("browser-client-id");
    when(mockContext.getClientSecret()).thenReturn("browser-client-secret");
    when(mockContext.getOAuthScopesForU2M()).thenReturn(List.of(new String[] {"scope1", "scope2"}));

    WorkspaceClient client = authenticator.getWorkspaceClient();
    assertNotNull(client);
    DatabricksConfig config = client.config();

    assertEquals("https://oauth-browser.databricks.com", config.getHost());
    assertEquals("browser-client-id", config.getClientId());
    assertEquals("browser-client-secret", config.getClientSecret());
    assertEquals(List.of(new String[] {"scope1", "scope2"}), config.getScopes());
    assertEquals(DatabricksJdbcConstants.U2M_AUTH_REDIRECT_URL, config.getOAuthRedirectUrl());
    assertEquals(DatabricksJdbcConstants.U2M_AUTH_TYPE, config.getAuthType());
  }
}
