package com.databricks.jdbc.auth;

import static com.databricks.jdbc.TestConstants.TEST_AUTH_URL;
import static com.databricks.jdbc.TestConstants.TEST_TOKEN_URL;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.HeaderFactory;
import com.databricks.sdk.core.oauth.OpenIDConnectEndpoints;
import com.databricks.sdk.core.oauth.Token;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Map;
import org.apache.http.HttpHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class CachingExternalBrowserCredentialsProviderTest {
  private static final String TEST_HOST = "test-host";
  private static final String AUTH_TYPE = "external-browser-with-cache";

  @Mock private TokenCache tokenCache;
  @Mock private IDatabricksConnectionContext connectionContext;
  @Mock private DatabricksConfig config;

  private CachingExternalBrowserCredentialsProvider provider;

  @BeforeEach
  void setUp() throws IOException {
    // Set up necessary mocks
    doReturn(new OpenIDConnectEndpoints(TEST_TOKEN_URL, TEST_AUTH_URL))
        .when(config)
        .getOidcEndpoints();
  }

  @Test
  void testAuthType() {
    provider =
        spy(new CachingExternalBrowserCredentialsProvider(config, connectionContext, tokenCache));
    assertEquals(AUTH_TYPE, provider.authType());
  }

  @Test
  void testConfigureWithInvalidConfig() {
    DatabricksConfig invalidConfig = new DatabricksConfig().setAuthType("invalid-type");
    provider =
        spy(new CachingExternalBrowserCredentialsProvider(config, connectionContext, tokenCache));
    assertNull(provider.configure(invalidConfig));
  }

  @Test
  void testUseValidTokenFromCache() throws IOException {
    when(config.getHost()).thenReturn(TEST_HOST);
    when(config.getAuthType()).thenReturn(AUTH_TYPE);
    // Setup valid token in cache
    Token validToken =
        new Token(
            "cached-token", "Bearer", "cached-refresh-token", LocalDateTime.now().plusHours(1));
    when(tokenCache.load()).thenReturn(validToken);

    provider =
        spy(new CachingExternalBrowserCredentialsProvider(config, connectionContext, tokenCache));

    // Should use cached token without any refresh or browser auth
    HeaderFactory headerFactory = provider.configure(config);
    assertNotNull(headerFactory);

    Map<String, String> headers = headerFactory.headers();
    assertEquals("Bearer cached-token", headers.get(HttpHeaders.AUTHORIZATION));

    // Verify no refresh or browser auth was attempted
    verify(provider, never()).refresh();
  }

  @Test
  void testRefreshExpiredTokenSuccess() throws IOException, DatabricksException {
    when(config.getHost()).thenReturn(TEST_HOST);
    when(config.getAuthType()).thenReturn(AUTH_TYPE);

    // Setup expired token in cache
    Token expiredToken =
        new Token(
            "expired-token", "Bearer", "cached-refresh-token", LocalDateTime.now().minusMinutes(5));
    when(tokenCache.load()).thenReturn(expiredToken);

    provider =
        spy(new CachingExternalBrowserCredentialsProvider(config, connectionContext, tokenCache));

    // Setup successful token refresh
    Token refreshedToken =
        new Token(
            "refreshed-token", "Bearer", "new-refresh-token", LocalDateTime.now().plusHours(1));
    doReturn(refreshedToken).when(provider).refreshAccessToken();

    // Should refresh token using cached refresh token
    HeaderFactory headerFactory = provider.configure(config);
    assertNotNull(headerFactory);

    Map<String, String> headers = headerFactory.headers();
    assertEquals("Bearer refreshed-token", headers.get(HttpHeaders.AUTHORIZATION));

    // Verify refresh was attempted
    verify(provider).refreshAccessToken();
    verify(tokenCache).save(refreshedToken);
  }

  @Test
  void testRefreshTokenFailureFallbackToBrowserAuth() throws IOException, DatabricksException {
    when(config.getHost()).thenReturn(TEST_HOST);
    when(config.getAuthType()).thenReturn(AUTH_TYPE);

    // Setup expired token in cache
    Token expiredToken =
        new Token(
            "expired-token",
            "Bearer",
            "invalid-refresh-token",
            LocalDateTime.now().minusMinutes(5));
    when(tokenCache.load()).thenReturn(expiredToken);

    provider =
        spy(new CachingExternalBrowserCredentialsProvider(config, connectionContext, tokenCache));

    // Setup failed token refresh
    doThrow(new DatabricksException("Invalid refresh token")).when(provider).refreshAccessToken();

    // Setup successful browser auth
    Token newToken =
        new Token("new-token", "Bearer", "new-refresh-token", LocalDateTime.now().plusHours(1));
    doReturn(newToken).when(provider).performBrowserAuth();

    // Configure and get the token to trigger refresh
    HeaderFactory headerFactory = provider.configure(config);
    assertNotNull(headerFactory);

    Map<String, String> headers = headerFactory.headers();
    assertEquals("Bearer new-token", headers.get(HttpHeaders.AUTHORIZATION));

    // Verify both refresh and browser auth were attempted
    verify(provider).refreshAccessToken();
    verify(provider).performBrowserAuth();
    verify(tokenCache).save(newToken);
  }

  @Test
  void testEmptyCacheFallbackToBrowserAuth() throws IOException, DatabricksException {
    when(config.getHost()).thenReturn(TEST_HOST);
    when(config.getAuthType()).thenReturn(AUTH_TYPE);

    // Setup empty cache with null token
    // RefreshableTokenSource initialization will create empty token if null is returned
    when(tokenCache.load()).thenReturn(null);
    provider =
        spy(new CachingExternalBrowserCredentialsProvider(config, connectionContext, tokenCache));

    // Setup successful browser auth
    Token newToken =
        new Token("new-token", "Bearer", "new-refresh-token", LocalDateTime.now().plusHours(1));
    doReturn(newToken).when(provider).performBrowserAuth();

    // Configure and get the token to trigger refresh
    HeaderFactory headerFactory = provider.configure(config);
    assertNotNull(headerFactory);

    Map<String, String> headers = headerFactory.headers();
    assertEquals("Bearer new-token", headers.get(HttpHeaders.AUTHORIZATION));

    // Verify only browser auth was attempted (refresh will not be attempted with null refresh
    // token)
    verify(provider).performBrowserAuth();
    verify(tokenCache).save(newToken);
  }

  @Test
  void testRefreshAccessToken() throws DatabricksException, IOException {
    // Create a token with a refresh token
    Token token =
        new Token(
            DatabricksJdbcConstants.EMPTY_STRING,
            DatabricksJdbcConstants.EMPTY_STRING,
            "test-refresh-token",
            LocalDateTime.now().minusMinutes(1));
    when(tokenCache.load()).thenReturn(token);

    provider =
        spy(new CachingExternalBrowserCredentialsProvider(config, connectionContext, tokenCache));

    // Test that refreshAccessToken is called by refresh()
    doThrow(new DatabricksException("Test exception")).when(provider).refreshAccessToken();

    assertThrows(DatabricksException.class, () -> provider.refresh());
  }
}
