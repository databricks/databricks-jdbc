package com.databricks.jdbc.auth;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.HeaderFactory;
import com.databricks.sdk.core.oauth.Token;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Arrays;
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

  @Mock private TokenCache tokenCache;

  private CachingExternalBrowserCredentialsProvider provider;
  private DatabricksConfig config;

  @BeforeEach
  void setUp() {
    config =
        new DatabricksConfig()
            .setAuthType("external-browser-with-cache")
            .setHost(TEST_HOST)
            .setClientId("test-client-id")
            .setUsername("test-user")
            .setScopes(Arrays.asList("offline_access", "clusters", "sql"));

    provider = spy(new CachingExternalBrowserCredentialsProvider(config, tokenCache));
  }

  @Test
  void testAuthType() {
    assertEquals("external-browser-with-cache", provider.authType());
  }

  @Test
  void testConfigureWithInvalidConfig() {
    DatabricksConfig invalidConfig = new DatabricksConfig().setAuthType("invalid-type");
    assertNull(provider.configure(invalidConfig));
  }

  @Test
  void testUseValidTokenFromCache() throws IOException {
    // Setup valid token in cache
    Token validToken =
        new Token(
            "cached-token", "Bearer", "cached-refresh-token", LocalDateTime.now().plusHours(1));
    when(tokenCache.load()).thenReturn(validToken);

    // Should use cached token without any refresh or browser auth
    HeaderFactory headerFactory = provider.configure(config);
    assertNotNull(headerFactory);

    Map<String, String> headers = headerFactory.headers();
    assertEquals("Bearer cached-token", headers.get(HttpHeaders.AUTHORIZATION));

    // Verify no refresh or browser auth was attempted
    verify(provider, never()).refreshAccessToken();
    verify(provider, never()).performBrowserAuth();
  }

  @Test
  void testRefreshExpiredTokenSuccess() throws IOException {
    // Setup expired token in cache
    Token expiredToken =
        new Token(
            "expired-token", "Bearer", "cached-refresh-token", LocalDateTime.now().minusMinutes(5));
    when(tokenCache.load()).thenReturn(expiredToken);

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

    // Verify refresh was attempted but not browser auth
    verify(provider).refreshAccessToken();
    verify(provider, never()).performBrowserAuth();
    verify(tokenCache).save(refreshedToken);
  }

  @Test
  void testRefreshTokenFailureFallbackToBrowserAuth() throws IOException {
    // Setup expired token in cache
    Token expiredToken =
        new Token(
            "expired-token",
            "Bearer",
            "invalid-refresh-token",
            LocalDateTime.now().minusMinutes(5));
    when(tokenCache.load()).thenReturn(expiredToken);

    // Setup failed token refresh
    doThrow(new IOException("Invalid refresh token")).when(provider).refreshAccessToken();

    // Setup successful browser auth
    Token newToken =
        new Token("new-token", "Bearer", "new-refresh-token", LocalDateTime.now().plusHours(1));
    doReturn(newToken).when(provider).performBrowserAuth();

    // Should fall back to browser auth after refresh fails
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
  void testEmptyCacheFallbackToBrowserAuth() throws IOException {
    // Setup empty cache
    when(tokenCache.load()).thenReturn(null);

    // Setup successful browser auth
    Token newToken =
        new Token("new-token", "Bearer", "new-refresh-token", LocalDateTime.now().plusHours(1));
    doReturn(newToken).when(provider).performBrowserAuth();

    // Should directly use browser auth
    HeaderFactory headerFactory = provider.configure(config);
    assertNotNull(headerFactory);

    Map<String, String> headers = headerFactory.headers();
    assertEquals("Bearer new-token", headers.get(HttpHeaders.AUTHORIZATION));

    // Verify only browser auth was attempted
    verify(provider, never()).refreshAccessToken();
    verify(provider).performBrowserAuth();
    verify(tokenCache).save(newToken);
  }
}
