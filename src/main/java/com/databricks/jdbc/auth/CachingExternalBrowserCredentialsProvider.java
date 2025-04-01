package com.databricks.jdbc.auth;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.sdk.core.CredentialsProvider;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.HeaderFactory;
import com.databricks.sdk.core.oauth.Consent;
import com.databricks.sdk.core.oauth.OAuthClient;
import com.databricks.sdk.core.oauth.SessionCredentials;
import com.databricks.sdk.core.oauth.Token;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpHeaders;

/**
 * A {@code CredentialsProvider} which implements the Authorization Code + PKCE flow with token
 * caching and automatic token refresh. This provider encrypts and caches tokens in the user's home
 * directory and reuses them when possible.
 */
public class CachingExternalBrowserCredentialsProvider implements CredentialsProvider {

  private static final String AUTH_TYPE = "external-browser-with-cache";
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(CachingExternalBrowserCredentialsProvider.class);
  private final TokenCache tokenCache;
  private final DatabricksConfig config;
  private Token currentToken;

  public CachingExternalBrowserCredentialsProvider(DatabricksConfig config, String passphrase) {
    this(config, new TokenCache(config.getUsername(), passphrase));
  }

  @VisibleForTesting
  public CachingExternalBrowserCredentialsProvider(DatabricksConfig config, TokenCache tokenCache) {
    this.config = config;
    this.tokenCache = tokenCache;
  }

  @Override
  public String authType() {
    return AUTH_TYPE;
  }

  @Override
  public HeaderFactory configure(DatabricksConfig config) {
    if (config.getHost() == null || !config.getAuthType().equals(AUTH_TYPE)) {
      return null;
    }

    try {
      ensureValidToken();
      return createHeaderFactory();
    } catch (Exception e) {
      LOGGER.error("Failed to configure external browser credentials", e);
      throw new DatabricksException("Failed to configure external browser credentials", e);
    }
  }

  private void ensureValidToken() throws IOException, DatabricksException {
    // Try to load from cache first
    currentToken = tokenCache.load();

    if (currentToken != null) {
      LOGGER.debug("Cached token found");
      if (!currentToken.isExpired()) {
        // Use cached access token if it's still valid
        LOGGER.debug("Use cached token since it is still valid");
        return;
      }

      // Try to refresh the token if we have a refresh token
      if (currentToken.getRefreshToken() != null) {
        try {
          LOGGER.debug("Using cached refresh token to get new access token");
          currentToken = refreshAccessToken();
          tokenCache.save(currentToken);
          return;
        } catch (Exception e) {
          LOGGER.info("Failed to refresh access token, will restart browser auth", e);
          // If refresh fails, fall through to browser auth
        }
      }
    }

    // If we get here, we need to do browser auth
    LOGGER.debug(
        "Cached token not found or unable to refresh access token, will restart browser auth");
    currentToken = performBrowserAuth();
    tokenCache.save(currentToken);
  }

  @VisibleForTesting
  Token refreshAccessToken() throws IOException, DatabricksException {
    OAuthClient client = new OAuthClient(config);
    Consent consent = client.initiateConsent();
    SessionCredentials creds =
        consent.exchangeCallbackParameters(Map.of("refresh_token", currentToken.getRefreshToken()));
    return creds.getToken();
  }

  @VisibleForTesting
  Token performBrowserAuth() throws IOException, DatabricksException {
    OAuthClient client = new OAuthClient(config);
    Consent consent = client.initiateConsent();
    SessionCredentials creds = consent.launchExternalBrowser();
    return creds.getToken();
  }

  private HeaderFactory createHeaderFactory() {
    return () -> {
      Map<String, String> headers = new HashMap<>();
      headers.put(
          HttpHeaders.AUTHORIZATION,
          currentToken.getTokenType() + " " + currentToken.getAccessToken());
      return headers;
    };
  }
}
