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
 * caching and automatic token refresh. This provider encrypts and caches tokens in the user's
 * temporary directory and reuses them when possible.
 *
 * <p>This provider extends the standard OAuth browser-based authentication flow by adding
 * persistence of tokens between sessions. When a token is obtained after successful authentication,
 * it is encrypted and stored locally. On subsequent connection attempts, the provider will:
 *
 * <ol>
 *   <li>Try to load and use a cached token if available
 *   <li>If the cached token is expired but has a refresh token, attempt to refresh it
 *   <li>If no cached token exists or refresh fails, initiate the browser-based OAuth flow
 * </ol>
 *
 * <p>This approach minimizes the need for users to repeatedly authenticate through the browser,
 * improving the user experience while maintaining security through encryption of the cached tokens.
 */
public class CachingExternalBrowserCredentialsProvider implements CredentialsProvider {

  private static final String AUTH_TYPE = "external-browser-with-cache";
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(CachingExternalBrowserCredentialsProvider.class);
  private final TokenCache tokenCache;
  private final DatabricksConfig config;
  private Token currentToken;

  /**
   * Creates a new CachingExternalBrowserCredentialsProvider with the specified configuration and
   * passphrase for token encryption.
   *
   * @param config The Databricks configuration to use for authentication
   * @param passphrase The passphrase to use for encrypting and decrypting cached tokens
   * @throws IllegalArgumentException if the passphrase is null or empty
   */
  public CachingExternalBrowserCredentialsProvider(DatabricksConfig config, String passphrase) {
    this(config, new TokenCache(passphrase));
  }

  /**
   * Creates a new CachingExternalBrowserCredentialsProvider with the specified configuration and
   * token cache. This constructor is primarily used for testing.
   *
   * @param config The Databricks configuration to use for authentication
   * @param tokenCache The token cache to use for storing and retrieving tokens
   */
  @VisibleForTesting
  public CachingExternalBrowserCredentialsProvider(DatabricksConfig config, TokenCache tokenCache) {
    this.config = config;
    this.tokenCache = tokenCache;
  }

  /**
   * Returns the authentication type identifier for this provider.
   *
   * @return The string "external-browser-with-cache"
   */
  @Override
  public String authType() {
    return AUTH_TYPE;
  }

  /**
   * Configures the authentication by setting up the necessary headers for authenticated requests.
   * This method implements the core OAuth flow with caching logic.
   *
   * @param config The Databricks configuration to use
   * @return A HeaderFactory that adds the OAuth authentication header to requests, or null if the
   *     configuration is not valid for this provider
   */
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

  /**
   * Ensures that a valid token is available for authentication. This method implements the token
   * loading, validation, refreshing, and browser authentication flow.
   *
   * <p>The method follows this sequence:
   *
   * <ol>
   *   <li>Try to load a token from the cache
   *   <li>If a token is found and not expired, use it
   *   <li>If a token is found and expired but has a refresh token, try to refresh it
   *   <li>If no token is found or refresh fails, perform browser-based authentication
   * </ol>
   *
   * @throws IOException If there is an error reading from or writing to the token cache
   * @throws DatabricksException If there is an error during the authentication process
   */
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

  /**
   * Refreshes an access token using the refresh token from the current token.
   *
   * @return A new token with a refreshed access token
   * @throws IOException If there is an error during the refresh process
   * @throws DatabricksException If the Databricks API returns an error
   */
  @VisibleForTesting
  Token refreshAccessToken() throws IOException, DatabricksException {
    OAuthClient client = new OAuthClient(config);
    Consent consent = client.initiateConsent();
    SessionCredentials creds =
        consent.exchangeCallbackParameters(Map.of("refresh_token", currentToken.getRefreshToken()));
    return creds.getToken();
  }

  /**
   * Performs browser-based authentication to obtain a new token.
   *
   * @return A new token obtained through browser authentication
   * @throws IOException If there is an error during the authentication process
   * @throws DatabricksException If the Databricks API returns an error
   */
  @VisibleForTesting
  Token performBrowserAuth() throws IOException, DatabricksException {
    OAuthClient client = new OAuthClient(config);
    Consent consent = client.initiateConsent();
    SessionCredentials creds = consent.launchExternalBrowser();
    return creds.getToken();
  }

  /**
   * Creates a HeaderFactory that adds the OAuth Bearer token to requests.
   *
   * @return A HeaderFactory that adds the Authorization header with the OAuth Bearer token
   */
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
