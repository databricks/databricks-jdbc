package com.databricks.jdbc.auth;

import static com.databricks.jdbc.auth.AuthConstants.GRANT_TYPE_KEY;
import static com.databricks.jdbc.auth.AuthConstants.GRANT_TYPE_REFRESH_TOKEN_KEY;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.common.util.DatabricksAuthUtil;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.sdk.core.CredentialsProvider;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.HeaderFactory;
import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.core.oauth.*;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpHeaders;

/**
 * A {@code CredentialsProvider} which implements the Authorization Code + PKCE flow with token
 * caching and automatic token refresh. This provider encrypts and caches tokens in the user's
 * temporary directory and reuses them when possible.
 *
 * <p>This provider extends {@code RefreshableTokenSource} to handle token refreshing in a
 * standardized way. When a token is obtained after successful authentication, it is encrypted and
 * stored locally. On subsequent connection attempts, the provider will:
 *
 * <ol>
 *   <li>Try to load and use a cached token if available
 *   <li>If the cached token is expired but has a refresh token, attempt to refresh it using the
 *       OAuth2 token endpoint
 *   <li>If no cached token exists or refresh fails, initiate the browser-based OAuth flow
 * </ol>
 *
 * <p>This approach minimizes the need for users to repeatedly authenticate through the browser,
 * improving the user experience while maintaining security through encryption of the cached tokens.
 */
public class CachingExternalBrowserCredentialsProvider extends RefreshableTokenSource
    implements CredentialsProvider {

  private static final String AUTH_TYPE = "external-browser-with-cache";
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(CachingExternalBrowserCredentialsProvider.class);
  private final TokenCache tokenCache;
  private final DatabricksConfig config;
  private final String tokenEndpoint;
  private HttpClient hc;

  /**
   * Creates a new CachingExternalBrowserCredentialsProvider with the specified configuration,
   * connection context, and token cache.
   *
   * @param config The Databricks configuration to use for authentication
   * @param context The connection context containing OAuth configuration parameters
   * @param tokenCache The token cache to use for storing and retrieving tokens
   */
  public CachingExternalBrowserCredentialsProvider(
      DatabricksConfig config, IDatabricksConnectionContext context, TokenCache tokenCache) {
    this.config = config;
    this.tokenCache = tokenCache;
    this.tokenEndpoint = DatabricksAuthUtil.getTokenEndpoint(config, context);
    try {
      // Initialize token from cache
      this.token = tokenCache.load();
      if (this.token == null) {
        LOGGER.debug("No cached token found");
        // Initialize with an expired token to force authentication on first use
        this.token =
            new Token(
                DatabricksJdbcConstants.EMPTY_STRING,
                DatabricksJdbcConstants.EMPTY_STRING,
                null,
                LocalDateTime.now().minusMinutes(1));
      } else {
        LOGGER.debug("Cached token found");
      }
    } catch (IOException e) {
      LOGGER.debug("Failed to load token from cache", e);
      // Initialize with an expired token to force authentication on first use
      this.token =
          new Token(
              DatabricksJdbcConstants.EMPTY_STRING,
              DatabricksJdbcConstants.EMPTY_STRING,
              null,
              LocalDateTime.now().minusMinutes(1));
    }
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

    if (this.hc == null) {
      this.hc = config.getHttpClient();
    }

    return () -> {
      Map<String, String> headers = new HashMap<>();
      headers.put(
          HttpHeaders.AUTHORIZATION, getToken().getTokenType() + " " + getToken().getAccessToken());
      return headers;
    };
  }

  /**
   * Implements the token refresh logic as required by RefreshableTokenSource. This method handles
   * token refreshing, falling back to browser authentication if needed.
   *
   * @return A new or refreshed token
   * @throws DatabricksException If there is an error during the authentication process
   */
  @Override
  protected Token refresh() {
    try {
      // Try to refresh if we have a refresh token
      if (this.token != null && this.token.getRefreshToken() != null) {
        try {
          LOGGER.debug("Using refresh token to get new access token");
          Token refreshedToken = refreshAccessToken();
          tokenCache.save(refreshedToken);
          return refreshedToken;
        } catch (Exception e) {
          LOGGER.info("Failed to refresh access token, will restart browser auth", e);
          // If refresh fails, fall through to browser auth
        }
      }

      // If we get here, we need to do browser auth
      LOGGER.debug("Performing browser authentication to get new access token");
      Token newToken = performBrowserAuth();
      tokenCache.save(newToken);
      return newToken;
    } catch (Exception e) {
      String errorMessage = "Failed to refresh or obtain new token";
      LOGGER.error(errorMessage, e);
      throw new DatabricksException(errorMessage, e);
    }
  }

  /**
   * Refreshes an access token using the refresh token from the current token. This method follows
   * the OAuth 2.0 refresh token flow by sending a request to the token endpoint with the refresh
   * token grant type.
   *
   * @return A new token with a refreshed access token
   * @throws DatabricksException If there is an error during the refresh process or if the token or
   *     refresh token is not available
   */
  @VisibleForTesting
  Token refreshAccessToken() throws DatabricksException {
    if (this.token == null || this.token.getRefreshToken() == null) {
      throw new DatabricksException("oauth2: token is not set or refresh token is not available");
    }

    Map<String, String> params = new HashMap<>();
    params.put(GRANT_TYPE_KEY, GRANT_TYPE_REFRESH_TOKEN_KEY);
    params.put(GRANT_TYPE_REFRESH_TOKEN_KEY, this.token.getRefreshToken());
    Map<String, String> headers = new HashMap<>();
    return retrieveToken(
        hc,
        config.getClientId(),
        config.getClientSecret(),
        tokenEndpoint,
        params,
        headers,
        AuthParameterPosition.BODY);
  }

  /**
   * Performs browser-based authentication to obtain a new token. This method launches a browser
   * window to allow the user to authenticate and authorize the application.
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
}
