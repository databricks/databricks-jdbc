package com.databricks.jdbc.auth;

import static com.databricks.jdbc.auth.AuthConstants.GRANT_TYPE_KEY;
import static com.databricks.jdbc.auth.AuthConstants.GRANT_TYPE_REFRESH_TOKEN_KEY;
import static com.databricks.sdk.core.oauth.TokenEndpointClient.retrieveToken;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.common.util.DatabricksAuthUtil;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.sdk.core.CredentialsProvider;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.HeaderFactory;
import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.core.oauth.AuthParameterPosition;
import com.databricks.sdk.core.oauth.Token;
import com.databricks.sdk.core.oauth.TokenCache;
import com.databricks.sdk.core.oauth.TokenSource;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpHeaders;

public class OAuthRefreshCredentialsProvider implements TokenSource, CredentialsProvider {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(OAuthRefreshCredentialsProvider.class);

  private HttpClient hc;
  private final String tokenEndpoint;
  private final String clientId;
  private final String clientSecret;
  private final Token token;
  private final TokenCache tokenCache;

  public OAuthRefreshCredentialsProvider(
      IDatabricksConnectionContext context, DatabricksConfig databricksConfig) {
    this(context, databricksConfig, new NoOpTokenCache());
  }

  public OAuthRefreshCredentialsProvider(
      IDatabricksConnectionContext context,
      DatabricksConfig databricksConfig,
      TokenCache tokenCache) {
    this.tokenEndpoint = DatabricksAuthUtil.getTokenEndpoint(databricksConfig, context);
    try {
      this.clientId = context.getClientId();
    } catch (DatabricksParsingException e) {
      String exceptionMessage = "Failed to parse client id";
      LOGGER.error(exceptionMessage);
      throw new DatabricksException(exceptionMessage, e);
    }
    this.clientSecret = context.getClientSecret();
    this.tokenCache = tokenCache;
    // Create an expired dummy token object with the refresh token to use
    this.token =
        new Token(
            DatabricksJdbcConstants.EMPTY_STRING,
            DatabricksJdbcConstants.EMPTY_STRING,
            context.getOAuthRefreshToken(),
            Instant.now().minus(Duration.ofMinutes(1)));
  }

  @Override
  public String authType() {
    return "oauth-refresh";
  }

  @Override
  public HeaderFactory configure(DatabricksConfig databricksConfig) {
    if (this.hc == null) {
      this.hc = databricksConfig.getHttpClient();
    }
    return () -> {
      Map<String, String> headers = new HashMap<>();
      // An example header looks like: "Authorization: Bearer <access-token>"
      headers.put(
          HttpHeaders.AUTHORIZATION, getToken().getTokenType() + " " + getToken().getAccessToken());
      return headers;
    };
  }

  @Override
  public Token getToken() {
    // 1. Check cache first for a valid token
    Token cachedToken = TokenCacheUtils.loadValidToken(tokenCache);
    if (cachedToken != null) {
      LOGGER.debug("Using cached OAuth refresh token");
      return cachedToken;
    }

    // 2. Cache miss or expired token - refresh using the refresh token
    if (this.token == null) {
      String exceptionMessage = "oauth2: token is not set";
      LOGGER.error(exceptionMessage);
      throw new DatabricksException(exceptionMessage);
    }
    String refreshToken = this.token.getRefreshToken();
    if (refreshToken == null) {
      String exceptionMessage = "oauth2: token expired and refresh token is not set";
      LOGGER.error(exceptionMessage);
      throw new DatabricksException(exceptionMessage);
    }

    Map<String, String> params = new HashMap<>();
    params.put(GRANT_TYPE_KEY, GRANT_TYPE_REFRESH_TOKEN_KEY);
    params.put(GRANT_TYPE_REFRESH_TOKEN_KEY, refreshToken);
    Map<String, String> headers = new HashMap<>();
    Token newToken =
        retrieveToken(
            hc, clientId, clientSecret, tokenEndpoint, params, headers, AuthParameterPosition.BODY);

    // 3. Cache the new token for future use
    tokenCache.save(newToken);
    LOGGER.debug("Refreshed and cached new OAuth token");

    return newToken;
  }
}
