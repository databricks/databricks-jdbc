package com.databricks.jdbc.auth;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.common.LogLevel;
import com.databricks.jdbc.common.util.LoggingUtil;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.sdk.core.CredentialsProvider;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.HeaderFactory;
import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.core.oauth.AuthParameterPosition;
import com.databricks.sdk.core.oauth.RefreshableTokenSource;
import com.databricks.sdk.core.oauth.Token;
import org.apache.http.HttpHeaders;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import static com.databricks.jdbc.auth.AuthConstants.*;

public class OAuthRefreshWithOptionalJwtCredentialsProvider extends RefreshableTokenSource
    implements CredentialsProvider {
  IDatabricksConnectionContext context;
  private HttpClient hc;
  private final String tokenUrl;
  private final String clientId;
  private final String clientSecret;
  private String jwt = null;

  public OAuthRefreshWithOptionalJwtCredentialsProvider(IDatabricksConnectionContext context) {
    this.context = context;
    this.tokenUrl = AuthUtils.getTokenEndpoint(context);
    try {
      this.clientId = context.getClientId();
    } catch (DatabricksParsingException e) {
      throw new DatabricksException("Failed to parse client id", e);
    }
    this.clientSecret = context.getClientSecret();
    // Create an expired dummy token object with the refresh token to use
    this.token =
        new Token(
            DatabricksJdbcConstants.EMPTY_STRING, DatabricksJdbcConstants.EMPTY_STRING,
                context.getOAuthRefreshToken(), LocalDateTime.now().minusMinutes(1));
    if (context.getEncodedJwt() != null) {
      this.jwt = context.getEncodedJwt();
    }
  }

  @Override
  public String authType() {
    return "oauth-refresh-with-optional-jwt";
  }

  @Override
  public HeaderFactory configure(DatabricksConfig databricksConfig) {
    if (this.hc == null) {
      this.hc = databricksConfig.getHttpClient();
    }
    return () -> {
      Map<String, String> headers = new HashMap<>();
      headers.put(
          HttpHeaders.AUTHORIZATION, getToken().getTokenType() + " " + getToken().getAccessToken());
      return headers;
    };
  }

  @Override
  protected Token refresh() {
    if (this.token == null) {
      LoggingUtil.log(LogLevel.ERROR, "oauth2: token is not set");
      throw new DatabricksException("oauth2: token is not set");
    }
    String refreshToken = this.token.getRefreshToken();
    if (refreshToken == null) {
      LoggingUtil.log(LogLevel.ERROR, "oauth2: token expired and refresh token is not set");
      throw new DatabricksException("oauth2: token expired and refresh token is not set");
    }

    Map<String, String> params = new HashMap<>();
    params.put(GRANT_TYPE_KEY, GRANT_TYPE_REFRESH_TOKEN_KEY);
    params.put(GRANT_TYPE_REFRESH_TOKEN_KEY, refreshToken);
    if (this.jwt != null) {
      params.put(CLIENT_ASSERTION_TYPE_KEY, CLIENT_ASSERTION_TYPE_JWT_OAUTH);
      params.put(CLIENT_ASSERTION_KEY, this.jwt);
    }
    Map<String, String> headers = new HashMap<>();
    return retrieveToken(
        hc, clientId, clientSecret, tokenUrl, params, headers, AuthParameterPosition.BODY);
  }
}
