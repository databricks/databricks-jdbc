package com.databricks.jdbc.auth;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.sdk.core.CredentialsProvider;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.HeaderFactory;
import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.core.oauth.AuthParameterPosition;
import com.databricks.sdk.core.oauth.RefreshableTokenSource;
import com.databricks.sdk.core.oauth.Token;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpHeaders;

public class JWTAndRefreshCredentialsProvider extends RefreshableTokenSource
    implements CredentialsProvider {
  IDatabricksConnectionContext context;
  private HttpClient hc;
  private final String tokenUrl;
  private final String clientId;
  private final String clientSecret;
  private final String jwt;

  public JWTAndRefreshCredentialsProvider(IDatabricksConnectionContext context) {
    this.context = context;
    if (context.getOAuth2TokenEndpoint() != null) {
      this.tokenUrl = context.getOAuth2TokenEndpoint();
    } else {
      this.tokenUrl = context.getHostForOAuth() + "oidc/v1/token";
    }
    try {
      this.clientId = context.getClientId();
    } catch (DatabricksParsingException e) {
      throw new DatabricksException("Failed to parse client id", e);
    }
    this.clientSecret = context.getClientSecret();
    // Create expired dummy token to refresh
    this.token =
        new Token(
            "xx", "Bearer", context.getOAuthRefreshToken(), LocalDateTime.now().minusMinutes(1));
    try {
      this.jwt = new String(Files.readAllBytes(Paths.get(context.getJwtPath())));
    } catch (IOException e) {
      throw new DatabricksException("Failed to read jwt file", e);
    }
  }

  @Override
  public String authType() {
    return "oauth-refresh-with-jwt";
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
      throw new DatabricksException("oauth2: token is not set");
    }
    String refreshToken = this.token.getRefreshToken();
    if (refreshToken == null) {
      throw new DatabricksException("oauth2: token expired and refresh token is not set");
    }

    Map<String, String> params = new HashMap<>();
    params.put("grant_type", "refresh_token");
    params.put("refresh_token", refreshToken);
    params.put("client_assertion_type", "urn:ietf:params:oauth:client-assertion-type:jwt-bearer");
    params.put("client_assertion", this.jwt);
    Map<String, String> headers = new HashMap<>();
    return retrieveToken(
        hc, clientId, clientSecret, tokenUrl, params, headers, AuthParameterPosition.BODY);
  }
}
