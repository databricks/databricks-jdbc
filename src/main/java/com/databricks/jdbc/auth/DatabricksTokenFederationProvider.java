package com.databricks.jdbc.auth;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.common.util.DriverUtil;
import com.databricks.jdbc.common.util.JsonUtil;
import com.databricks.jdbc.common.util.ValidationUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.exception.DatabricksDriverException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.core.*;
import com.databricks.sdk.core.oauth.CachedTokenSource;
import com.databricks.sdk.core.oauth.OAuthResponse;
import com.databricks.sdk.core.oauth.Token;
import com.databricks.sdk.core.oauth.TokenSource;
import com.google.common.annotations.VisibleForTesting;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.message.BasicNameValuePair;

/**
 * Implementation of the Credential Provider that exchanges the third party access token for a
 * Databricks InHouse Token This class exchanges the access token if the issued token is not from
 * the same host as the Databricks host.
 *
 * <p>Note: In future this class will be replaced with the Databricks SDK implementation
 */
public class DatabricksTokenFederationProvider implements CredentialsProvider, TokenSource {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksTokenFederationProvider.class);

  private HeaderFactory externalHeaderFactory;
  private CachedTokenSource cachedTokenSource;
  private static final Map<String, String> TOKEN_EXCHANGE_PARAMS =
      Map.of(
          "grant_type",
          "urn:ietf:params:oauth:grant-type:token-exchange",
          "scope",
          "sql",
          "subject_token_type",
          "urn:ietf:params:oauth:token-type:jwt",
          "return_original_token_if_authenticated",
          "true");
  private static final String TOKEN_EXCHANGE_ENDPOINT = "/oidc/v1/token";
  private final IDatabricksConnectionContext connectionContext;
  private final CredentialsProvider credentialsProvider;
  private DatabricksConfig config;
  private Map<String, String> externalProviderHeaders;
  private IDatabricksHttpClient hc;

  public DatabricksTokenFederationProvider(
      IDatabricksConnectionContext connectionContext, CredentialsProvider credentialsProvider) {
    this.connectionContext = connectionContext;
    this.credentialsProvider = credentialsProvider;
    this.externalProviderHeaders = new HashMap<>();
    this.hc = DatabricksHttpClientFactory.getInstance().getClient(connectionContext);
    // Initialize a minimal config; real config will be provided via configure(databricksConfig)
    this.config = null;
    this.externalHeaderFactory = null;
  }

  @VisibleForTesting
  DatabricksTokenFederationProvider(
      IDatabricksConnectionContext connectionContext,
      CredentialsProvider credentialsProvider,
      DatabricksConfig config) {
    this.connectionContext = connectionContext;
    this.credentialsProvider = credentialsProvider;
    this.config = config;
    this.externalHeaderFactory = this.credentialsProvider.configure(this.config);
    this.externalProviderHeaders = new HashMap<>();
  }

  public String authType() {
    return this.credentialsProvider.authType();
  }

  public CredentialsProvider getCredentialsProvider() {
    return this.credentialsProvider;
  }

  public HeaderFactory configure(DatabricksConfig databricksConfig) {
    LOGGER.debug("DatabricksTokenFederation configure");

    // ByPassing the token exchange for fake service test
    // Issue: Unable to map token exchange URL to localhost (WireMock host)
    // because URLs are generated inside SDK
    if (DriverUtil.isRunningAgainstFake()) {
      return this.credentialsProvider.configure(databricksConfig);
    }

    this.config = databricksConfig;
    // Call the underlying provider's configure ONCE and cache the HeaderFactory
    this.externalHeaderFactory = this.credentialsProvider.configure(this.config);

    // Initialize CachedTokenSource to cache the exchanged token
    if (this.cachedTokenSource == null) {
      TokenSource refreshLogic = this::fetchAndExchangeToken;
      this.cachedTokenSource = new CachedTokenSource.Builder(refreshLogic).build();
    }

    return () -> {
      Token exchangedToken = getToken();
      Map<String, String> headers = new HashMap<>(this.externalProviderHeaders);
      headers.put(
          HttpHeaders.AUTHORIZATION,
          exchangedToken.getTokenType() + " " + exchangedToken.getAccessToken());
      return headers;
    };
  }

  public Token getToken() {
    if (this.cachedTokenSource == null) {
      throw new DatabricksDriverException(
          "CachedTokenSource not initialized. Call configure() first.",
          DatabricksDriverErrorCode.AUTH_ERROR);
    }
    return cachedTokenSource.getToken();
  }

  /**
   * Fetches token from the underlying provider and exchanges it if needed. This method is wrapped
   * by CachedTokenSource for in-memory caching.
   */
  private Token fetchAndExchangeToken() {
    LOGGER.debug("Fetching and exchanging token");

    if (this.externalHeaderFactory == null) {
      throw new DatabricksDriverException(
          "ExternalHeaderFactory not initialized. Call configure() first.",
          DatabricksDriverErrorCode.AUTH_ERROR);
    }
    this.externalProviderHeaders = this.externalHeaderFactory.headers();
    String[] tokenInfo = extractTokenInfoFromHeader(this.externalProviderHeaders);
    String accessTokenType = tokenInfo[0];
    String accessToken = tokenInfo[1];

    try {
      SignedJWT signedJWT = SignedJWT.parse(accessToken);
      JWTClaimsSet claims = signedJWT.getJWTClaimsSet();

      Optional<Token> optionalToken = Optional.empty();
      if (!isSameHost(claims.getIssuer(), this.config.getHost())) {
        optionalToken = tryTokenExchange(accessToken, accessTokenType);
      }
      if (optionalToken.isEmpty()) {
        optionalToken = Optional.of(createToken(accessToken, accessTokenType));
      }

      LOGGER.debug("Successfully fetched and exchanged token");
      return optionalToken.get();
    } catch (Exception e) {
      LOGGER.error(e, "Failed to refresh access token");
      throw new DatabricksDriverException(
          "Failed to refresh access token", e, DatabricksDriverErrorCode.AUTH_ERROR);
    }
  }

  @VisibleForTesting
  Optional<Token> tryTokenExchange(String accessToken, String accessTokenType) {
    LOGGER.debug(
        "Token tryTokenExchange(String accessToken, String accessTokenType = {})", accessTokenType);
    try {
      return Optional.of(exchangeToken(accessToken));
    } catch (Exception e) {
      LOGGER.error(e, "Token exchange failed, falling back to using external token");
      return Optional.empty();
    }
  }

  @VisibleForTesting
  Token createToken(String accessToken, String accessTokenType) throws ParseException {
    SignedJWT signedJWT = SignedJWT.parse(accessToken);
    JWTClaimsSet claims = signedJWT.getJWTClaimsSet();

    Instant expiry = Instant.ofEpochMilli(claims.getExpirationTime().getTime());
    return new Token(accessToken, accessTokenType, DatabricksJdbcConstants.EMPTY_STRING, expiry);
  }

  @VisibleForTesting
  Token exchangeToken(String accessToken) {
    LOGGER.debug("Token exchangeToken( String accessToken )");
    final String tokenUrl = this.config.getHost() + TOKEN_EXCHANGE_ENDPOINT;

    Map<String, String> params = new HashMap<>(TOKEN_EXCHANGE_PARAMS);
    params.put("subject_token", accessToken);

    if (connectionContext.getIdentityFederationClientId() != null) {
      params.put("client_id", connectionContext.getIdentityFederationClientId());
    }

    Map<String, String> headers = new HashMap<>();
    headers.put(HttpHeaders.ACCEPT, "*/*");
    headers.put(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded");

    return retrieveToken(hc, tokenUrl, params, headers);
  }

  @VisibleForTesting
  Token retrieveToken(
      IDatabricksHttpClient hc,
      String tokenUrl,
      Map<String, String> params,
      Map<String, String> headers) {
    try {
      URIBuilder uriBuilder = new URIBuilder(tokenUrl);
      HttpPost postRequest = new HttpPost(uriBuilder.build());
      postRequest.setEntity(
          new UrlEncodedFormEntity(
              params.entrySet().stream()
                  .map(e -> new BasicNameValuePair(e.getKey(), e.getValue()))
                  .collect(Collectors.toList()),
              StandardCharsets.UTF_8));
      headers.forEach(postRequest::setHeader);
      HttpResponse response = hc.execute(postRequest);

      // Check for HTTP errors and build detailed error message
      String httpError = ValidationUtil.checkHTTPErrorWithoutThrowingError(response);
      if (!httpError.equals(DatabricksJdbcConstants.EMPTY_STRING)) {
        String errorMessage =
            String.format(
                "Failed to retrieve the exchanged token from OIDC token endpoint. %s", httpError);
        LOGGER.error(errorMessage);
        throw new DatabricksDriverException(errorMessage, DatabricksDriverErrorCode.AUTH_ERROR);
      }

      OAuthResponse resp =
          JsonUtil.getMapper().readValue(response.getEntity().getContent(), OAuthResponse.class);
      return createToken(resp.getAccessToken(), resp.getTokenType());
    } catch (DatabricksDriverException e) {
      // Already logged and has telemetry, just re-throw
      throw e;
    } catch (Exception e) {
      String errorMessage =
          String.format(
              "Failed to parse token response from OIDC token endpoint. Error: %s", e.getMessage());
      LOGGER.error(e, errorMessage);
      throw new DatabricksDriverException(errorMessage, e, DatabricksDriverErrorCode.AUTH_ERROR);
    }
  }

  private boolean isSameHost(String url1, String url2) {
    try {
      String host1 = new URL(url1).getHost();
      String host2 = new URL(url2).getHost();
      return host1.equals(host2);
    } catch (MalformedURLException e) {
      LOGGER.error(e, "Unable to parse URL String");
    }
    return false;
  }

  private String[] extractTokenInfoFromHeader(Map<String, String> headers) {
    String authHeader = headers.get(HttpHeaders.AUTHORIZATION);
    try {
      return authHeader.split(" ", 2);
    } catch (NullPointerException e) {
      LOGGER.error(e, "Failed to extract token info from header");
      throw new DatabricksDriverException(
          "Failed to extract token info from header", e, DatabricksDriverErrorCode.AUTH_ERROR);
    }
  }
}
