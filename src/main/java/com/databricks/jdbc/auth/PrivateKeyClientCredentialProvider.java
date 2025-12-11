package com.databricks.jdbc.auth;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.util.DatabricksAuthUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.sdk.core.CredentialsProvider;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.HeaderFactory;
import com.databricks.sdk.core.oauth.Token;
import com.databricks.sdk.core.oauth.TokenCache;
import com.google.common.annotations.VisibleForTesting;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpHeaders;

public class PrivateKeyClientCredentialProvider implements CredentialsProvider {

  IDatabricksConnectionContext connectionContext;
  String tokenEndpoint;

  IDatabricksHttpClient httpClient;
  TokenCache tokenCache;

  public PrivateKeyClientCredentialProvider(
      IDatabricksConnectionContext connectionContext, DatabricksConfig databricksConfig) {
    this.connectionContext = connectionContext;
    this.httpClient = DatabricksHttpClientFactory.getInstance().getClient(connectionContext);
    this.tokenEndpoint = DatabricksAuthUtil.getTokenEndpoint(databricksConfig, connectionContext);
    this.tokenCache = createTokenCache(databricksConfig);
  }

  /**
   * Creates a TokenCache instance for JWT M2M credentials.
   *
   * <p>Uses the connection UUID to ensure each connection has its own isolated token cache,
   * preventing cache collisions between different connections.
   *
   * @param config The Databricks configuration
   * @return An EncryptedFileTokenCache instance
   */
  private TokenCache createTokenCache(DatabricksConfig config) {
    String userHome = System.getProperty("user.home");
    Path homeDir = Paths.get(userHome);
    Path databricksDir = homeDir.resolve(".config/databricks-jdbc/oauth");

    // Use connection UUID directly as cache filename - guarantees uniqueness
    String connectionUuid = connectionContext.getConnectionUuid();
    Path cachePath = databricksDir.resolve(connectionUuid);

    return TokenCacheUtils.createEncryptedCache(
        cachePath,
        connectionContext.getTokenCachePassPhrase(),
        config.getHost(),
        config.getClientId());
  }

  @Override
  public String authType() {
    return "custom-oauth-m2m";
  }

  @VisibleForTesting
  JwtPrivateKeyClientCredentials getClientCredentialObject(DatabricksConfig config) {
    return new JwtPrivateKeyClientCredentials.Builder()
        .withHttpClient(this.httpClient)
        .withClientId(config.getClientId())
        .withJwtKid(connectionContext.getKID())
        .withJwtKeyFile(connectionContext.getJWTKeyFile())
        .withJwtKeyPassphrase(connectionContext.getJWTPassphrase())
        .withJwtAlgorithm(connectionContext.getJWTAlgorithm())
        .withTokenUrl(tokenEndpoint)
        .withScopes(Collections.singletonList(connectionContext.getAuthScope()))
        .withTokenCache(this.tokenCache)
        .build();
  }

  @Override
  public HeaderFactory configure(DatabricksConfig config) {
    JwtPrivateKeyClientCredentials clientCredentials = getClientCredentialObject(config);
    return () -> {
      Token token = clientCredentials.getToken();
      Map<String, String> headers = new HashMap<>();
      headers.put(HttpHeaders.AUTHORIZATION, token.getTokenType() + " " + token.getAccessToken());
      headers.put(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded");
      return headers;
    };
  }
}
