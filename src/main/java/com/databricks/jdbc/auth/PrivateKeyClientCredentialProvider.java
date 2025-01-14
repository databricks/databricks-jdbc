package com.databricks.jdbc.auth;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.util.DatabricksAuthUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.sdk.core.CredentialsProvider;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.HeaderFactory;
import com.databricks.sdk.core.oauth.Token;
import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpHeaders;

public class PrivateKeyClientCredentialProvider implements CredentialsProvider {

  IDatabricksConnectionContext connectionContext;
  String tokenEndpoint;

  IDatabricksHttpClient httpClient;

  public PrivateKeyClientCredentialProvider(
      IDatabricksConnectionContext connectionContext, DatabricksConfig databricksConfig) {
    this.connectionContext = connectionContext;
    this.httpClient = DatabricksHttpClientFactory.getInstance().getClient(connectionContext);
    this.tokenEndpoint = DatabricksAuthUtil.getTokenEndpoint(databricksConfig, connectionContext);
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
