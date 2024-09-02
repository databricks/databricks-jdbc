package com.databricks.jdbc.auth;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.LogLevel;
import com.databricks.jdbc.common.util.LoggingUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.sdk.core.CredentialsProvider;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.HeaderFactory;
import com.databricks.sdk.core.oauth.OpenIDConnectEndpoints;
import com.databricks.sdk.core.oauth.Token;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;

public class PrivateKeyClientCredentialProvider implements CredentialsProvider {

  IDatabricksConnectionContext connectionContext;
  String tokenEndpoint;

  IDatabricksHttpClient httpClient;

  public PrivateKeyClientCredentialProvider(IDatabricksConnectionContext connectionContext) {
    this(connectionContext, DatabricksHttpClient.getInstance(connectionContext));
  }

  @VisibleForTesting
  PrivateKeyClientCredentialProvider(
      IDatabricksConnectionContext connectionContext, IDatabricksHttpClient httpClient) {
    this.connectionContext = connectionContext;
    this.httpClient = httpClient;
  }

  @Override
  public String authType() {
    return "custom-oauth-m2m";
  }

  @VisibleForTesting
  JwtPrivateKeyClientCredentials getClientCredentialObject(DatabricksConfig config) {
    tokenEndpoint = connectionContext.getTokenEndpoint();
    if (tokenEndpoint == null && connectionContext.isOAuthDiscoveryModeEnabled()) {
      updateOidcFromDiscoveryEndpoint();
    }
    if (tokenEndpoint == null) {
      try {
        tokenEndpoint = config.getOidcEndpoints().getTokenEndpoint();
      } catch (IOException e) {
        LoggingUtil.log(LogLevel.ERROR, "Unable to set default token endpoint with error " + e);
      }
    }
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
      headers.put("Authorization", token.getTokenType() + " " + token.getAccessToken());
      headers.put("Content-Type", "application/x-www-form-urlencoded");
      return headers;
    };
  }

  /*
   * TODO : The following will be removed once SDK changes are merged
   * https://github.com/databricks/databricks-sdk-java/pull/336
   * */
  private void updateOidcFromDiscoveryEndpoint() {
    if (connectionContext.getOAuthDiscoveryURL() == null) {
      LoggingUtil.log(
          LogLevel.ERROR, "If discovery mode is enabled, we also need to put discovery URL");
      // not throwing exception as the interface does not support it
      return;
    }
    try {
      URIBuilder uriBuilder = new URIBuilder(connectionContext.getOAuthDiscoveryURL());
      HttpGet getRequest = new HttpGet(uriBuilder.build());
      CloseableHttpResponse response = this.httpClient.execute(getRequest);
      if (response.getStatusLine().getStatusCode() != 200) {
        LoggingUtil.log(
            LogLevel.DEBUG,
            "Error while calling discovery endpoint to fetch token endpoint. Response: "
                + response);
      }
      OpenIDConnectEndpoints openIDConnectEndpoints =
          new ObjectMapper()
              .readValue(response.getEntity().getContent(), OpenIDConnectEndpoints.class);
      tokenEndpoint = openIDConnectEndpoints.getTokenEndpoint();
    } catch (URISyntaxException | DatabricksHttpException | IOException e) {
      LoggingUtil.log(
          LogLevel.ERROR,
          "Unable to retrieve token and auth endpoint from discovery endpoint. Error " + e);
    }
  }
}
