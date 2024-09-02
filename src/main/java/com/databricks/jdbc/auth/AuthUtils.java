package com.databricks.jdbc.auth;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.LogLevel;
import com.databricks.jdbc.common.util.LoggingUtil;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.oauth.OpenIDConnectEndpoints;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;

public class AuthUtils {
  public static String getTokenEndpoint(IDatabricksConnectionContext context) {
    String tokenUrl;
    if (context.getTokenEndpoint() != null) {
      tokenUrl = context.getTokenEndpoint();
    } else if (context.isOAuthDiscoveryModeEnabled()) {
      try {
        tokenUrl = getTokenEndpointFromDiscoveryEndpoint(context);
      } catch (DatabricksException e) {
        LoggingUtil.log(LogLevel.ERROR, "Failed to get token endpoint from discovery endpoint");
        throw new DatabricksException("Failed to get token endpoint from discovery endpoint", e);
      }
    } else {
      try {
        tokenUrl =
            new URIBuilder()
                .setHost(context.getHostForOAuth())
                .setScheme("https")
                .setPathSegments("oidc", "v1", "token")
                .build()
                .toString();
      } catch (URISyntaxException e) {
        LoggingUtil.log(LogLevel.ERROR, "Failed to build token url");
        throw new DatabricksException("Failed to build token url", e);
      }
    }
    return tokenUrl;
  }

  /*
   * TODO : The following will be removed once SDK changes are merged
   * https://github.com/databricks/databricks-sdk-java/pull/336
   * */
  private static String getTokenEndpointFromDiscoveryEndpoint(
      IDatabricksConnectionContext connectionContext) throws DatabricksException {
    if (connectionContext.getOAuthDiscoveryURL() == null) {
      LoggingUtil.log(
          LogLevel.ERROR,
          "If discovery mode is enabled, we also need the discovery URL to be set.");
      throw new DatabricksException(
          "If discovery mode is enabled, we also need the discovery URL to be set");
    }
    try {
      URIBuilder uriBuilder = new URIBuilder(connectionContext.getOAuthDiscoveryURL());
      DatabricksHttpClient httpClient = DatabricksHttpClient.getInstance(connectionContext);
      HttpGet getRequest = new HttpGet(uriBuilder.build());
      try (CloseableHttpResponse response = httpClient.execute(getRequest)) {
        if (response.getStatusLine().getStatusCode() != 200) {
          LoggingUtil.log(
              LogLevel.DEBUG,
              "Error while calling discovery endpoint to fetch token endpoint. Response: "
                  + response);
          throw new DatabricksHttpException(
              "Error while calling discovery endpoint to fetch token endpoint. Response: "
                  + response);
        }
        OpenIDConnectEndpoints openIDConnectEndpoints =
            new ObjectMapper()
                .readValue(response.getEntity().getContent(), OpenIDConnectEndpoints.class);
        return openIDConnectEndpoints.getTokenEndpoint();
      }
    } catch (URISyntaxException | DatabricksHttpException | IOException e) {
      LoggingUtil.log(
          LogLevel.ERROR,
          "Unable to retrieve token and auth endpoint from discovery endpoint. Error " + e);
      throw new DatabricksException(
          "Unable to retrieve token and auth endpoint from discovery endpoint. Error " + e);
    }
  }
}
