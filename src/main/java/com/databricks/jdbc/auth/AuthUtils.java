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
    // Check if the token endpoint is explicitly set
    String tokenEndpoint = context.getTokenEndpoint();
    if (tokenEndpoint != null) {
      return tokenEndpoint;
    }

    // If OAuth discovery mode is enabled, try to get the token endpoint from the discovery service
    if (context.isOAuthDiscoveryModeEnabled()) {
      return getTokenEndpointWithDiscovery(context);
    }

    // Fall back to the default token endpoint if no discovery mode or token endpoint is available
    return getDefaultTokenEndpoint(context);
  }

  private static String getTokenEndpointWithDiscovery(IDatabricksConnectionContext context) {
    try {
      return getTokenEndpointFromDiscoveryEndpoint(context);
    } catch (DatabricksException e) {
      String errorMessage =
          "Failed to get token endpoint from discovery endpoint. Falling back to default token endpoint.";
      LoggingUtil.log(LogLevel.ERROR, errorMessage);
      return getDefaultTokenEndpoint(context);
    }
  }

  static String getDefaultTokenEndpoint(IDatabricksConnectionContext context) {
    try {
      return new URIBuilder()
          .setHost(context.getHostForOAuth())
          .setScheme("https")
          .setPathSegments("oidc", "v1", "token")
          .build()
          .toString();
    } catch (URISyntaxException e) {
      String errorMessage = "Failed to build default token endpoint URL.";
      LoggingUtil.log(LogLevel.ERROR, errorMessage);
      throw new DatabricksException(errorMessage, e);
    }
  }

  /*
   * TODO : The following will be removed once SDK changes are merged
   * https://github.com/databricks/databricks-sdk-java/pull/336
   * */
  private static String getTokenEndpointFromDiscoveryEndpoint(
      IDatabricksConnectionContext connectionContext) throws DatabricksException {
    if (connectionContext.getOAuthDiscoveryURL() == null) {
      String exceptionMessage =
          "If discovery mode is enabled, we also need the discovery URL to be set.";
      LoggingUtil.log(LogLevel.ERROR, exceptionMessage);
      throw new DatabricksException(exceptionMessage);
    }
    try {
      URIBuilder uriBuilder = new URIBuilder(connectionContext.getOAuthDiscoveryURL());
      DatabricksHttpClient httpClient = DatabricksHttpClient.getInstance(connectionContext);
      HttpGet getRequest = new HttpGet(uriBuilder.build());
      try (CloseableHttpResponse response = httpClient.execute(getRequest)) {
        if (response.getStatusLine().getStatusCode() != 200) {
          String exceptionMessage =
              "Error while calling discovery endpoint to fetch token endpoint. Response: "
                  + response;
          LoggingUtil.log(LogLevel.DEBUG, exceptionMessage);
          throw new DatabricksHttpException(exceptionMessage);
        }
        OpenIDConnectEndpoints openIDConnectEndpoints =
            new ObjectMapper()
                .readValue(response.getEntity().getContent(), OpenIDConnectEndpoints.class);
        return openIDConnectEndpoints.getTokenEndpoint();
      }
    } catch (URISyntaxException | DatabricksHttpException | IOException e) {
      String exceptionMessage = "Failed to get token endpoint from discovery endpoint";
      LoggingUtil.log(LogLevel.ERROR, exceptionMessage);
      throw new DatabricksException(exceptionMessage, e);
    }
  }
}
