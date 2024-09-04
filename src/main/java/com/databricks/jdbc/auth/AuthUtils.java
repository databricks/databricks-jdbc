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
        String exceptionMessage = "Failed to get token endpoint from discovery endpoint";
        LoggingUtil.log(LogLevel.ERROR, exceptionMessage);
        throw new DatabricksException(exceptionMessage, e);
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
        String exceptionMessage = "Failed to build token url";
        LoggingUtil.log(LogLevel.ERROR, exceptionMessage);
        throw new DatabricksException(exceptionMessage, e);
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
