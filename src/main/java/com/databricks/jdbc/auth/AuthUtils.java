package com.databricks.jdbc.auth;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.LogLevel;
import com.databricks.jdbc.common.util.LoggingUtil;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.oauth.OpenIDConnectEndpoints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
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
    } else if (context.isOAuthDiscoveryModeEnabled() && context.getOAuthDiscoveryURL() != null) {
      tokenUrl = getTokenEndpointFromDiscoveryEndpoint(context);
    } else {
      tokenUrl = getTokenEndpointUsingDatabricksConfig(context);
    }
    return tokenUrl;
  }

  public static String getTokenEndpointUsingDatabricksConfig(IDatabricksConnectionContext context) {
    try {
      return getBarebonesDatabricksConfig(context).getOidcEndpoints().getTokenEndpoint();
    } catch (DatabricksParsingException | IOException e) {
      String exceptionMessage = "Failed to build token url";
      LoggingUtil.log(LogLevel.ERROR, exceptionMessage);
      throw new DatabricksException(exceptionMessage, e);
    }
  }

  @VisibleForTesting
  public static DatabricksConfig getBarebonesDatabricksConfig(IDatabricksConnectionContext context)
      throws DatabricksParsingException {
    return new DatabricksConfig().setHost(context.getHostUrl()).resolve();
  }

  /*
   * TODO : The following will be removed once SDK changes are merged
   * https://github.com/databricks/databricks-sdk-java/pull/336
   * */
  private static String getTokenEndpointFromDiscoveryEndpoint(
      IDatabricksConnectionContext connectionContext) {
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
