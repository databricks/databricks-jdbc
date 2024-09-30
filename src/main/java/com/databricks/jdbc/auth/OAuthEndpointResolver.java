package com.databricks.jdbc.auth;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
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

public class OAuthEndpointResolver {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(OAuthEndpointResolver.class);
  private final IDatabricksConnectionContext context;

  public OAuthEndpointResolver(IDatabricksConnectionContext context) {
    this.context = context;
  }

  public String getTokenEndpoint() {
    // Check if the token endpoint is explicitly set
    String tokenEndpoint = context.getTokenEndpoint();
    if (tokenEndpoint != null) {
      return tokenEndpoint;
    }

    // If OAuth discovery mode is enabled, try to get the token endpoint from the discovery service
    if (context.isOAuthDiscoveryModeEnabled() && context.getOAuthDiscoveryURL() != null) {
      return getTokenEndpointWithDiscovery();
    }

    // Fall back to the default token endpoint if no discovery mode or token endpoint is available
    return getDefaultTokenEndpoint();
  }

  private String getTokenEndpointWithDiscovery() {
    try {
      return getTokenEndpointFromDiscoveryEndpoint();
    } catch (DatabricksException e) {
      String errorMessage =
          "Failed to get token endpoint from discovery endpoint. Falling back to default token endpoint.";
      LOGGER.error(errorMessage);
      return getDefaultTokenEndpoint();
    }
  }

  String getDefaultTokenEndpoint() {
    try {
      return getBarebonesDatabricksConfig().getOidcEndpoints().getTokenEndpoint();
    } catch (DatabricksParsingException | IOException e) {
      String errorMessage = "Failed to build default token endpoint URL.";
      LOGGER.error(errorMessage);
      throw new DatabricksException(errorMessage, e);
    }
  }

  @VisibleForTesting
  DatabricksConfig getBarebonesDatabricksConfig() throws DatabricksParsingException {
    return new DatabricksConfig().setHost(context.getHostForOAuth()).resolve();
  }

  /**
   * Fetches the token endpoint from the discovery endpoint.
   *
   * <p>TODO: Remove once the <a
   * href="https://github.com/databricks/databricks-sdk-java/pull/336">PR</a> is merged
   */
  private String getTokenEndpointFromDiscoveryEndpoint() {
    try {
      URIBuilder uriBuilder = new URIBuilder(context.getOAuthDiscoveryURL());
      DatabricksHttpClient httpClient = DatabricksHttpClient.getInstance(context);
      HttpGet getRequest = new HttpGet(uriBuilder.build());
      try (CloseableHttpResponse response = httpClient.execute(getRequest)) {
        if (response.getStatusLine().getStatusCode() != 200) {
          String exceptionMessage =
              "Error while calling discovery endpoint to fetch token endpoint. Response: "
                  + response;
          LOGGER.debug(exceptionMessage);
          throw new DatabricksHttpException(exceptionMessage);
        }
        OpenIDConnectEndpoints openIDConnectEndpoints =
            new ObjectMapper()
                .readValue(response.getEntity().getContent(), OpenIDConnectEndpoints.class);
        return openIDConnectEndpoints.getTokenEndpoint();
      }
    } catch (URISyntaxException | DatabricksHttpException | IOException e) {
      String exceptionMessage = "Failed to get token endpoint from discovery endpoint";
      LOGGER.error(exceptionMessage);
      throw new DatabricksException(exceptionMessage, e);
    }
  }
}
