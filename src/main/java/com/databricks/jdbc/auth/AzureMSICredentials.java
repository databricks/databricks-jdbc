package com.databricks.jdbc.auth;

import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.oauth.OAuthResponse;
import com.databricks.sdk.core.oauth.RefreshableTokenSource;
import com.databricks.sdk.core.oauth.Token;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;

public class AzureMSICredentials extends RefreshableTokenSource {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(AzureMSICredentials.class);
  private static String AZURE_METADATA_SERVICE_TOKEN_URL =
      "http://169.254.169.254/metadata/identity/oauth2/token";
  private static final String API_VERSION = "2021-10-01";
  private static final String AZURE_DATABRICKS_SCOPE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d";
  private static final String AZURE_MANAGEMENT_ENDPOINT = "https://management.core.windows.net/";

  private final IDatabricksHttpClient hc;

  private final String clientId;

  AzureMSICredentials(IDatabricksHttpClient hc, String clientId) {
    this.hc = hc;
    this.clientId = clientId;
  }

  @Override
  protected Token refresh() {
    return getTokenForResource(AZURE_DATABRICKS_SCOPE);
  }

  public Token getManagementEndpointToken() {
    return getTokenForResource(AZURE_MANAGEMENT_ENDPOINT);
  }

  private Token getTokenForResource(String resource) {
    Map<String, String> params = new HashMap<>();
    params.put("api-version", API_VERSION);
    params.put("resource", resource);
    if (clientId != null) {
      params.put("client_id", clientId);
    }
    Map<String, String> headers = new HashMap<>();
    headers.put("Metadata", "true");
    return retrieveToken(hc, AZURE_METADATA_SERVICE_TOKEN_URL, params, headers);
  }

  @VisibleForTesting
  protected static Token retrieveToken(
      IDatabricksHttpClient hc,
      String tokenUrl,
      Map<String, String> params,
      Map<String, String> headers) {
    try {
      URIBuilder uriBuilder = new URIBuilder(tokenUrl);
      params.forEach(uriBuilder::addParameter);
      HttpGet getRequest = new HttpGet(uriBuilder.build());
      headers.forEach(getRequest::setHeader);
      System.out.println(getRequest.toString());
      HttpResponse response = hc.execute(getRequest);
      OAuthResponse resp =
          new ObjectMapper().readValue(response.getEntity().getContent(), OAuthResponse.class);
      LocalDateTime expiry = LocalDateTime.now().plus(resp.getExpiresIn(), ChronoUnit.SECONDS);
      return new Token(resp.getAccessToken(), resp.getTokenType(), resp.getRefreshToken(), expiry);
    } catch (IOException | URISyntaxException | DatabricksHttpException e) {
      String errorMessage = "Failed to retrieve Azure MSI token: " + e.getMessage();
      LOGGER.error(errorMessage);
      throw new DatabricksException(errorMessage, e);
    }
  }
}
