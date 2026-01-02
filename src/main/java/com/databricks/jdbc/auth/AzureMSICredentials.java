package com.databricks.jdbc.auth;

import com.databricks.jdbc.common.util.JsonUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.oauth.CachedTokenSource;
import com.databricks.sdk.core.oauth.OAuthResponse;
import com.databricks.sdk.core.oauth.Token;
import com.databricks.sdk.core.oauth.TokenSource;
import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;

/**
 * Provides authentication functionality using Azure Managed Service Identity (MSI).
 *
 * <p>This class implements TokenSource to handle token retrieval and refreshing for Databricks
 * services running on Azure. It leverages the Azure Instance Metadata Service to obtain OAuth
 * tokens that can be used to authenticate with Databricks and Azure Management endpoints.
 *
 * <p>The class supports both user-assigned and system-assigned managed identities. For
 * user-assigned managed identities, a client ID should be provided.
 */
public class AzureMSICredentials implements TokenSource {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(AzureMSICredentials.class);

  /** The Azure Instance Metadata Service endpoint for token retrieval */
  private static final String AZURE_METADATA_SERVICE_TOKEN_URL =
      "http://169.254.169.254/metadata/identity/oauth2/token";

  /** The API version for the Azure Instance Metadata Service */
  private static final String API_VERSION = "2021-10-01";

  /** The resource ID for Databricks service in Azure */
  private static final String AZURE_DATABRICKS_SCOPE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d";

  /** The resource URL for Azure Management endpoint */
  private static final String AZURE_MANAGEMENT_ENDPOINT = "https://management.core.windows.net/";

  /** The HTTP client used for making token requests */
  private final IDatabricksHttpClient hc;

  /** The client ID for user-assigned managed identity (null for system-assigned) */
  private final String clientId;

  /** In-memory token cache for Databricks scope tokens */
  private final CachedTokenSource databricksTokenCache;

  /** In-memory token cache for management endpoint tokens */
  private final CachedTokenSource managementTokenCache;

  /**
   * Constructs a new AzureMSICredentials instance with token caching.
   *
   * @param hc The HTTP client to use for making token requests
   * @param clientId The client ID for user-assigned managed identity, or null for system-assigned
   */
  AzureMSICredentials(IDatabricksHttpClient hc, String clientId) {
    this.hc = hc;
    this.clientId = clientId;

    // Initialize in-memory token caches
    TokenSource databricksRefreshLogic = () -> getTokenForResource(AZURE_DATABRICKS_SCOPE);
    this.databricksTokenCache = new CachedTokenSource.Builder(databricksRefreshLogic).build();

    TokenSource managementRefreshLogic = () -> getTokenForResource(AZURE_MANAGEMENT_ENDPOINT);
    this.managementTokenCache = new CachedTokenSource.Builder(managementRefreshLogic).build();
  }

  /**
   * Retrieves the Databricks access token.
   *
   * <p>This method uses an in-memory cache for fast token retrieval. Tokens are refreshed
   * automatically when expired.
   *
   * @return A Token object containing the access token
   */
  @Override
  public Token getToken() {
    return databricksTokenCache.getToken();
  }

  /**
   * Retrieves a token for accessing the Azure Management endpoint.
   *
   * <p>This method uses an in-memory cache for fast token retrieval. Tokens are refreshed
   * automatically when expired.
   *
   * <p>This token is used for operations that require access to Azure Resource Manager, such as
   * managing workspace resources.
   *
   * @return A Token object containing the access token for the Azure Management endpoint
   */
  public Token getManagementEndpointToken() {
    return managementTokenCache.getToken();
  }

  /**
   * Retrieves a token for a specific Azure resource.
   *
   * <p>This method makes a request to the Azure Instance Metadata Service to obtain an access token
   * for the specified resource.
   *
   * @param resource The resource identifier or URL for which to obtain a token
   * @return A Token object containing the access token for the specified resource
   */
  private Token getTokenForResource(String resource) {
    Map<String, String> params = new HashMap<>();
    params.put("api-version", API_VERSION);
    params.put("resource", resource);
    if (clientId != null) {
      LOGGER.debug(
          "Attempting to connect via Azure user-assigned managed identity with client ID: {}",
          clientId);
      params.put("client_id", clientId);
    } else {
      LOGGER.debug("Attempting to connect via Azure system-assigned managed identity");
    }
    Map<String, String> headers = new HashMap<>();
    headers.put("Metadata", "true");
    return retrieveToken(hc, AZURE_METADATA_SERVICE_TOKEN_URL, params, headers);
  }

  /**
   * Makes an HTTP request to retrieve an OAuth token.
   *
   * <p>This method handles the details of building the request, sending it, parsing the response,
   * and constructing a Token object.
   *
   * @param hc The HTTP client to use for the request
   * @param tokenUrl The URL of the token endpoint
   * @param params Query parameters to include in the request
   * @param headers HTTP headers to include in the request
   * @return A Token object containing the retrieved access token and related information
   * @throws DatabricksException If an error occurs during token retrieval
   */
  private static Token retrieveToken(
      IDatabricksHttpClient hc,
      String tokenUrl,
      Map<String, String> params,
      Map<String, String> headers) {
    try {
      URIBuilder uriBuilder = new URIBuilder(tokenUrl);
      params.forEach(uriBuilder::addParameter);
      HttpGet getRequest = new HttpGet(uriBuilder.build());
      headers.forEach(getRequest::setHeader);
      LOGGER.debug("Executing GET request to retrieve Azure MSI token");
      HttpResponse response = hc.execute(getRequest);
      OAuthResponse resp =
          JsonUtil.getMapper().readValue(response.getEntity().getContent(), OAuthResponse.class);
      Instant expiry = Instant.now().plus(resp.getExpiresIn(), ChronoUnit.SECONDS);
      LOGGER.debug("Azure MSI Token retrieved successfully");
      return new Token(resp.getAccessToken(), resp.getTokenType(), resp.getRefreshToken(), expiry);
    } catch (IOException | URISyntaxException | DatabricksHttpException e) {
      String errorMessage = "Failed to retrieve Azure MSI token: " + e.getMessage();
      LOGGER.error(errorMessage);
      throw new DatabricksException(errorMessage, e);
    }
  }
}
