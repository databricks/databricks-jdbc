package com.databricks.jdbc.auth;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.AZURE_MSI_AUTH_TYPE;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.sdk.core.CredentialsProvider;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.HeaderFactory;
import com.databricks.sdk.core.oauth.TokenCache;
import com.google.common.annotations.VisibleForTesting;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpHeaders;

/**
 * Implementation of CredentialsProvider that uses Azure Managed Service Identity (MSI) for
 * authentication with Databricks services.
 *
 * <p>This provider obtains access tokens from Azure MSI and includes them in the request headers
 * for Databricks API calls. It supports both resource ID-based authentication and direct token
 * authentication.
 *
 * <p>When Azure Workspace Resource ID is provided, the provider will include both the resource ID
 * and a management endpoint token in the request headers.
 */
public class AzureMSICredentialProvider implements CredentialsProvider {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(AzureMSICredentialProvider.class);
  private final IDatabricksHttpClient httpClient;
  private final String resourceId;
  private final String clientId;
  private final TokenCache databricksTokenCache;
  private final TokenCache managementTokenCache;

  /**
   * Constructs a new AzureMSICredentialProvider.
   *
   * @param connectionContext The Databricks connection context containing authentication parameters
   *     such as client ID and Azure workspace resource ID.
   */
  public AzureMSICredentialProvider(IDatabricksConnectionContext connectionContext) {
    this.httpClient = DatabricksHttpClientFactory.getInstance().getClient(connectionContext);
    this.clientId = connectionContext.getNullableClientId();
    this.resourceId = connectionContext.getAzureWorkspaceResourceId();
    this.databricksTokenCache = createTokenCache(connectionContext, "databricks");
    this.managementTokenCache = createTokenCache(connectionContext, "management");
  }

  /**
   * Creates a TokenCache instance for Azure MSI credentials.
   * Azure MSI requires TWO separate caches:
   * - databricks scope: for Databricks API calls (resource: 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d)
   * - management scope: for Azure Resource Manager calls (resource: https://management.core.windows.net/)
   *
   * @param connectionContext The connection context
   * @param scope The scope identifier (databricks or management)
   * @return An EncryptedFileTokenCache instance
   */
  private TokenCache createTokenCache(IDatabricksConnectionContext connectionContext, String scope) {
    String userHome = System.getProperty("user.home");
    Path homeDir = Paths.get(userHome);
    Path databricksDir = homeDir.resolve(".config/databricks-jdbc/oauth");

    // Create unique cache path based on host + clientId + scope
    String host = connectionContext.getHostForOAuth();
    String cacheId = (host + clientId + scope).hashCode() + "";
    Path cachePath = databricksDir.resolve("token-cache-azure-msi-" + cacheId);

    // For scope-specific caches, append scope to the passphrase
    String configuredPassphrase = connectionContext.getTokenCachePassPhrase();
    String passphraseWithScope = configuredPassphrase;
    if (configuredPassphrase == null || configuredPassphrase.isEmpty()) {
      passphraseWithScope = null; // Let utility generate default
    } else {
      passphraseWithScope = configuredPassphrase + "-" + scope;
    }

    return TokenCacheUtils.createEncryptedCache(
        cachePath,
        passphraseWithScope,
        host,
        clientId);
  }

  /**
   * Constructs a new AzureMSICredentialProvider with a custom HTTP client. This constructor is
   * primarily used for testing purposes.
   *
   * @param connectionContext The Databricks connection context containing authentication
   *     parameters.
   * @param httpClient The HTTP client to use for making authentication requests.
   */
  @VisibleForTesting
  AzureMSICredentialProvider(
      IDatabricksConnectionContext connectionContext, IDatabricksHttpClient httpClient) {
    this.httpClient = httpClient;
    this.clientId = connectionContext.getNullableClientId();
    this.resourceId = connectionContext.getAzureWorkspaceResourceId();
  }

  /**
   * Returns the authentication type identifier for this provider.
   *
   * @return The string constant representing Azure MSI authentication type.
   */
  @Override
  public String authType() {
    return AZURE_MSI_AUTH_TYPE;
  }

  /**
   * Configures and returns a HeaderFactory that produces authentication headers for Databricks API
   * requests.
   *
   * <p>The returned HeaderFactory will provide:
   *
   * <ul>
   *   <li>An Authorization header with the Azure MSI access token
   *   <li>If resourceId is available, an X-Databricks-Azure-Workspace-Resource-Id header
   *   <li>If resourceId is not available, an X-Databricks-Azure-SP-Management-Token header with a
   *       management endpoint token
   * </ul>
   *
   * @param databricksConfig The Databricks configuration object.
   * @return A HeaderFactory that produces the required authentication headers.
   */
  @Override
  public HeaderFactory configure(DatabricksConfig databricksConfig) {
    AzureMSICredentials azureMSICredentials =
        new AzureMSICredentials(httpClient, clientId, databricksTokenCache, managementTokenCache);

    return () -> {
      Map<String, String> headers = new HashMap<>();
      if (resourceId != null) {
        LOGGER.warn(
            "In case of Azure MSI configuration, azure_workspace_resource_id parameter should not be null.");
        headers.put("X-Databricks-Azure-Workspace-Resource-Id", resourceId);
        headers.put(
            "X-Databricks-Azure-SP-Management-Token",
            azureMSICredentials.getManagementEndpointToken().getAccessToken());
      }
      headers.put(
          HttpHeaders.AUTHORIZATION, "Bearer " + azureMSICredentials.getToken().getAccessToken());

      return headers;
    };
  }
}
