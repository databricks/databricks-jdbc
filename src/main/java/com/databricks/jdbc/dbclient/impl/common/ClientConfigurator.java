package com.databricks.jdbc.dbclient.impl.common;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.*;
import static com.databricks.jdbc.common.util.DatabricksAuthUtil.initializeConfigWithToken;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.auth.AzureMSICredentialProvider;
import com.databricks.jdbc.auth.OAuthRefreshCredentialsProvider;
import com.databricks.jdbc.auth.PrivateKeyClientCredentialProvider;
import com.databricks.jdbc.common.AuthMech;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.common.util.DriverUtil;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.CredentialsProvider;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.ProxyConfig;
import com.databricks.sdk.core.commons.CommonsHttpClient;
import com.databricks.sdk.core.utils.Cloud;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

/**
 * This class is responsible for configuring the Databricks config based on the connection context.
 * The databricks config is then used to create the SDK or Thrift client.
 */
public class ClientConfigurator {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ClientConfigurator.class);
  private final IDatabricksConnectionContext connectionContext;
  private DatabricksConfig databricksConfig;

  public ClientConfigurator(IDatabricksConnectionContext connectionContext) {
    this.connectionContext = connectionContext;
    this.databricksConfig = new DatabricksConfig();
    CommonsHttpClient.Builder httpClientBuilder = new CommonsHttpClient.Builder();
    httpClientBuilder.withTimeoutSeconds(connectionContext.getSocketTimeout());
    setupProxyConfig(httpClientBuilder);
    setupConnectionManager(httpClientBuilder);
    this.databricksConfig.setHttpClient(httpClientBuilder.build());
    setupDiscoveryEndpoint();
    setupAuthConfig();
    this.databricksConfig.resolve();
  }

  /**
   * Setup the SSL configuration in the httpClientBuilder.
   *
   * @param httpClientBuilder The builder to which the SSL configuration should be added.
   */
  void setupConnectionManager(CommonsHttpClient.Builder httpClientBuilder) {
    PoolingHttpClientConnectionManager connManager =
        ConfiguratorUtils.getBaseConnectionManager(connectionContext);
    // Default value is 100 which is consistent with the value in the SDK
    connManager.setMaxTotal(connectionContext.getHttpConnectionPoolSize());
    httpClientBuilder.withConnectionManager(connManager);
  }

  /** Setup proxy settings in the databricks config. */
  public void setupProxyConfig(CommonsHttpClient.Builder httpClientBuilder) {
    ProxyConfig proxyConfig =
        new ProxyConfig().setUseSystemProperties(connectionContext.getUseSystemProxy());
    if (connectionContext.getUseProxy()) {
      proxyConfig
          .setHost(connectionContext.getProxyHost())
          .setPort(connectionContext.getProxyPort());
    }
    if (connectionContext.getUseProxy() || connectionContext.getUseSystemProxy()) {
      proxyConfig
          .setUsername(connectionContext.getProxyUser())
          .setPassword(connectionContext.getProxyPassword())
          .setProxyAuthType(connectionContext.getProxyAuthType())
          .setNonProxyHosts(
              convertNonProxyHostConfigToBeSystemPropertyCompliant(
                  connectionContext.getNonProxyHosts()));
    }
    httpClientBuilder.withProxyConfig(proxyConfig);
  }

  public WorkspaceClient getWorkspaceClient() {
    return new WorkspaceClient(databricksConfig);
  }

  /** Setup the workspace authentication settings in the databricks config. */
  public void setupAuthConfig() {
    AuthMech authMech = connectionContext.getAuthMech();
    try {
      switch (authMech) {
        case OAUTH:
          setupOAuthConfig();
          break;
        case PAT:
        default:
          setupAccessTokenConfig();
      }
    } catch (DatabricksParsingException e) {
      String errorMessage = "Error while parsing auth config";
      LOGGER.error(errorMessage);
      throw new DatabricksException(errorMessage, e);
    }
  }

  /** Setup the OAuth authentication settings in the databricks config. */
  public void setupOAuthConfig() throws DatabricksParsingException {
    switch (this.connectionContext.getAuthFlow()) {
      case TOKEN_PASSTHROUGH:
        if (connectionContext.getOAuthRefreshToken() != null) {
          setupU2MRefreshConfig();
        } else {
          setupOAuthAccessTokenConfig();
        }
        break;
      case CLIENT_CREDENTIALS:
        setupM2MConfig();
        break;
      case BROWSER_BASED_AUTHENTICATION:
        setupU2MConfig();
        break;
      case AZURE_MANAGED_IDENTITIES:
        setupAzureMI();
        break;
    }
  }

  /** Setup the OAuth U2M authentication settings in the databricks config. */
  public void setupU2MConfig() throws DatabricksParsingException {
    int redirectPort = findAvailablePort(connectionContext.getOAuth2RedirectUrlPorts());
    String redirectUrl = String.format("http://localhost:%d", redirectPort);

    databricksConfig
        .setAuthType(DatabricksJdbcConstants.U2M_AUTH_TYPE)
        .setHost(connectionContext.getHostForOAuth())
        .setClientId(connectionContext.getClientId())
        .setClientSecret(connectionContext.getClientSecret())
        .setOAuthRedirectUrl(redirectUrl);

    LOGGER.info("Using OAuth redirect URL: %s", redirectUrl);

    if (!databricksConfig.isAzure()) {
      databricksConfig.setScopes(connectionContext.getOAuthScopesForU2M());
    }
  }

  /**
   * Finds the first available port from the provided list of ports. If a single port is provided,
   * it tries incremental ports (port, port+1, port+2, etc.) If multiple ports are provided, it
   * tries each port in the list.
   *
   * @param initialPorts List of ports to try
   * @return The first available port
   * @throws DatabricksException if no available port is found
   */
  int findAvailablePort(List<Integer> initialPorts) {
    List<Integer> portsToTry;

    // If single port provided, generate sequence of ports to try
    if (initialPorts.size() == 1) {
      int startPort = initialPorts.get(0);
      int maxAttempts = 20;
      portsToTry = new ArrayList<>(maxAttempts);
      for (int i = 0; i < maxAttempts; i++) {
        portsToTry.add(startPort + i);
      }
      LOGGER.debug(
          "Single port provided (%s), will try ports %s through %s",
          startPort, startPort, startPort + maxAttempts - 1);
    } else {
      portsToTry = initialPorts;
      LOGGER.debug("Multiple ports provided, will try: %s", portsToTry);
    }

    // Try each port in the list
    for (int port : portsToTry) {
      if (isPortAvailable(port)) {
        return port;
      }
      LOGGER.debug("Port %s is not available, trying next port", port);
    }

    // No available ports found
    LOGGER.error("No available ports found among: %s", portsToTry);
    throw new DatabricksException(
        "No available port found for OAuth redirect URL. Tried ports: " + portsToTry);
  }

  /**
   * Checks if a port is available by trying to open a server socket on it.
   *
   * @param port Port to check
   * @return true if the port is available, false otherwise
   */
  boolean isPortAvailable(int port) {
    try (ServerSocket serverSocket = new ServerSocket(port)) {
      serverSocket.setReuseAddress(true);
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  /** Setup the PAT authentication settings in the databricks config. */
  public void setupAccessTokenConfig() throws DatabricksParsingException {
    databricksConfig
        .setAuthType(DatabricksJdbcConstants.ACCESS_TOKEN_AUTH_TYPE)
        .setHost(connectionContext.getHostUrl())
        .setToken(connectionContext.getToken());
  }

  public void setupOAuthAccessTokenConfig() throws DatabricksParsingException {
    databricksConfig
        .setAuthType(DatabricksJdbcConstants.ACCESS_TOKEN_AUTH_TYPE)
        .setHost(connectionContext.getHostUrl())
        .setToken(connectionContext.getPassThroughAccessToken());
  }

  public void resetAccessTokenInConfig(String newAccessToken) {
    this.databricksConfig = initializeConfigWithToken(newAccessToken, databricksConfig);
    this.databricksConfig.resolve();
  }

  /** Setup the OAuth U2M refresh token authentication settings in the databricks config. */
  public void setupU2MRefreshConfig() throws DatabricksParsingException {
    CredentialsProvider provider =
        new OAuthRefreshCredentialsProvider(connectionContext, databricksConfig);
    databricksConfig
        .setHost(connectionContext.getHostForOAuth())
        .setAuthType(provider.authType()) // oauth-refresh
        .setCredentialsProvider(provider)
        .setClientId(connectionContext.getClientId())
        .setClientSecret(connectionContext.getClientSecret());
  }

  /** Setup the OAuth M2M authentication settings in the databricks config. */
  public void setupM2MConfig() throws DatabricksParsingException {
    if (DriverUtil.isRunningAgainstFake()) {
      databricksConfig.setHost(
          connectionContext.getHostUrl()); // add port when running fake service test
    } else {
      databricksConfig.setHost(connectionContext.getHostForOAuth());
    }
    if (connectionContext.getCloud() == Cloud.GCP
        && !connectionContext.getGcpAuthType().equals(M2M_AUTH_TYPE)) {
      String authType = connectionContext.getGcpAuthType();
      databricksConfig.setAuthType(authType);
      if (authType.equals(GCP_GOOGLE_CREDENTIALS_AUTH_TYPE)) {
        databricksConfig.setGoogleCredentials(connectionContext.getGoogleCredentials());
      } else {
        databricksConfig.setGoogleServiceAccount(connectionContext.getGoogleServiceAccount());
      }
    } else if (connectionContext.getAzureTenantId() != null) {
      // If azure tenant id is specified, use Azure Active Directory (AAD) Service Principal OAuth
      LOGGER.debug("Using Azure Active Directory (AAD) Service Principal OAuth");
      if (connectionContext.getCloud() != Cloud.AZURE) {
        throw new DatabricksParsingException(
            "Azure client credentials flow is only supported for Azure cloud",
            DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
      }
      databricksConfig
          .setAuthType(M2M_AZURE_CLIENT_SECRET_AUTH_TYPE)
          .setAzureClientId(connectionContext.getClientId())
          .setAzureClientSecret(connectionContext.getClientSecret())
          .setAzureTenantId(connectionContext.getAzureTenantId());
    } else {
      databricksConfig
          .setAuthType(DatabricksJdbcConstants.M2M_AUTH_TYPE)
          .setClientId(connectionContext.getClientId())
          .setClientSecret(connectionContext.getClientSecret());
      if (connectionContext.useJWTAssertion()) {
        databricksConfig.setCredentialsProvider(
            new PrivateKeyClientCredentialProvider(connectionContext, databricksConfig));
      }
    }
  }

  private void setupAzureMI() {
    databricksConfig.setHost(connectionContext.getHostForOAuth());
    databricksConfig.setAuthType(DatabricksJdbcConstants.AZURE_MSI_AUTH_TYPE);
    databricksConfig.setCredentialsProvider(new AzureMSICredentialProvider(connectionContext));
  }

  /**
   * Currently, the ODBC driver takes in nonProxyHosts as a comma separated list of suffix of
   * non-proxy hosts i.e. suffix1|suffix2|suffix3. Whereas, the SDK takes in nonProxyHosts as a list
   * of patterns separated by '|'. This pattern conforms to the system property format in the Java
   * Proxy Guide.
   *
   * @param nonProxyHosts Comma separated list of suffix of non-proxy hosts
   * @return nonProxyHosts in system property compliant format from <a
   *     href="https://docs.oracle.com/javase/8/docs/technotes/guides/net/proxies.html">Java Proxy
   *     Guide</a>
   */
  public static String convertNonProxyHostConfigToBeSystemPropertyCompliant(String nonProxyHosts) {
    if (nonProxyHosts == null || nonProxyHosts.isEmpty()) {
      return EMPTY_STRING;
    }
    if (nonProxyHosts.contains(DatabricksJdbcConstants.PIPE)) {
      // Already in system property compliant format
      return nonProxyHosts;
    }
    return Arrays.stream(nonProxyHosts.split(DatabricksJdbcConstants.COMMA))
        .map(
            suffix -> {
              if (suffix.startsWith(DatabricksJdbcConstants.FULL_STOP)) {
                return DatabricksJdbcConstants.ASTERISK + suffix;
              }
              return suffix;
            })
        .collect(Collectors.joining(DatabricksJdbcConstants.PIPE));
  }

  public DatabricksConfig getDatabricksConfig() {
    return this.databricksConfig;
  }

  private void setupDiscoveryEndpoint() {
    if (connectionContext.isOAuthDiscoveryModeEnabled()) {
      databricksConfig.setDiscoveryUrl(connectionContext.getOAuthDiscoveryURL());
    }
  }
}
