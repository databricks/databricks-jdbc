package com.databricks.jdbc.dbclient.impl.common;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.auth.OAuthRefreshCredentialsProvider;
import com.databricks.jdbc.auth.PrivateKeyClientCredentialProvider;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.CredentialsProvider;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksException;

/**
 * This class is responsible for configuring the Databricks config based on the connection context.
 * The databricks config is then used to create the SDK or Thrift client.
 */
public class ClientConfigurator {

  public static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ClientConfigurator.class);
  private final IDatabricksConnectionContext connectionContext;
  private final DatabricksConfig databricksConfig;

  public ClientConfigurator(IDatabricksConnectionContext connectionContext) {
    this.connectionContext = connectionContext;
    this.databricksConfig = new DatabricksConfig();
    setupProxyConfig();
    setupAuthConfig();
    this.databricksConfig.resolve();
  }

  /** Setup proxy settings in the databricks config. */
  public void setupProxyConfig() {
    this.databricksConfig.setUseSystemPropertiesHttp(connectionContext.getUseSystemProxy());
    // Setup proxy settings
    if (connectionContext.getUseProxy()) {
      databricksConfig
          .setProxyHost(connectionContext.getProxyHost())
          .setProxyPort(connectionContext.getProxyPort());
    }
    databricksConfig
        .setProxyAuthType(connectionContext.getProxyAuthType())
        .setProxyUsername(connectionContext.getProxyUser())
        .setProxyPassword(connectionContext.getProxyPassword());
  }

  public WorkspaceClient getWorkspaceClient() {
    return new WorkspaceClient(databricksConfig);
  }

  /** Setup the workspace authentication settings in the databricks config. */
  public void setupAuthConfig() {
    IDatabricksConnectionContext.AuthMech authMech = connectionContext.getAuthMech();
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
    // TODO(Madhav): Revisit these to set JDBC values
    switch (this.connectionContext.getAuthFlow()) {
      case TOKEN_PASSTHROUGH:
        if (connectionContext.getOAuthRefreshToken() != null) {
          setupU2MRefreshConfig();
        } else {
          setupAccessTokenConfig();
        }
        break;
      case CLIENT_CREDENTIALS:
        setupM2MConfig();
        break;
      case BROWSER_BASED_AUTHENTICATION:
        setupU2MConfig();
        break;
    }
  }

  /** Setup the OAuth U2M authentication settings in the databricks config. */
  public void setupU2MConfig() throws DatabricksParsingException {
    databricksConfig
        .setAuthType(DatabricksJdbcConstants.U2M_AUTH_TYPE)
        .setHost(connectionContext.getHostForOAuth())
        .setClientId(connectionContext.getClientId())
        .setClientSecret(connectionContext.getClientSecret())
        .setOAuthRedirectUrl(DatabricksJdbcConstants.U2M_AUTH_REDIRECT_URL);
    if (!databricksConfig.isAzure()) {
      databricksConfig.setScopes(connectionContext.getOAuthScopesForU2M());
    }
  }

  /** Setup the PAT authentication settings in the databricks config. */
  public void setupAccessTokenConfig() throws DatabricksParsingException {
    databricksConfig
        .setAuthType(DatabricksJdbcConstants.ACCESS_TOKEN_AUTH_TYPE)
        .setHost(connectionContext.getHostUrl())
        .setToken(connectionContext.getToken());
  }

  /** Setup the OAuth U2M refresh token authentication settings in the databricks config. */
  public void setupU2MRefreshConfig() throws DatabricksParsingException {
    CredentialsProvider provider = new OAuthRefreshCredentialsProvider(connectionContext);
    databricksConfig
        .setHost(connectionContext.getHostForOAuth())
        .setAuthType(provider.authType())
        .setCredentialsProvider(provider)
        .setClientId(connectionContext.getClientId())
        .setClientSecret(connectionContext.getClientSecret());
  }

  /** Setup the OAuth M2M authentication settings in the databricks config. */
  public void setupM2MConfig() throws DatabricksParsingException {
    databricksConfig
        .setAuthType(DatabricksJdbcConstants.M2M_AUTH_TYPE)
        .setHost(connectionContext.getHostForOAuth())
        .setClientId(connectionContext.getClientId())
        .setClientSecret(connectionContext.getClientSecret());
    if (connectionContext.useJWTAssertion()) {
      databricksConfig.setCredentialsProvider(
          new PrivateKeyClientCredentialProvider(connectionContext));
    }
  }

  public DatabricksConfig getDatabricksConfig() {
    return this.databricksConfig;
  }
}
