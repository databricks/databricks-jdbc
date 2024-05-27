package com.databricks.jdbc.core;

import com.databricks.jdbc.driver.DatabricksCommonHttpClient;
import com.databricks.jdbc.driver.DatabricksJdbcConstants;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.jdbc.driver.SSLConfiguration;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import java.security.NoSuchAlgorithmException;
import javax.net.ssl.SSLContext;

public class OAuthAuthenticator {

  private final IDatabricksConnectionContext connectionContext;

  public OAuthAuthenticator(IDatabricksConnectionContext connectionContext) {
    this.connectionContext = connectionContext;
  }

  public WorkspaceClient getWorkspaceClient() throws DatabricksParsingException {
    return new WorkspaceClient(getDatabricksConfig());
  }

  public DatabricksConfig getDatabricksConfig() throws DatabricksParsingException {
    if (this.connectionContext.getAuthMech().equals(IDatabricksConnectionContext.AuthMech.PAT)) {
      return createAccessTokenConfig();
    }
    // TODO(Madhav): Revisit these to set JDBC values
    else if (this.connectionContext
        .getAuthMech()
        .equals(IDatabricksConnectionContext.AuthMech.OAUTH)) {
      switch (this.connectionContext.getAuthFlow()) {
        case TOKEN_PASSTHROUGH:
          return createAccessTokenConfig();
        case CLIENT_CREDENTIALS:
          return createM2MConfig();
        case BROWSER_BASED_AUTHENTICATION:
          return createU2MConfig();
      }
    }
    return createAccessTokenConfig();
  }

  public DatabricksConfig createU2MConfig() throws DatabricksParsingException {
    DatabricksConfig config =
        new DatabricksConfig()
            .setAuthType(DatabricksJdbcConstants.U2M_AUTH_TYPE)
            .setHost(connectionContext.getHostForOAuth())
            .setClientId(connectionContext.getClientId())
            .setClientSecret(connectionContext.getClientSecret())
            .setOAuthRedirectUrl(DatabricksJdbcConstants.U2M_AUTH_REDIRECT_URL);
    if (!config.isAzure()) {
      config.setScopes(connectionContext.getOAuthScopesForU2M());
    }
    return config;
  }

  public DatabricksConfig createAccessTokenConfig() throws DatabricksParsingException {
    SSLContext sslContext = null;
    try {
      sslContext = SSLContext.getDefault();
    } catch (NoSuchAlgorithmException e) {

    }
    if (connectionContext.isSSLEnabled()) {
      try {
        sslContext =
            SSLConfiguration.configureSslContext(
                this.connectionContext.getSSLKeyStorePath(),
                this.connectionContext.getSSLKeyStorePassword());
      } catch (Exception e) {

      }
    }
    return new DatabricksConfig()
        .setAuthType(DatabricksJdbcConstants.ACCESS_TOKEN_AUTH_TYPE)
        .setHost(connectionContext.getHostUrl())
        .setToken(connectionContext.getToken())
        .setHttpClient(new DatabricksCommonHttpClient(300, sslContext));
  }

  public DatabricksConfig createM2MConfig() throws DatabricksParsingException {
    return new DatabricksConfig()
        .setAuthType(DatabricksJdbcConstants.M2M_AUTH_TYPE)
        .setHost(connectionContext.getHostForOAuth())
        .setClientId(connectionContext.getClientId())
        .setClientSecret(connectionContext.getClientSecret());
  }
}
