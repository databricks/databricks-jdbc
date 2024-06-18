package com.databricks.jdbc.core;

import com.databricks.jdbc.driver.DatabricksJdbcConstants;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;

public class OAuthAuthenticator {

  private final IDatabricksConnectionContext connectionContext;

  public OAuthAuthenticator(IDatabricksConnectionContext connectionContext) {
    this.connectionContext = connectionContext;
  }

  public WorkspaceClient getWorkspaceClient(DatabricksConfig databricksConfig) throws DatabricksParsingException {
    if (this.connectionContext.getAuthMech().equals(IDatabricksConnectionContext.AuthMech.PAT)) {
      return authenticateAccessToken(databricksConfig);
    }
    // TODO(Madhav): Revisit these to set JDBC values
    else if (this.connectionContext
        .getAuthMech()
        .equals(IDatabricksConnectionContext.AuthMech.OAUTH)) {
      switch (this.connectionContext.getAuthFlow()) {
        case TOKEN_PASSTHROUGH:
          return authenticateAccessToken(databricksConfig);
        case CLIENT_CREDENTIALS:
          return authenticateM2M(databricksConfig);
        case BROWSER_BASED_AUTHENTICATION:
          return authenticateU2M(databricksConfig);
      }
    }
    return authenticateAccessToken(databricksConfig);
  }

  public WorkspaceClient authenticateU2M(DatabricksConfig config) throws DatabricksParsingException {
    config
            .setAuthType(DatabricksJdbcConstants.U2M_AUTH_TYPE)
            .setHost(connectionContext.getHostForOAuth())
            .setClientId(connectionContext.getClientId())
            .setClientSecret(connectionContext.getClientSecret())
            .setOAuthRedirectUrl(DatabricksJdbcConstants.U2M_AUTH_REDIRECT_URL);
    if (!config.isAzure()) {
      config.setScopes(connectionContext.getOAuthScopesForU2M());
    }
    return new WorkspaceClient(config);
  }

  public WorkspaceClient authenticateAccessToken(DatabricksConfig config) throws DatabricksParsingException {
    config
        .setAuthType(DatabricksJdbcConstants.ACCESS_TOKEN_AUTH_TYPE)
        .setHost(connectionContext.getHostUrl())
        .setToken(connectionContext.getToken());
    return new WorkspaceClient(config);
  }

  public WorkspaceClient authenticateM2M(DatabricksConfig config) throws DatabricksParsingException {
    config
        .setAuthType(DatabricksJdbcConstants.M2M_AUTH_TYPE)
        .setHost(connectionContext.getHostForOAuth())
        .setClientId(connectionContext.getClientId())
        .setClientSecret(connectionContext.getClientSecret());
    return new WorkspaceClient(config);
  }
}
