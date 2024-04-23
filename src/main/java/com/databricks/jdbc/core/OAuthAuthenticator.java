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

  public WorkspaceClient getWorkspaceClient(DatabricksConfig databricksConfig)
      throws DatabricksParsingException {
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

  public WorkspaceClient authenticateU2M(DatabricksConfig databricksConfig)
      throws DatabricksParsingException {
    databricksConfig
        .setAuthType(DatabricksJdbcConstants.U2M_AUTH_TYPE)
        .setHost(this.connectionContext.getHostForOAuth())
        .setClientId(this.connectionContext.getClientId())
        .setClientSecret(this.connectionContext.getClientSecret())
        .setOAuthRedirectUrl(DatabricksJdbcConstants.U2M_AUTH_REDIRECT_URL);
    if (!databricksConfig.isAzure()) {
      // Default scope is already being set for Azure in databricks-sdk.
      databricksConfig.setScopes(this.connectionContext.getOAuthScopesForU2M());
    }
    return new WorkspaceClient(databricksConfig);
  }

  public WorkspaceClient authenticateAccessToken(DatabricksConfig databricksConfig)
      throws DatabricksParsingException {
    databricksConfig
        .setAuthType(DatabricksJdbcConstants.ACCESS_TOKEN_AUTH_TYPE)
        .setHost(this.connectionContext.getHostUrl())
        .setToken(this.connectionContext.getToken());
    return new WorkspaceClient(databricksConfig);
  }

  public WorkspaceClient authenticateM2M(DatabricksConfig databricksConfig)
      throws DatabricksParsingException {
    databricksConfig
        .setAuthType(DatabricksJdbcConstants.M2M_AUTH_TYPE)
        .setHost(this.connectionContext.getHostForOAuth())
        .setClientId(this.connectionContext.getClientId())
        .setClientSecret(this.connectionContext.getClientSecret());
    return new WorkspaceClient(databricksConfig);
  }
}
