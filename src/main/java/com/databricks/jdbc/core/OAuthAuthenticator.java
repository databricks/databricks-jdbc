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

  public WorkspaceClient getWorkspaceClient() {
    if (this.connectionContext.getAuthMech().equals(IDatabricksConnectionContext.AuthMech.PAT)) {
      return authenticateAccessToken();
    }
    // TODO(Madhav): Revisit these to set JDBC values
    else if (this.connectionContext
        .getAuthMech()
        .equals(IDatabricksConnectionContext.AuthMech.OAUTH)) {
      switch (this.connectionContext.getAuthFlow()) {
        case TOKEN_PASSTHROUGH:
          return authenticateAccessToken();
        case CLIENT_CREDENTIALS:
          return authenticateM2M();
        case BROWSER_BASED_AUTHENTICATION:
          return authenticateU2M();
      }
    }
    return authenticateAccessToken();
  }

  public WorkspaceClient authenticateU2M() {
    DatabricksConfig config =
        new DatabricksConfig()
            .setAuthType(DatabricksJdbcConstants.U2M_AUTH_TYPE)
            .setHost(this.connectionContext.getHostForOAuth())
            .setClientId(this.connectionContext.getClientId())
            .setClientSecret(this.connectionContext.getClientSecret())
            .setOAuthRedirectUrl(DatabricksJdbcConstants.U2M_AUTH_REDIRECT_URL);
    if (!config.isAzure()) {
      // Default scope is already being set for Azure in databricks-sdk.
      config.setScopes(this.connectionContext.getOAuthScopesForU2M());
    }
    return new WorkspaceClient(config);
  }

  public WorkspaceClient authenticateAccessToken() {
    DatabricksConfig config =
        new DatabricksConfig()
            .setAuthType(DatabricksJdbcConstants.ACCESS_TOKEN_AUTH_TYPE)
            .setHost(this.connectionContext.getHostUrl())
            .setToken(this.connectionContext.getToken());
    return new WorkspaceClient(config);
  }

  public WorkspaceClient authenticateM2M() {
    DatabricksConfig config =
        new DatabricksConfig()
            .setAuthType(DatabricksJdbcConstants.M2M_AUTH_TYPE)
            .setHost(this.connectionContext.getHostForOAuth())
            .setClientId(this.connectionContext.getClientId())
            .setClientSecret(this.connectionContext.getClientSecret());
    return new WorkspaceClient(config);
  }
}
