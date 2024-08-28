package com.databricks.jdbc.auth;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;

public class OAuthAuthenticator {

  private final IDatabricksConnectionContext connectionContext;

  public OAuthAuthenticator(IDatabricksConnectionContext connectionContext) {
    this.connectionContext = connectionContext;
  }

  public WorkspaceClient getWorkspaceClient(DatabricksConfig databricksConfig)
      throws DatabricksParsingException {
    setupDatabricksConfig(databricksConfig);
    return new WorkspaceClient(databricksConfig);
  }

  public void setupDatabricksConfig(DatabricksConfig databricksConfig)
      throws DatabricksParsingException {
    if (this.connectionContext.getAuthMech().equals(IDatabricksConnectionContext.AuthMech.PAT)) {
      setupAccessTokenConfig(databricksConfig);
    }
    // TODO(Madhav): Revisit these to set JDBC values
    else if (this.connectionContext
        .getAuthMech()
        .equals(IDatabricksConnectionContext.AuthMech.OAUTH)) {
      switch (this.connectionContext.getAuthFlow()) {
        case TOKEN_PASSTHROUGH:
          setupAccessTokenConfig(databricksConfig);
          break;
        case CLIENT_CREDENTIALS:
          setupM2MConfig(databricksConfig);
          break;
        case BROWSER_BASED_AUTHENTICATION:
          setupU2MConfig(databricksConfig);
          break;
      }
    } else {
      setupAccessTokenConfig(databricksConfig);
    }
  }

  public void setupU2MConfig(DatabricksConfig databricksConfig) throws DatabricksParsingException {
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

  public void setupAccessTokenConfig(DatabricksConfig databricksConfig)
      throws DatabricksParsingException {
    databricksConfig
        .setAuthType(DatabricksJdbcConstants.ACCESS_TOKEN_AUTH_TYPE)
        .setHost(connectionContext.getHostUrl())
        .setToken(connectionContext.getToken());
  }

  public void setupM2MConfig(DatabricksConfig databricksConfig) throws DatabricksParsingException {
    databricksConfig
        .setAuthType(DatabricksJdbcConstants.M2M_AUTH_TYPE)
        .setHost(connectionContext.getHostForOAuth())
        .setClientId(connectionContext.getClientId())
        .setClientSecret(connectionContext.getClientSecret());
  }
}
