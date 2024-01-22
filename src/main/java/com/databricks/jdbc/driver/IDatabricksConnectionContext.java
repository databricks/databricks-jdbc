package com.databricks.jdbc.driver;

import com.databricks.jdbc.core.types.CompressionType;
import java.util.List;

public interface IDatabricksConnectionContext {

  enum AuthFlow {
    TOKEN_PASSTHROUGH,
    CLIENT_CREDENTIALS,
    BROWSER_BASED_AUTHENTICATION
  }

  enum AuthMech {
    OTHER,
    PAT,
    OAUTH;

    public static AuthMech parseAuthMech(String authMech) {
      int authMechValue = Integer.parseInt(authMech);
      switch (authMechValue) {
        case 3:
          return AuthMech.PAT;
        case 11:
          return AuthMech.OAUTH;
        default:
          throw new UnsupportedOperationException();
      }
    }
  }

  /**
   * Returns host-Url for Databricks server as parsed from JDBC connection in format
   * https://server:port
   *
   * @return Databricks host-Url
   */
  String getHostUrl();

  /**
   * Returns warehouse-Id as parsed from JDBC connection Url
   *
   * @return warehouse-Id
   */
  String getWarehouse();

  /**
   * Returns the auth token (personal access token/OAuth token etc)
   *
   * @return auth token
   */
  String getToken();

  String getHostForOAuth();

  String getClientId();

  String getClientSecret();

  List<String> getOAuthScopesForU2M();

  AuthMech getAuthMech();

  AuthFlow getAuthFlow();

  String getLogLevelString();

  String getLogPathString();

  String getClientUserAgent();

  CompressionType getCompressionType();
}
