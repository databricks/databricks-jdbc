package com.databricks.jdbc.common.util;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.http.HttpClient;
import com.nimbusds.jwt.SignedJWT;
import java.io.IOException;
import java.text.ParseException;

public class DatabricksAuthUtil {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DatabricksAuthUtil.class);

  public static String getTokenEndpoint(
      DatabricksConfig databricksConfig, IDatabricksConnectionContext connectionContext) {
    String userProvidedTokenEndpoint = connectionContext.getTokenEndpoint();
    if (userProvidedTokenEndpoint != null) {
      return userProvidedTokenEndpoint;
    }
    try {
      return databricksConfig.getOidcEndpoints().getTokenEndpoint();
    } catch (IOException e) {
      String errorMessage = "Failed to build default token endpoint URL.";
      LOGGER.error(errorMessage);
      throw new DatabricksException(errorMessage, e);
    }
  }

  public static DatabricksConfig initializeConfigWithToken(
      String newAccessToken, DatabricksConfig config) {
    String hostUrl = config.getHost();
    HttpClient httpClient = config.getHttpClient();
    DatabricksConfig newConfig = new DatabricksConfig();
    newConfig
        .setHost(hostUrl)
        .setHttpClient(httpClient)
        .setAuthType(DatabricksJdbcConstants.ACCESS_TOKEN_AUTH_TYPE)
        .setToken(newAccessToken);
    return newConfig;
  }

  public static Boolean isTokenJWT(String accessToken) {
    try {
      // If token is parsable, it is a JWT
      SignedJWT signedJWT = SignedJWT.parse(accessToken);
      return true;
    } catch (ParseException | NullPointerException e) {
      return false;
    }
  }
}
