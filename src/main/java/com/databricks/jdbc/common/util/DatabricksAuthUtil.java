package com.databricks.jdbc.common.util;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.sdk.core.CredentialsProvider;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.http.HttpClient;
import com.nimbusds.jwt.SignedJWT;
import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;

public class DatabricksAuthUtil {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DatabricksAuthUtil.class);

  /** Parses the space-separated Auth_Scope value, returning an empty list for blank values. */
  public static List<String> parseOAuthScopes(String authScope) {
    if (authScope == null || authScope.trim().isEmpty()) {
      return com.google.common.collect.ImmutableList.of();
    }
    return Arrays.asList(authScope.trim().split("\\s+"));
  }

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
    CredentialsProvider credentialsProvider = config.getCredentialsProvider();
    DatabricksConfig newConfig = new DatabricksConfig();
    newConfig
        .setHost(hostUrl)
        .setHttpClient(httpClient)
        .setAuthType(DatabricksJdbcConstants.ACCESS_TOKEN_AUTH_TYPE)
        .setToken(newAccessToken);

    // Preserve and reconfigure the credentials provider if it exists
    if (credentialsProvider != null) {
      newConfig.setCredentialsProvider(credentialsProvider);
    }

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
