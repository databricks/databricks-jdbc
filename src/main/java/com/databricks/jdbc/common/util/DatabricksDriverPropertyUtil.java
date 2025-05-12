package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.common.AuthFlow.BROWSER_BASED_AUTHENTICATION;
import static com.databricks.jdbc.common.AuthFlow.CLIENT_CREDENTIALS;
import static com.databricks.jdbc.common.AuthFlow.TOKEN_PASSTHROUGH;
import static com.databricks.jdbc.common.AuthMech.OAUTH;
import static com.databricks.jdbc.common.AuthMech.PAT;
import static com.databricks.jdbc.common.DatabricksJdbcUrlParams.*;

import com.databricks.jdbc.api.impl.DatabricksConnectionContext;
import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.common.AuthFlow;
import com.databricks.jdbc.common.AuthMech;
import com.databricks.jdbc.common.DatabricksJdbcUrlParams;
import com.databricks.jdbc.common.LogLevel;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.sdk.core.utils.Cloud;
import java.sql.DriverPropertyInfo;
import java.util.*;

/** Utility class for Databricks driver properties. */
public class DatabricksDriverPropertyUtil {

  private static final List<DatabricksJdbcUrlParams> OPTIONAL_PROPERTIES =
      Arrays.asList(
          DatabricksJdbcUrlParams.SSL,
          DatabricksJdbcUrlParams.LOG_LEVEL,
          DatabricksJdbcUrlParams.USE_PROXY,
          DatabricksJdbcUrlParams.USE_THRIFT_CLIENT,
          DatabricksJdbcUrlParams.ENABLE_ARROW,
          DatabricksJdbcUrlParams.DIRECT_RESULT,
          DatabricksJdbcUrlParams.COMPRESSION_FLAG,
          DatabricksJdbcUrlParams.LZ4_COMPRESSION_FLAG,
          DatabricksJdbcUrlParams.USER_AGENT_ENTRY);

  public static List<DriverPropertyInfo> getMissingProperties(String url, Properties info)
      throws DatabricksParsingException {
    if (url == null) {
      url = "";
    }
    DatabricksConnectionContext connectionContext =
        (DatabricksConnectionContext)
            DatabricksConnectionContextFactory.createWithoutError(url, info);
    // check if null
    if (connectionContext == null) {
      // return host
      DriverPropertyInfo hostProperty = new DriverPropertyInfo("host", null);
      hostProperty.required = true;
      hostProperty.description =
          "JDBC URL must be in the form: <protocol>://<host or domain>:<port>/<path>";
      return Collections.singletonList(hostProperty);
    }
    // check if url contains HTTP_PATH
    if (!connectionContext.isPropertyPresent(HTTP_PATH)) {
      return Collections.singletonList(getUrlParamInfo(HTTP_PATH, true));
    }
    // check if url contains AUTH_MECH
    if (!connectionContext.isPropertyPresent(AUTH_MECH)) {
      return Collections.singletonList(getUrlParamInfo(AUTH_MECH, true));
    }

    return buildMissingPropertiesList(connectionContext);
  }

  public static List<DriverPropertyInfo> buildMissingPropertiesList(
      DatabricksConnectionContext connectionContext) throws DatabricksParsingException {
    List<DriverPropertyInfo> missingPropertyInfos = new ArrayList<>();
    // add required properties
    for (DatabricksJdbcUrlParams param : DatabricksJdbcUrlParams.values()) {
      if (param.isRequired()) {
        addMissingProperty(missingPropertyInfos, connectionContext, param, true);
      }
    }

    // add optional but important properties
    for (DatabricksJdbcUrlParams param : OPTIONAL_PROPERTIES) {
      addMissingProperty(missingPropertyInfos, connectionContext, param, false);
    }

    // log-level properties
    if (connectionContext.isPropertyPresent(LOG_LEVEL)
        && connectionContext.getLogLevel() != LogLevel.OFF) {
      addMissingProperty(missingPropertyInfos, connectionContext, LOG_PATH, false);
      addMissingProperty(missingPropertyInfos, connectionContext, LOG_FILE_SIZE, false);
      addMissingProperty(missingPropertyInfos, connectionContext, LOG_FILE_COUNT, false);
    }

    // auth-related properties
    AuthMech authMech = connectionContext.getAuthMech();
    if (authMech == PAT) {
      addMissingProperty(missingPropertyInfos, connectionContext, PWD, true);
    } else if (authMech == OAUTH) {
      AuthFlow authFlow = connectionContext.getAuthFlow();
      addMissingProperty(
          missingPropertyInfos, connectionContext, IDENTITY_FEDERATION_CLIENT_ID, false);

      if (connectionContext.isPropertyPresent(AUTH_FLOW)) {
        switch (authFlow) {
          case TOKEN_PASSTHROUGH:
            if (connectionContext.getOAuthRefreshToken() != null) {
              addMissingProperty(missingPropertyInfos, connectionContext, CLIENT_ID, true);
              addMissingProperty(missingPropertyInfos, connectionContext, CLIENT_SECRET, true);
              handleTokenEndpointAndDiscoveryMode(missingPropertyInfos, connectionContext);
            } else {
              addMissingProperty(
                  missingPropertyInfos, connectionContext, OAUTH_REFRESH_TOKEN, false);
              addMissingProperty(missingPropertyInfos, connectionContext, AUTH_ACCESS_TOKEN, true);
            }
            break;
          case CLIENT_CREDENTIALS:
            if (connectionContext.getCloud() == Cloud.GCP) {
              addMissingProperty(
                  missingPropertyInfos, connectionContext, GOOGLE_SERVICE_ACCOUNT, true);
              addMissingProperty(
                  missingPropertyInfos, connectionContext, GOOGLE_CREDENTIALS_FILE, true);
            }
            addMissingProperty(missingPropertyInfos, connectionContext, CLIENT_SECRET, true);
            addMissingProperty(missingPropertyInfos, connectionContext, CLIENT_ID, true);

            if (connectionContext.isPropertyPresent(USE_JWT_ASSERTION)) {
              if (connectionContext.useJWTAssertion()) {
                addMissingProperty(missingPropertyInfos, connectionContext, JWT_KEY_FILE, true);
                addMissingProperty(missingPropertyInfos, connectionContext, JWT_ALGORITHM, true);
                addMissingProperty(missingPropertyInfos, connectionContext, JWT_PASS_PHRASE, true);
                addMissingProperty(missingPropertyInfos, connectionContext, JWT_KID, true);
                handleTokenEndpointAndDiscoveryMode(missingPropertyInfos, connectionContext);
              }
            } else {
              addMissingProperty(missingPropertyInfos, connectionContext, USE_JWT_ASSERTION, false);
            }
            break;

          case BROWSER_BASED_AUTHENTICATION:
            addMissingProperty(missingPropertyInfos, connectionContext, CLIENT_ID, false);
            addMissingProperty(missingPropertyInfos, connectionContext, CLIENT_SECRET, false);
            addMissingProperty(missingPropertyInfos, connectionContext, AUTH_SCOPE, false);
            break;

          case AZURE_MANAGED_IDENTITIES:
            addMissingProperty(
                missingPropertyInfos, connectionContext, AZURE_WORKSPACE_RESOURCE_ID, true);
        }
      } else {
        missingPropertyInfos.add(getUrlParamInfo(AUTH_FLOW, true));
      }
    }

    // proxy-related properties
    if (connectionContext.isPropertyPresent(USE_PROXY) && connectionContext.getUseProxy()) {
      addMissingProperty(missingPropertyInfos, connectionContext, PROXY_HOST, true);
      addMissingProperty(missingPropertyInfos, connectionContext, PROXY_PORT, true);
      addMissingProperty(missingPropertyInfos, connectionContext, PROXY_USER, false);
      addMissingProperty(missingPropertyInfos, connectionContext, PROXY_PWD, false);
    }

    return missingPropertyInfos;
  }

  private static void handleTokenEndpointAndDiscoveryMode(
      List<DriverPropertyInfo> missingPropertyInfos,
      DatabricksConnectionContext connectionContext) {
    if (connectionContext.getTokenEndpoint() == null
        && connectionContext.isOAuthDiscoveryModeEnabled()
        && connectionContext.getOAuthDiscoveryURL() == null) {
      addMissingProperty(missingPropertyInfos, connectionContext, DISCOVERY_URL, true);
    } else {
      addMissingProperty(missingPropertyInfos, connectionContext, TOKEN_ENDPOINT, false);
      addMissingProperty(missingPropertyInfos, connectionContext, DISCOVERY_MODE, false);
      addMissingProperty(missingPropertyInfos, connectionContext, DISCOVERY_URL, false);
    }
  }

  private static void addMissingProperty(
      List<DriverPropertyInfo> missingPropertyInfos,
      DatabricksConnectionContext connectionContext,
      DatabricksJdbcUrlParams param,
      boolean required) {
    if (!connectionContext.isPropertyPresent(param)) {
      missingPropertyInfos.add(getUrlParamInfo(param, required));
    }
  }
}
