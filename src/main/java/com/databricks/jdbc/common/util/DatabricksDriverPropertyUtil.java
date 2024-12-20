package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.common.DatabricksJdbcUrlParams.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.impl.DatabricksConnectionContext;
import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.common.DatabricksJdbcUrlParams;
import com.databricks.jdbc.common.LogLevel;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.sdk.core.utils.Cloud;
import com.google.common.collect.ImmutableMap;
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
                  DatabricksJdbcUrlParams.USER_AGENT_ENTRY
          );




  /**
   * Retrieves the invalid URL property information for the specified required parameter.
   *
   * @param param the Databricks JDBC URL parameter
   * @return an array of DriverPropertyInfo objects describing the invalid property
   */
  public static DriverPropertyInfo[] getInvalidUrlRequiredPropertyInfo(
      DatabricksJdbcUrlParams param) {
    DriverPropertyInfo[] propertyInfos = new DriverPropertyInfo[1];
    propertyInfos[0] = getUrlParamInfo(param, true);
    return propertyInfos;
  }

  /**
   * Builds a map of properties from the given connection parameter string and properties object.
   *
   * @param connectionParamString the connection parameter string
   * @param properties the properties object
   * @return an immutable map of properties
   */
  public static ImmutableMap<String, String> buildPropertiesMap(
          String connectionParamString, Properties properties) {
    ImmutableMap.Builder<String, String> parametersBuilder = ImmutableMap.builder();
    String[] urlParts = connectionParamString.split(DatabricksJdbcConstants.URL_DELIMITER);
    for (int urlPartIndex = 1; urlPartIndex < urlParts.length; urlPartIndex++) {
      String[] pair = urlParts[urlPartIndex].split(DatabricksJdbcConstants.PAIR_DELIMITER);
      if (pair.length == 1) {
        pair = new String[] {pair[0], ""};
      }
      parametersBuilder.put(pair[0].toLowerCase(), pair[1]);
    }
    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      parametersBuilder.put(entry.getKey().toString().toLowerCase(), entry.getValue().toString());
    }
    return parametersBuilder.build();
  }
  public static List<DriverPropertyInfo> getMissingProperties(String url, Properties info) {
      DatabricksConnectionContext connectionContext = (DatabricksConnectionContext) DatabricksConnectionContextFactory.createWithoutError(url, info);
      // check if null
      if (connectionContext == null) {
        // return host
        DriverPropertyInfo hostProperty = new DriverPropertyInfo("host", null);
        hostProperty.required = true;
        hostProperty.description = "JDBC URL must be in the form: <protocol>://<host or domain>:<port>/<path>";
        return Collections.singletonList(hostProperty);
      }
      // check if url contains HTTP_PATH
    if(!url.toLowerCase().contains(HTTP_PATH.getParamName().toLowerCase())) {
      return Collections.singletonList(getUrlParamInfo(HTTP_PATH, true));
    }

    // check if url contains AUTH_MECH
    if (!url.toLowerCase().contains(AUTH_MECH.getParamName().toLowerCase()) || info.containsKey()) {
        return Collections.singletonList(getUrlParamInfo(AUTH_MECH, true));
    }

    return buildMissingPropertiesList(url, connectionContext);
  }

  public static List<DriverPropertyInfo> buildMissingPropertiesList(DatabricksConnectionContext connectionContext) {

  }

  /**
   * Retrieves the list of missing properties required for the connection.
   *
   * @param connectionParamString the connection parameter string
   * @param properties the properties object
   * @return a list of DriverPropertyInfo objects describing the missing properties
   * @throws DatabricksSQLException if a database access error occurs
   */
  public static List<DriverPropertyInfo> getMissingProperties(
      String host, String connectionParamString, Properties properties)
      throws DatabricksSQLException {
    ImmutableMap<String, String> connectionPropertiesMap =
        buildPropertiesMap(connectionParamString, properties);
    DatabricksConnectionContext connectionContext =
        new DatabricksConnectionContext(host, connectionPropertiesMap);

    List<DriverPropertyInfo> missingPropertyInfos = new ArrayList<>();
    // add required properties
    for (DatabricksJdbcUrlParams param : DatabricksJdbcUrlParams.values()) {
      if (param.isRequired()) {
        addMissingProperty(missingPropertyInfos, connectionPropertiesMap, param, true);
      }
    }
    // add optional but important properties
    addMissingProperty(missingPropertyInfos, connectionPropertiesMap, SSL, false);
    addMissingProperty(missingPropertyInfos, connectionPropertiesMap, LOG_LEVEL, false);
    addMissingProperty(missingPropertyInfos, connectionPropertiesMap, USE_PROXY, false);
    addMissingProperty(missingPropertyInfos, connectionPropertiesMap, USE_THRIFT_CLIENT, false);
    addMissingProperty(missingPropertyInfos, connectionPropertiesMap, ENABLE_ARROW, false);
    addMissingProperty(missingPropertyInfos, connectionPropertiesMap, DIRECT_RESULT, false);
    addMissingProperty(missingPropertyInfos, connectionPropertiesMap, COMPRESSION_FLAG, false);
    addMissingProperty(missingPropertyInfos, connectionPropertiesMap, LZ4_COMPRESSION_FLAG, false);
    addMissingProperty(missingPropertyInfos, connectionPropertiesMap, USER_AGENT_ENTRY, false);

    // log-level properties
    if (connectionPropertiesMap.containsKey(LOG_LEVEL.getParamName().toLowerCase())
        && connectionContext.getLogLevel() != LogLevel.OFF) {
      addMissingProperty(missingPropertyInfos, connectionPropertiesMap, LOG_PATH, false);
      addMissingProperty(missingPropertyInfos, connectionPropertiesMap, LOG_FILE_SIZE, false);
      addMissingProperty(missingPropertyInfos, connectionPropertiesMap, LOG_FILE_COUNT, false);
    }

    // auth-related properties
    IDatabricksConnectionContext.AuthMech authMech = connectionContext.getAuthMech();
    if (authMech == IDatabricksConnectionContext.AuthMech.PAT) {
      addMissingProperty(missingPropertyInfos, connectionPropertiesMap, PWD, true);
    } else if (authMech == IDatabricksConnectionContext.AuthMech.OAUTH) {
      IDatabricksConnectionContext.AuthFlow authFlow = connectionContext.getAuthFlow();

      if (connectionPropertiesMap.containsKey(AUTH_FLOW.getParamName().toLowerCase())) {
        switch (authFlow) {
          case TOKEN_PASSTHROUGH:
            if (connectionContext.getOAuthRefreshToken() != null) {
              addMissingProperty(missingPropertyInfos, connectionPropertiesMap, CLIENT_ID, true);
              addMissingProperty(
                  missingPropertyInfos, connectionPropertiesMap, CLIENT_SECRET, true);
              handleTokenEndpointAndDiscoveryMode(
                  missingPropertyInfos, connectionPropertiesMap, connectionContext);
            } else {
              addMissingProperty(
                  missingPropertyInfos, connectionPropertiesMap, OAUTH_REFRESH_TOKEN, false);
              addMissingProperty(
                  missingPropertyInfos, connectionPropertiesMap, AUTH_ACCESS_TOKEN, true);
            }
            break;
          case CLIENT_CREDENTIALS:
            if (connectionContext.getCloud() == Cloud.GCP) {
              addMissingProperty(
                  missingPropertyInfos, connectionPropertiesMap, GOOGLE_SERVICE_ACCOUNT, true);
              addMissingProperty(
                  missingPropertyInfos, connectionPropertiesMap, GOOGLE_CREDENTIALS_FILE, true);
            }
            addMissingProperty(missingPropertyInfos, connectionPropertiesMap, CLIENT_SECRET, true);
            addMissingProperty(missingPropertyInfos, connectionPropertiesMap, CLIENT_ID, true);

            if (connectionPropertiesMap.containsKey(
                USE_JWT_ASSERTION.getParamName().toLowerCase())) {
              if (connectionContext.useJWTAssertion()) {
                addMissingProperty(
                    missingPropertyInfos, connectionPropertiesMap, JWT_KEY_FILE, true);
                addMissingProperty(
                    missingPropertyInfos, connectionPropertiesMap, JWT_ALGORITHM, true);
                addMissingProperty(
                    missingPropertyInfos, connectionPropertiesMap, JWT_PASS_PHRASE, true);
                addMissingProperty(missingPropertyInfos, connectionPropertiesMap, JWT_KID, true);
                handleTokenEndpointAndDiscoveryMode(
                    missingPropertyInfos, connectionPropertiesMap, connectionContext);
              }
            } else {
              addMissingProperty(
                  missingPropertyInfos, connectionPropertiesMap, USE_JWT_ASSERTION, false);
            }
            break;

          case BROWSER_BASED_AUTHENTICATION:
            addMissingProperty(missingPropertyInfos, connectionPropertiesMap, CLIENT_ID, false);
            addMissingProperty(missingPropertyInfos, connectionPropertiesMap, CLIENT_SECRET, false);
            addMissingProperty(missingPropertyInfos, connectionPropertiesMap, AUTH_SCOPE, false);
            break;
        }
      } else {
        missingPropertyInfos.add(getUrlParamInfo(AUTH_FLOW, true));
      }
    }

    // proxy-related properties
    if (connectionPropertiesMap.containsKey(USE_PROXY.getParamName().toLowerCase())
        && connectionContext.getUseProxy()) {
      addMissingProperty(missingPropertyInfos, connectionPropertiesMap, PROXY_HOST, true);
      addMissingProperty(missingPropertyInfos, connectionPropertiesMap, PROXY_PORT, true);
      addMissingProperty(missingPropertyInfos, connectionPropertiesMap, PROXY_USER, false);
      addMissingProperty(missingPropertyInfos, connectionPropertiesMap, PROXY_PWD, false);
    }

    return missingPropertyInfos;
  }

  private static void handleTokenEndpointAndDiscoveryMode(
      List<DriverPropertyInfo> missingPropertyInfos,
      Map<String, String> connectionPropertiesMap,
      DatabricksConnectionContext connectionContext) {
    if (connectionContext.getTokenEndpoint() == null
        && connectionContext.isOAuthDiscoveryModeEnabled()
        && connectionContext.getOAuthDiscoveryURL() == null) {
      addMissingProperty(missingPropertyInfos, connectionPropertiesMap, DISCOVERY_URL, true);
    } else {
      addMissingProperty(missingPropertyInfos, connectionPropertiesMap, TOKEN_ENDPOINT, false);
      addMissingProperty(missingPropertyInfos, connectionPropertiesMap, DISCOVERY_MODE, false);
      addMissingProperty(missingPropertyInfos, connectionPropertiesMap, DISCOVERY_URL, false);
    }
  }

  private static void addMissingProperty(
      List<DriverPropertyInfo> missingPropertyInfos,
      Map<String, String> connectionPropertiesMap,
      DatabricksJdbcUrlParams param,
      boolean required) {
    if (!connectionPropertiesMap.containsKey(param.getParamName().toLowerCase())) {
      missingPropertyInfos.add(getUrlParamInfo(param, required));
    }
  }
}
