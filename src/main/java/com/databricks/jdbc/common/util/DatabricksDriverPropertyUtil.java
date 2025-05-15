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
          DatabricksJdbcUrlParams.LOG_LEVEL,
          DatabricksJdbcUrlParams.LOG_PATH,
          DatabricksJdbcUrlParams.LOG_FILE_SIZE,
          DatabricksJdbcUrlParams.LOG_FILE_COUNT,
          DatabricksJdbcUrlParams.USER,
          DatabricksJdbcUrlParams.PASSWORD,
          DatabricksJdbcUrlParams.CLIENT_ID,
          DatabricksJdbcUrlParams.CLIENT_SECRET,
          DatabricksJdbcUrlParams.AUTH_MECH,
          DatabricksJdbcUrlParams.AUTH_ACCESS_TOKEN,
          DatabricksJdbcUrlParams.CONN_CATALOG,
          DatabricksJdbcUrlParams.CONN_SCHEMA,
          DatabricksJdbcUrlParams.PROXY_HOST,
          DatabricksJdbcUrlParams.PROXY_PORT,
          DatabricksJdbcUrlParams.PROXY_USER,
          DatabricksJdbcUrlParams.PROXY_PWD,
          DatabricksJdbcUrlParams.USE_PROXY,
          DatabricksJdbcUrlParams.PROXY_AUTH,
          DatabricksJdbcUrlParams.NON_PROXY_HOSTS,
          DatabricksJdbcUrlParams.USE_SYSTEM_PROXY,
          DatabricksJdbcUrlParams.USE_CF_PROXY,
          DatabricksJdbcUrlParams.CF_PROXY_HOST,
          DatabricksJdbcUrlParams.CF_PROXY_PORT,
          DatabricksJdbcUrlParams.CF_PROXY_AUTH,
          DatabricksJdbcUrlParams.CF_PROXY_USER,
          DatabricksJdbcUrlParams.CF_PROXY_PWD,
          DatabricksJdbcUrlParams.AUTH_FLOW,
          DatabricksJdbcUrlParams.OAUTH_REFRESH_TOKEN,
          DatabricksJdbcUrlParams.OAUTH_REFRESH_TOKEN_2,
          DatabricksJdbcUrlParams.OAUTH_REDIRECT_URL_PORT,
          DatabricksJdbcUrlParams.PWD,
          DatabricksJdbcUrlParams.POLL_INTERVAL,
          DatabricksJdbcUrlParams.HTTP_PATH,
          DatabricksJdbcUrlParams.HTTP_HEADERS,
          DatabricksJdbcUrlParams.SSL,
          DatabricksJdbcUrlParams.USE_THRIFT_CLIENT,
          DatabricksJdbcUrlParams.RATE_LIMIT_RETRY_TIMEOUT,
          DatabricksJdbcUrlParams.JWT_KEY_FILE,
          DatabricksJdbcUrlParams.JWT_ALGORITHM,
          DatabricksJdbcUrlParams.JWT_PASS_PHRASE,
          DatabricksJdbcUrlParams.JWT_KID,
          DatabricksJdbcUrlParams.USE_JWT_ASSERTION,
          DatabricksJdbcUrlParams.OIDC_DISCOVERY_MODE,
          DatabricksJdbcUrlParams.DISCOVERY_MODE,
          DatabricksJdbcUrlParams.AUTH_SCOPE,
          DatabricksJdbcUrlParams.OIDC_DISCOVERY_ENDPOINT,
          DatabricksJdbcUrlParams.DISCOVERY_URL,
          DatabricksJdbcUrlParams.IDENTITY_FEDERATION_CLIENT_ID,
          DatabricksJdbcUrlParams.ENABLE_ARROW,
          DatabricksJdbcUrlParams.DIRECT_RESULT,
          DatabricksJdbcUrlParams.LZ4_COMPRESSION_FLAG,
          DatabricksJdbcUrlParams.COMPRESSION_FLAG,
          DatabricksJdbcUrlParams.USER_AGENT_ENTRY,
          DatabricksJdbcUrlParams.USE_EMPTY_METADATA,
          DatabricksJdbcUrlParams.TEMPORARILY_UNAVAILABLE_RETRY,
          DatabricksJdbcUrlParams.TEMPORARILY_UNAVAILABLE_RETRY_TIMEOUT,
          DatabricksJdbcUrlParams.RATE_LIMIT_RETRY,
          DatabricksJdbcUrlParams.IDLE_HTTP_CONNECTION_EXPIRY,
          DatabricksJdbcUrlParams.SUPPORT_MANY_PARAMETERS,
          DatabricksJdbcUrlParams.CLOUD_FETCH_THREAD_POOL_SIZE,
          DatabricksJdbcUrlParams.OAUTH_ENDPOINT,
          DatabricksJdbcUrlParams.AUTH_ENDPOINT,
          DatabricksJdbcUrlParams.OAUTH_TOKEN_ENDPOINT,
          DatabricksJdbcUrlParams.TOKEN_ENDPOINT,
          DatabricksJdbcUrlParams.SSL_TRUST_STORE,
          DatabricksJdbcUrlParams.SSL_TRUST_STORE_PASSWORD,
          DatabricksJdbcUrlParams.SSL_TRUST_STORE_TYPE,
          DatabricksJdbcUrlParams.CHECK_CERTIFICATE_REVOCATION,
          DatabricksJdbcUrlParams.ACCEPT_UNDETERMINED_CERTIFICATE_REVOCATION,
          DatabricksJdbcUrlParams.GOOGLE_SERVICE_ACCOUNT,
          DatabricksJdbcUrlParams.ENABLE_TELEMETRY,
          DatabricksJdbcUrlParams.TELEMETRY_BATCH_SIZE,
          DatabricksJdbcUrlParams.MAX_BATCH_SIZE,
          DatabricksJdbcUrlParams.ALLOWED_VOLUME_INGESTION_PATHS,
          DatabricksJdbcUrlParams.ALLOWED_STAGING_INGESTION_PATHS,
          DatabricksJdbcUrlParams.UC_INGESTION_RETRIABLE_HTTP_CODE,
          DatabricksJdbcUrlParams.VOLUME_OPERATION_RETRYABLE_HTTP_CODE,
          DatabricksJdbcUrlParams.UC_INGESTION_RETRY_TIMEOUT,
          DatabricksJdbcUrlParams.VOLUME_OPERATION_RETRY_TIMEOUT,
          DatabricksJdbcUrlParams.ENABLE_REQUEST_TRACING,
          DatabricksJdbcUrlParams.HTTP_CONNECTION_POOL_SIZE,
          DatabricksJdbcUrlParams.ENABLE_SQL_EXEC_HYBRID_RESULTS,
          DatabricksJdbcUrlParams.ENABLE_COMPLEX_DATATYPE_SUPPORT,
          DatabricksJdbcUrlParams.ALLOW_SELF_SIGNED_CERTS,
          DatabricksJdbcUrlParams.USE_SYSTEM_TRUST_STORE,
          DatabricksJdbcUrlParams.ROWS_FETCHED_PER_BLOCK,
          DatabricksJdbcUrlParams.AZURE_WORKSPACE_RESOURCE_ID,
          DatabricksJdbcUrlParams.AZURE_TENANT_ID,
          DatabricksJdbcUrlParams.DEFAULT_STRING_COLUMN_LENGTH,
          DatabricksJdbcUrlParams.SOCKET_TIMEOUT,
          DatabricksJdbcUrlParams.TOKEN_CACHE_PASS_PHRASE,
          DatabricksJdbcUrlParams.ENABLE_TOKEN_CACHE);

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
