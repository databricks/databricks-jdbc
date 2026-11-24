package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.common.AuthMech.OAUTH;
import static com.databricks.jdbc.common.AuthMech.PAT;
import static com.databricks.jdbc.common.DatabricksJdbcUrlParams.*;

import com.databricks.jdbc.api.impl.DatabricksConnectionContext;
import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.common.AuthFlow;
import com.databricks.jdbc.common.AuthMech;
import com.databricks.jdbc.common.DatabricksJdbcUrlParams;
import com.databricks.jdbc.common.LogLevel;
import com.databricks.jdbc.exception.DatabricksDriverException;
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
          DatabricksJdbcUrlParams.USER_AGENT_ENTRY,
          DatabricksJdbcUrlParams.ENABLE_SQL_EXEC_HYBRID_RESULTS,
          DatabricksJdbcUrlParams.ENABLE_SQL_EXEC_DIRECT_RESULTS,
          DatabricksJdbcUrlParams.ENABLE_COMPLEX_DATATYPE_SUPPORT,
          DatabricksJdbcUrlParams.ROWS_FETCHED_PER_BLOCK,
          DatabricksJdbcUrlParams.DEFAULT_STRING_COLUMN_LENGTH,
          DatabricksJdbcUrlParams.SOCKET_TIMEOUT,
          DatabricksJdbcUrlParams.ENABLE_TOKEN_CACHE,
          DatabricksJdbcUrlParams.ENABLE_SQL_VALIDATION_FOR_IS_VALID,
          DatabricksJdbcUrlParams.DISABLE_OAUTH_REFRESH_TOKEN,
          DatabricksJdbcUrlParams.ENABLE_MULTIPLE_CATALOG_SUPPORT,
          DatabricksJdbcUrlParams.IGNORE_TRANSACTIONS);

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
    return buildMissingPropertiesList(connectionContext);
  }

  public static List<DriverPropertyInfo> buildMissingPropertiesList(
      DatabricksConnectionContext connectionContext) throws DatabricksParsingException {
    List<DriverPropertyInfo> missingPropertyInfos = new ArrayList<>();
    // add required properties
    for (DatabricksJdbcUrlParams param : DatabricksJdbcUrlParams.values()) {
      if (param.isRequired() && param != PWD) {
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

    if (connectionContext.isPropertyPresent(SSL)) {
      addMissingProperty(missingPropertyInfos, connectionContext, SSL_TRUST_STORE, false);
      if (connectionContext.getSSLTrustStore() != null
          && !connectionContext.getSSLTrustStore().isEmpty()) {
        addMissingProperty(missingPropertyInfos, connectionContext, SSL_TRUST_STORE_TYPE, false);
      }
      addMissingProperty(missingPropertyInfos, connectionContext, USE_SYSTEM_TRUST_STORE, false);
      addMissingProperty(missingPropertyInfos, connectionContext, ALLOW_SELF_SIGNED_CERTS, false);
      addMissingProperty(
          missingPropertyInfos, connectionContext, CHECK_CERTIFICATE_REVOCATION, false);
      if (connectionContext.checkCertificateRevocation()) {
        addMissingProperty(
            missingPropertyInfos,
            connectionContext,
            ACCEPT_UNDETERMINED_CERTIFICATE_REVOCATION,
            false);
      } else if (!connectionContext.isPropertyPresent(CHECK_CERTIFICATE_REVOCATION)) {
        addMissingProperty(
            missingPropertyInfos,
            connectionContext,
            ACCEPT_UNDETERMINED_CERTIFICATE_REVOCATION,
            false);
      }
    }

    // auth-related properties
    AuthMech authMech = null;
    try {
      authMech =
          connectionContext.hasNonBlankProperty(AUTH_MECH)
              ? connectionContext.getAuthMech()
              : AuthMech.parseAuthMech(AUTH_MECH.getDefaultValue());
    } catch (DatabricksDriverException invalidAuthMech) {
      missingPropertyInfos.add(getUrlParamInfo(AUTH_MECH, true));
    }
    if (authMech == PAT) {
      if (isNullOrBlank(connectionContext.getToken())) {
        missingPropertyInfos.add(getUrlParamInfo(PWD, true));
      }
    } else if (authMech == OAUTH) {
      addMissingProperty(
          missingPropertyInfos, connectionContext, IDENTITY_FEDERATION_CLIENT_ID, false);

      AuthFlow authFlow = connectionContext.getAuthFlowIfValid();
      if (authFlow != null) {
        switch (authFlow) {
          case TOKEN_PASSTHROUGH:
            if (!isNullOrBlank(connectionContext.getOAuthRefreshToken())) {
              addMissingClientId(missingPropertyInfos, connectionContext);
              addMissingClientSecret(missingPropertyInfos, connectionContext);
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
            } else if (connectionContext.getCloud() == Cloud.AZURE) {
              addMissingProperty(missingPropertyInfos, connectionContext, AZURE_TENANT_ID, false);
            }
            addMissingClientSecret(missingPropertyInfos, connectionContext);
            addMissingClientId(missingPropertyInfos, connectionContext);

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
            addMissingProperty(
                missingPropertyInfos, connectionContext, OAUTH_REDIRECT_URL_PORT, false);
            addMissingProperty(missingPropertyInfos, connectionContext, ENABLE_TOKEN_CACHE, false);
            if (connectionContext.isPropertyPresent(ENABLE_TOKEN_CACHE)
                && connectionContext.isTokenCacheEnabled()) {
              addMissingProperty(
                  missingPropertyInfos, connectionContext, TOKEN_CACHE_PASS_PHRASE, true);
            }
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

    List<DriverPropertyInfo> requiredProperties = new ArrayList<>();
    List<DriverPropertyInfo> optionalProperties = new ArrayList<>();
    for (DriverPropertyInfo property : missingPropertyInfos) {
      if (property.required) {
        requiredProperties.add(property);
      } else {
        optionalProperties.add(property);
      }
    }
    requiredProperties.addAll(optionalProperties);
    return requiredProperties;
  }

  private static void handleTokenEndpointAndDiscoveryMode(
      List<DriverPropertyInfo> missingPropertyInfos,
      DatabricksConnectionContext connectionContext) {
    if (isNullOrBlank(connectionContext.getTokenEndpoint())
        && connectionContext.isOAuthDiscoveryModeEnabled()
        && isNullOrBlank(connectionContext.getOAuthDiscoveryURL())) {
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
    if (required
        ? !connectionContext.hasNonBlankProperty(param)
        : !connectionContext.isPropertyPresent(param)) {
      missingPropertyInfos.add(getUrlParamInfo(param, required));
    }
  }

  /**
   * Reports {@code OAuth2Secret} as missing unless a client id is resolvable — either from the
   * explicit param or, in M2M mode, from the JDBC user/UID fallback (see issue #1132). Uses the
   * resolved value from {@link DatabricksConnectionContext#getNullableClientId()} rather than raw
   * key presence, so a UID/user of {@code "token"} (the PAT sentinel, which the resolver ignores)
   * is correctly still reported as missing.
   */
  private static void addMissingClientId(
      List<DriverPropertyInfo> missingPropertyInfos,
      DatabricksConnectionContext connectionContext) {
    if (!isNullOrBlank(connectionContext.getNullableClientId())) {
      return;
    }
    missingPropertyInfos.add(getUrlParamInfo(CLIENT_ID, true));
  }

  /**
   * Reports {@code OAuth2Secret} as missing unless a client secret is resolvable — either from the
   * explicit param or, in M2M mode, from the JDBC password/PWD fallback (see issue #1132). Uses the
   * resolved value from {@link DatabricksConnectionContext#getClientSecret()} so it stays
   * consistent with what the auth flow actually receives.
   */
  private static void addMissingClientSecret(
      List<DriverPropertyInfo> missingPropertyInfos,
      DatabricksConnectionContext connectionContext) {
    if (!isNullOrBlank(connectionContext.getClientSecret())) {
      return;
    }
    missingPropertyInfos.add(getUrlParamInfo(CLIENT_SECRET, true));
  }

  private static boolean isNullOrBlank(String value) {
    return value == null || value.isBlank();
  }
}
