package com.databricks.jdbc.api;

import com.databricks.jdbc.common.CompressionType;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.common.LogLevel;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.sdk.core.ProxyConfig;
import java.sql.DriverPropertyInfo;
import java.util.List;
import java.util.Map;

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
   * Returns host-Url for Databricks server as parsed from JDBC connection in format <code>
   * https://server:port</code>
   *
   * @return Databricks host-Url
   */
  String getHostUrl() throws DatabricksParsingException;

  /**
   * Returns warehouse-Id as parsed from JDBC connection Url
   *
   * @return warehouse-Id
   */
  IDatabricksComputeResource getComputeResource();

  /**
   * Returns the auth token (personal access token)
   *
   * @return auth token
   */
  String getToken();

  /**
   * Returns the pass through access token
   *
   * @return access token
   */
  String getPassThroughAccessToken();

  String getHostForOAuth();

  String getClientId() throws DatabricksParsingException;

  String getClientSecret();

  List<String> getOAuthScopesForU2M() throws DatabricksParsingException;

  AuthMech getAuthMech();

  AuthFlow getAuthFlow();

  LogLevel getLogLevel();

  String getLogPathString();

  int getLogFileSize();

  int getLogFileCount();

  String getClientUserAgent();

  CompressionType getCompressionType();

  String getCatalog();

  String getSchema();

  Map<String, String> getSessionConfigs();

  boolean isAllPurposeCluster();

  String getHttpPath();

  String getProxyHost();

  int getProxyPort();

  String getProxyUser();

  String getProxyPassword();

  Boolean getUseProxy();

  ProxyConfig.ProxyAuthType getProxyAuthType();

  Boolean getUseSystemProxy();

  Boolean getUseCloudFetchProxy();

  String getCloudFetchProxyHost();

  int getCloudFetchProxyPort();

  String getCloudFetchProxyUser();

  String getCloudFetchProxyPassword();

  ProxyConfig.ProxyAuthType getCloudFetchProxyAuthType();

  String getEndpointURL() throws DatabricksParsingException;

  int getAsyncExecPollInterval();

  Boolean shouldEnableArrow();

  DatabricksClientType getClientType();

  Boolean getUseEmptyMetadata();

  /** Returns the number of threads to be used for fetching data from cloud storage */
  int getCloudFetchThreadPoolSize();

  Boolean getDirectResultMode();

  Boolean shouldRetryTemporarilyUnavailableError();

  Boolean shouldRetryRateLimitError();

  int getTemporarilyUnavailableRetryTimeout();

  int getRateLimitRetryTimeout();

  int getIdleHttpConnectionExpiry();

  boolean supportManyParameters();

  String getConnectionURL();

  boolean checkCertificateRevocation();

  boolean acceptUndeterminedCertificateRevocation();

  /** Returns the file path to the JWT private key used for signing the JWT. */
  String getJWTKeyFile();

  /** Returns the Key ID (KID) used in the JWT header, identifying the key. */
  String getKID();

  /** Returns the passphrase to decrypt the private key if the key is encrypted. */
  String getJWTPassphrase();

  /** Returns the algorithm used for signing the JWT (e.g., RS256, ES256). */
  String getJWTAlgorithm();

  /** Returns whether JWT assertion should be used for OAuth2 authentication. */
  boolean useJWTAssertion();

  /** Returns the OAuth2 token endpoint URL for retrieving tokens. */
  String getTokenEndpoint();

  /** Returns the OAuth2 authorization endpoint URL for the authorization code flow. */
  String getAuthEndpoint();

  /** Returns whether OAuth2 discovery mode is enabled, which fetches endpoints dynamically. */
  boolean isOAuthDiscoveryModeEnabled();

  /** Returns the discovery URL used to obtain the OAuth2 token and authorization endpoints. */
  String getOAuthDiscoveryURL();

  /** Returns the OAuth2 authentication scope used in the request. */
  String getAuthScope();

  /**
   * Returns the OAuth2 refresh token used to obtain a new access token when the current one
   * expires.
   */
  String getOAuthRefreshToken();

  /** Returns the non-proxy hosts that should be excluded from proxying. */
  String getNonProxyHosts();

  /** Returns the SSL trust store file path used for SSL connections. */
  String getSSLTrustStore();

  /** Returns the SSL trust store provider of the trust store file. */
  String getSSLTrustStoreProvider();

  /** Returns the SSL trust store password of the trust store file. */
  String getSSLTrustStorePassword();

  /** Returns the SSL trust store type of the trust store file. */
  String getSSLTrustStoreType();

  /** Returns the maximum number of commands that can be executed in a single batch. */
  int getMaxBatchSize();

  List<DriverPropertyInfo> getMissingProperties();
}
