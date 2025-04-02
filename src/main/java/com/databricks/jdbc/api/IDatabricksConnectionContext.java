package com.databricks.jdbc.api;

import com.databricks.jdbc.common.*;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.sdk.core.ProxyConfig;
import com.databricks.sdk.core.utils.Cloud;
import java.util.List;
import java.util.Map;

public interface IDatabricksConnectionContext {

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

  String getNullableClientId();

  String getClientSecret();

  List<String> getOAuthScopesForU2M() throws DatabricksParsingException;

  AuthMech getAuthMech();

  AuthFlow getAuthFlow();

  boolean isPropertyPresent(DatabricksJdbcUrlParams urlParam);

  LogLevel getLogLevel();

  String getLogPathString();

  int getLogFileSize();

  int getLogFileCount();

  /** Returns the userAgent string specific to client used to fetch results. */
  String getClientUserAgent();

  CompressionCodec getCompressionCodec();

  /** Returns the userAgent string specified as part of the JDBC connection string */
  String getCustomerUserAgent();

  String getCatalog();

  String getSchema();

  Map<String, String> getSessionConfigs();

  Map<String, String> getClientInfoProperties();

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

  Cloud getCloud() throws DatabricksParsingException;

  String getCloudFetchProxyHost();

  int getCloudFetchProxyPort();

  String getCloudFetchProxyUser();

  String getCloudFetchProxyPassword();

  ProxyConfig.ProxyAuthType getCloudFetchProxyAuthType();

  String getEndpointURL() throws DatabricksParsingException;

  int getAsyncExecPollInterval();

  Boolean shouldEnableArrow();

  DatabricksClientType getClientType();

  void setClientType(DatabricksClientType clientType);

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

  String getGcpAuthType() throws DatabricksParsingException;

  String getGoogleServiceAccount();

  String getGoogleCredentials();

  /** Returns the non-proxy hosts that should be excluded from proxying. */
  String getNonProxyHosts();

  /** Returns the SSL trust store file path used for SSL connections. */
  String getSSLTrustStore();

  /** Returns the SSL trust store password of the trust store file. */
  String getSSLTrustStorePassword();

  /** Returns the SSL trust store type of the trust store file. */
  String getSSLTrustStoreType();

  /** Returns the maximum number of commands that can be executed in a single batch. */
  int getMaxBatchSize();

  /** Checks if Telemetry is enabled */
  boolean isTelemetryEnabled();

  /** Returns the batch size for Telemetry logs processing */
  int getTelemetryBatchSize();

  /**
   * Returns a unique identifier for this connection context.
   *
   * <p>This UUID is generated when the connection context is instantiated and serves as a unique
   * internal identifier for each JDBC connection.
   */
  String getConnectionUuid();

  /** Returns allowlisted local file paths for UC Volume operations */
  String getVolumeOperationAllowedPaths();

  /** Returns true if driver should use hybrid results in SQL_EXEC API. */
  boolean isSqlExecHybridResultsEnabled();

  /** Returns the Azure tenant ID for the Azure Databricks workspace. */
  String getAzureTenantId();

  /** Returns true if request tracing should be enabled. */
  boolean isRequestTracingEnabled();

  /** Returns maximum number of characters that can be contained in STRING columns. */
  int getDefaultStringColumnLength();

  /** Returns true if driver return complex data type java objects natively as opposed to string */
  boolean isComplexDatatypeSupportEnabled();

  /** Returns the size for HTTP connection pool */
  int getHttpConnectionPoolSize();

  /** Returns the list of HTTP codes to retry for UC Volume Ingestion */
  List<Integer> getUCIngestionRetriableHttpCodes();

  /** Returns retry timeout in seconds for UC Volume Ingestion */
  int getUCIngestionRetryTimeoutSeconds();

  String getAzureWorkspaceResourceId();

  /** Returns maximum number of rows that a query returns at a time. */
  int getRowsFetchedPerBlock();
}
