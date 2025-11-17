package com.databricks.jdbc.api.internal;

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
   * Returns just the host parsed from JDBC connection. Note : this is not the url.
   *
   * @return Databricks host
   */
  String getHost();

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

  /**
   * Returns the OAuth scopes to request for the user-to-machine (U2M) authorization flow.
   *
   * <p>If an explicit auth scope is provided via connection parameters, this returns a singleton
   * list containing that scope. On AWS and GCP, this returns the SQL scope and offline access
   * scope. On Azure, this returns {@code null} because the default scope is set by the Databricks
   * SDK.
   *
   * @return a list of OAuth scopes to request, or {@code null} on Azure to use the SDK default
   * @throws DatabricksParsingException if connection parameters cannot be parsed
   */
  List<String> getOAuthScopesForU2M() throws DatabricksParsingException;

  AuthMech getAuthMech();

  AuthFlow getAuthFlow();

  boolean isPropertyPresent(DatabricksJdbcUrlParams urlParam);

  LogLevel getLogLevel();

  TelemetryLogLevel getTelemetryLogLevel();

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

  /**
   * Returns the custom headers set in the JDBC connection string.
   *
   * @return Map of custom headers
   */
  Map<String, String> getCustomHeaders();

  boolean isAllPurposeCluster();

  String getHttpPath();

  /** Returns the value of the EnableSQLValidationForIsValid connection property. */
  boolean getEnableSQLValidationForIsValid();

  /** Returns the value of the enableMultipleCatalogSupport connection property. */
  boolean getEnableMultipleCatalogSupport();

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

  /** Returns the minimum expected download speed threshold in MB/s for CloudFetch operations */
  double getCloudFetchSpeedThreshold();

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

  /**
   * OAuth Client Id for identity federation which is used in exchanging the access token with
   * Databricks in-house token
   */
  String getIdentityFederationClientId();

  /** Returns the discovery URL used to obtain the OAuth2 token and authorization endpoints. */
  String getOAuthDiscoveryURL();

  /** Returns the OAuth2 authentication scope used in the request. */
  String getAuthScope();

  /**
   * Returns the OAuth2 refresh token used to obtain a new access token when the current one
   * expires.
   */
  String getOAuthRefreshToken();

  /** Returns the list of OAuth2 redirect URL ports used for OAuth authentication. */
  List<Integer> getOAuth2RedirectUrlPorts();

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

  /** Returns the SSL key store file path used for SSL connections. */
  String getSSLKeyStore();

  /** Returns the SSL key store password of the key store file. */
  String getSSLKeyStorePassword();

  /** Returns the SSL key store type of the key store file. */
  String getSSLKeyStoreType();

  /** Returns the SSL key store provider for the key store. */
  String getSSLKeyStoreProvider();

  /** Returns the SSL trust store provider for the trust store. */
  String getSSLTrustStoreProvider();

  /** Returns the maximum number of commands that can be executed in a single batch. */
  int getMaxBatchSize();

  /** Checks if Telemetry is enabled */
  boolean isTelemetryEnabled();

  /** Returns the batch size for Telemetry logs processing */
  int getTelemetryBatchSize();

  /** Returns the maximum number of rows per batch insert execution */
  int getBatchInsertSize();

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

  /** Returns true if driver should use direct results in SQL_EXEC API. */
  boolean isSqlExecDirectResultsEnabled();

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

  /** Returns the socket timeout in seconds for HTTP connections. */
  int getSocketTimeout();

  /**
   * Returns whether self-signed certificates are allowed for SSL connections.
   *
   * <p>When true, the driver will accept any certificate, including self-signed certificates. This
   * option is insecure and should only be used in non-production environments.
   *
   * @return true if self-signed certificates are allowed, false otherwise
   */
  boolean allowSelfSignedCerts();

  /**
   * Returns whether the system property trust store should be used for SSL certificate validation.
   *
   * <p>When true, the driver will use either:
   *
   * <ol>
   *   <li>The trust store specified by the Java system property <code>javax.net.ssl.trustStore
   *       </code> if set
   *   <li>Or the JDK's default trust store (cacerts) if no system property is set
   * </ol>
   *
   * <p>When false, the driver will:
   *
   * <ol>
   *   <li>Use the custom trust store specified by the SSLTrustStore parameter if provided
   *   <li>Or use the JDK's default trust store (cacerts) but ignore any javax.net.ssl.trustStore
   *       system property
   * </ol>
   *
   * @return true if the system property trust store should be used, false otherwise
   */
  boolean useSystemTrustStore();

  /** Returns the passphrase used for encrypting/decrypting token cache */
  String getTokenCachePassPhrase();

  /** Returns whether token caching is enabled for OAuth authentication */
  boolean isTokenCacheEnabled();

  /*
   * Returns maximum number of concurrent pre-signed requests sent to Databricks File System (DBFS)
   * Ensures rate-limit when uploading multiple files to DBFS in parallel.
   */
  int getMaxDBFSConcurrentPresignedRequests();

  /** Returns the application name using JDBC Connection */
  String getApplicationName();

  /** Returns the timeout in seconds for waiting for a chunk to be ready. */
  int getChunkReadyTimeoutSeconds();

  /** Returns whether telemetry is enabled for all connections */
  boolean forceEnableTelemetry();

  /** Returns the flush interval in milliseconds for telemetry */
  int getTelemetryFlushIntervalInMilliseconds();

  /** Returns whether circuit breaker is enabled for telemetry */
  boolean isTelemetryCircuitBreakerEnabled();

  /** Returns the maximum number of HTTP connections per route */
  int getHttpMaxConnectionsPerRoute();

  /** Returns the HTTP connection request timeout in seconds */
  Integer getHttpConnectionRequestTimeout();

  boolean enableShowCommandsForGetFunctions();

  /** Returns whether batched INSERT optimization is enabled */
  boolean isBatchedInsertsEnabled();

  /** Returns whether transaction-related method calls should be ignored */
  boolean getIgnoreTransactions();

  /**
   * Returns whether to fetch auto-commit state from server using SQL query instead of cached value
   */
  boolean getFetchAutoCommitFromServer();

  /* Returns whether metric view metadata is enabled */
  boolean getEnableMetricViewMetadata();

  /**
   * Returns whether the x-databricks-sea-can-run-fully-sync header should be enabled for
   * synchronous metadata requests in SEA mode
   */
  boolean isSeaSyncMetadataEnabled();

  /** Returns whether OAuth refresh tokens should be disabled (omit offline_access by default). */
  boolean getDisableOauthRefreshToken();
}
