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

  /**
   * Returns the OAuth host URL used for authentication
   *
   * @return OAuth host URL
   */
  String getHostForOAuth();

  /**
   * Returns the OAuth client ID used for authentication
   *
   * @return OAuth client ID
   * @throws DatabricksParsingException if there's an error parsing the client ID
   */
  String getClientId() throws DatabricksParsingException;

  /**
   * Returns the OAuth client ID if present, null otherwise
   *
   * @return OAuth client ID or null
   */
  String getNullableClientId();

  /**
   * Returns the OAuth client secret used for authentication
   *
   * @return OAuth client secret
   */
  String getClientSecret();

  /**
   * Returns the list of OAuth scopes required for user-to-machine authentication
   *
   * @return List of OAuth scopes
   * @throws DatabricksParsingException if there's an error parsing the scopes
   */
  List<String> getOAuthScopesForU2M() throws DatabricksParsingException;

  /**
   * Returns the authentication mechanism being used
   *
   * @return Authentication mechanism
   */
  AuthMech getAuthMech();

  /**
   * Returns the authentication flow being used
   *
   * @return Authentication flow
   */
  AuthFlow getAuthFlow();

  /**
   * Checks if a specific JDBC URL parameter is present in the connection string
   *
   * @param urlParam The parameter to check
   * @return true if the parameter is present, false otherwise
   */
  boolean isPropertyPresent(DatabricksJdbcUrlParams urlParam);

  /**
   * Returns the logging level for the JDBC driver
   *
   * @return Logging level
   */
  LogLevel getLogLevel();

  /**
   * Returns the path where log files will be stored
   *
   * @return Log file path
   */
  String getLogPathString();

  /**
   * Returns the maximum size of each log file in bytes
   *
   * @return Maximum log file size
   */
  int getLogFileSize();

  /**
   * Returns the maximum number of log files to maintain
   *
   * @return Maximum number of log files
   */
  int getLogFileCount();

  /**
   * Returns the userAgent string specific to client used to fetch results.
   *
   * @return userAgent string
   */
  String getClientUserAgent();

  /**
   * Returns the compression codec used for data transfer
   *
   * @return Compression codec
   */
  CompressionCodec getCompressionCodec();

  /**
   * Returns the userAgent string specified as part of the JDBC connection string
   *
   * @return userAgent string
   */
  String getCustomerUserAgent();

  /**
   * Returns the default catalog name
   *
   * @return Catalog name
   */
  String getCatalog();

  /**
   * Returns the default schema name
   *
   * @return Schema name
   */
  String getSchema();

  /**
   * Returns the session configuration parameters
   *
   * @return Map of session configuration key-value pairs
   */
  Map<String, String> getSessionConfigs();

  /**
   * Returns the client info properties
   *
   * @return Map of client info properties
   */
  Map<String, String> getClientInfoProperties();

  /**
   * Checks if the compute resource is an all-purpose cluster
   *
   * @return true if all-purpose cluster, false otherwise
   */
  boolean isAllPurposeCluster();

  /**
   * Returns the HTTP path for the JDBC connection
   *
   * @return HTTP path
   */
  String getHttpPath();

  /**
   * Returns the proxy host address
   *
   * @return Proxy host
   */
  String getProxyHost();

  /**
   * Returns the proxy port number
   *
   * @return Proxy port
   */
  int getProxyPort();

  /**
   * Returns the username for proxy authentication
   *
   * @return Proxy username
   */
  String getProxyUser();

  /**
   * Returns the password for proxy authentication
   *
   * @return Proxy password
   */
  String getProxyPassword();

  /**
   * Returns whether to use a proxy for connections
   *
   * @return true if proxy should be used, false otherwise
   */
  Boolean getUseProxy();

  /**
   * Returns the proxy authentication type
   *
   * @return Proxy authentication type
   */
  ProxyConfig.ProxyAuthType getProxyAuthType();

  /**
   * Returns whether to use system proxy settings
   *
   * @return true if system proxy should be used, false otherwise
   */
  Boolean getUseSystemProxy();

  /**
   * Returns whether to use proxy for cloud fetch operations
   *
   * @return true if cloud fetch proxy should be used, false otherwise
   */
  Boolean getUseCloudFetchProxy();

  /**
   * Returns the cloud provider for the Databricks deployment
   *
   * @return Cloud provider
   * @throws DatabricksParsingException if there's an error determining the cloud provider
   */
  Cloud getCloud() throws DatabricksParsingException;

  /**
   * Returns the host address for cloud fetch proxy
   *
   * @return Cloud fetch proxy host
   */
  String getCloudFetchProxyHost();

  /**
   * Returns the port number for cloud fetch proxy
   *
   * @return Cloud fetch proxy port
   */
  int getCloudFetchProxyPort();

  /**
   * Returns the username for cloud fetch proxy authentication
   *
   * @return Cloud fetch proxy username
   */
  String getCloudFetchProxyUser();

  /**
   * Returns the password for cloud fetch proxy authentication
   *
   * @return Cloud fetch proxy password
   */
  String getCloudFetchProxyPassword();

  /**
   * Returns the authentication type for cloud fetch proxy
   *
   * @return Cloud fetch proxy authentication type
   */
  ProxyConfig.ProxyAuthType getCloudFetchProxyAuthType();

  /**
   * Returns the endpoint URL for the Databricks deployment
   *
   * @return Endpoint URL
   * @throws DatabricksParsingException if there's an error parsing the endpoint URL
   */
  String getEndpointURL() throws DatabricksParsingException;

  /**
   * Returns the polling interval for async execution in milliseconds
   *
   * @return Polling interval
   */
  int getAsyncExecPollInterval();

  /**
   * Returns whether Arrow format should be enabled for data transfer
   *
   * @return true if Arrow should be enabled, false otherwise
   */
  Boolean shouldEnableArrow();

  /**
   * Returns the type of Databricks client being used
   *
   * @return Client type
   */
  DatabricksClientType getClientType();

  /**
   * Sets the type of Databricks client
   *
   * @param clientType The client type to set
   */
  void setClientType(DatabricksClientType clientType);

  /**
   * Returns whether to use empty metadata
   *
   * @return true if empty metadata should be used, false otherwise
   */
  Boolean getUseEmptyMetadata();

  /**
   * Returns the number of threads to be used for fetching data from cloud storage
   *
   * @return Number of threads
   */
  int getCloudFetchThreadPoolSize();

  /**
   * Returns whether to use direct result mode
   *
   * @return true if direct result mode should be used, false otherwise
   */
  Boolean getDirectResultMode();

  /**
   * Returns whether to retry on temporarily unavailable errors
   *
   * @return true if retry should be attempted, false otherwise
   */
  Boolean shouldRetryTemporarilyUnavailableError();

  /**
   * Returns whether to retry on rate limit errors
   *
   * @return true if retry should be attempted, false otherwise
   */
  Boolean shouldRetryRateLimitError();

  /**
   * Returns the timeout in seconds for retrying temporarily unavailable errors
   *
   * @return Timeout in seconds
   */
  int getTemporarilyUnavailableRetryTimeout();

  /**
   * Returns the timeout in seconds for retrying rate limit errors
   *
   * @return Timeout in seconds
   */
  int getRateLimitRetryTimeout();

  /**
   * Returns the expiry time in seconds for idle HTTP connections
   *
   * @return Expiry time in seconds
   */
  int getIdleHttpConnectionExpiry();

  /**
   * Returns whether the connection supports many parameters
   *
   * @return true if many parameters are supported, false otherwise
   */
  boolean supportManyParameters();

  /**
   * Returns the complete JDBC connection URL
   *
   * @return Connection URL
   */
  String getConnectionURL();

  /**
   * Returns whether to check certificate revocation
   *
   * @return true if certificate revocation should be checked, false otherwise
   */
  boolean checkCertificateRevocation();

  /**
   * Returns whether to accept undetermined certificate revocation status
   *
   * @return true if undetermined revocation status should be accepted, false otherwise
   */
  boolean acceptUndeterminedCertificateRevocation();

  /**
   * Returns the file path to the JWT private key used for signing the JWT.
   *
   * @return File path to JWT private key
   */
  String getJWTKeyFile();

  /**
   * Returns the Key ID (KID) used in the JWT header, identifying the key.
   *
   * @return Key ID (KID)
   */
  String getKID();

  /**
   * Returns the passphrase to decrypt the private key if the key is encrypted.
   *
   * @return Passphrase
   */
  String getJWTPassphrase();

  /**
   * Returns the algorithm used for signing the JWT (e.g., RS256, ES256).
   *
   * @return Algorithm
   */
  String getJWTAlgorithm();

  /**
   * Returns whether JWT assertion should be used for OAuth2 authentication.
   *
   * @return true if JWT assertion should be used, false otherwise
   */
  boolean useJWTAssertion();

  /**
   * Returns the OAuth2 token endpoint URL for retrieving tokens.
   *
   * @return Token endpoint URL
   */
  String getTokenEndpoint();

  /**
   * Returns the OAuth2 authorization endpoint URL for the authorization code flow.
   *
   * @return Authorization endpoint URL
   */
  String getAuthEndpoint();

  /**
   * Returns whether OAuth2 discovery mode is enabled, which fetches endpoints dynamically.
   *
   * @return true if discovery mode is enabled, false otherwise
   */
  boolean isOAuthDiscoveryModeEnabled();

  /**
   * Returns the discovery URL used to obtain the OAuth2 token and authorization endpoints.
   *
   * @return Discovery URL
   */
  String getOAuthDiscoveryURL();

  /**
   * Returns the OAuth2 authentication scope used in the request.
   *
   * @return Authentication scope
   */
  String getAuthScope();

  /**
   * Returns the OAuth2 refresh token used to obtain a new access token when the current one
   * expires.
   *
   * @return Refresh token
   */
  String getOAuthRefreshToken();

  /**
   * Returns the GCP authentication type
   *
   * @return GCP authentication type
   * @throws DatabricksParsingException if there's an error parsing the auth type
   */
  String getGcpAuthType() throws DatabricksParsingException;

  /**
   * Returns the Google service account information
   *
   * @return Google service account
   */
  String getGoogleServiceAccount();

  /**
   * Returns the Google credentials information
   *
   * @return Google credentials
   */
  String getGoogleCredentials();

  /**
   * Returns the non-proxy hosts that should be excluded from proxying.
   *
   * @return Non-proxy hosts
   */
  String getNonProxyHosts();

  /**
   * Returns the SSL trust store file path used for SSL connections.
   *
   * @return SSL trust store file path
   */
  String getSSLTrustStore();

  /**
   * Returns the SSL trust store password of the trust store file.
   *
   * @return SSL trust store password
   */
  String getSSLTrustStorePassword();

  /**
   * Returns the SSL trust store type of the trust store file.
   *
   * @return SSL trust store type
   */
  String getSSLTrustStoreType();

  /**
   * Returns the maximum number of commands that can be executed in a single batch.
   *
   * @return Maximum batch size
   */
  int getMaxBatchSize();

  /**
   * Checks if Telemetry is enabled
   *
   * @return true if Telemetry is enabled, false otherwise
   */
  boolean isTelemetryEnabled();

  /**
   * Returns the batch size for Telemetry logs processing
   *
   * @return Batch size
   */
  int getTelemetryBatchSize();

  /**
   * Returns a unique identifier for this connection context.
   *
   * <p>This UUID is generated when the connection context is instantiated and serves as a unique
   * internal identifier for each JDBC connection.
   *
   * @return Unique identifier
   */
  String getConnectionUuid();

  /**
   * Returns allowlisted local file paths for UC Volume operations
   *
   * @return Allowlisted file paths
   */
  String getVolumeOperationAllowedPaths();

  /**
   * Returns true if driver should use hybrid results in SQL_EXEC API.
   *
   * @return true if hybrid results should be used, false otherwise
   */
  boolean isSqlExecHybridResultsEnabled();

  /**
   * Returns the Azure tenant ID for the Azure Databricks workspace.
   *
   * @return Azure tenant ID
   */
  String getAzureTenantId();

  /**
   * Returns true if request tracing should be enabled.
   *
   * @return true if request tracing should be enabled, false otherwise
   */
  boolean isRequestTracingEnabled();

  /** Returns maximum number of characters that can be contained in STRING columns. */
  int getDefaultStringColumnLength();

  /**
   * Returns true if driver return complex data type java objects natively as opposed to string
   *
   * @return true if complex data type support is enabled, false otherwise
   */
  boolean isComplexDatatypeSupportEnabled();

  /**
   * Returns the size for HTTP connection pool
   *
   * @return HTTP connection pool size
   */
  int getHttpConnectionPoolSize();

  /** Returns the list of HTTP codes to retry for UC Volume Ingestion */
  List<Integer> getUCIngestionRetriableHttpCodes();

  /** Returns retry timeout in seconds for UC Volume Ingestion */
  int getUCIngestionRetryTimeoutSeconds();

  /**
   * Returns the Azure workspace resource ID
   *
   * @return Azure workspace resource ID
   */
  String getAzureWorkspaceResourceId();

  /** Returns maximum number of rows that a query returns at a time. */
  int getRowsFetchedPerBlock();
}
