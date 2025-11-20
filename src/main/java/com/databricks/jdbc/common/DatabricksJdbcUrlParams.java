package com.databricks.jdbc.common;

import java.sql.DriverPropertyInfo;

/** Enum to hold all the Databricks JDBC URL parameters. */
public enum DatabricksJdbcUrlParams {
  LOG_LEVEL("loglevel", "Log level for debugging"),
  TELEMETRY_LOG_LEVEL("telemetryLogLevel", "Log level for telemetry logs", "DEBUG"),
  LOG_PATH("logpath", "Path to the log file"),
  LOG_FILE_SIZE("LogFileSize", "Maximum size of the log file", "10"), // 10 MB
  LOG_FILE_COUNT("LogFileCount", "Number of log files to retain", "10"),
  UID("uid", "UID for authentication"),
  PASSWORD("password", "Password for authentication"),
  CLIENT_ID("OAuth2ClientId", "OAuth2 Client ID"),
  CLIENT_SECRET("OAuth2Secret", "OAuth2 Client Secret"),
  AUTH_MECH("authmech", "Authentication mechanism", "3", true),
  AUTH_ACCESS_TOKEN("Auth_AccessToken", "OAuth2 Access Token"),
  CONN_CATALOG("conncatalog", "Connection catalog"),
  CONN_SCHEMA("connschema", "Connection schema"),
  PROXY_HOST("proxyhost", "Proxy host"),
  PROXY_PORT("proxyport", "Proxy port"),
  PROXY_USER("proxyuid", "Proxy username"),
  PROXY_PWD("proxypwd", "Proxy password"),
  USE_PROXY("useproxy", "Use proxy"),
  PROXY_AUTH("proxyauth", "Proxy authentication"),
  NON_PROXY_HOSTS("proxyignorelist", "Non-proxy hosts", ""),
  USE_SYSTEM_PROXY("usesystemproxy", "Use system proxy"),
  USE_CF_PROXY("usecfproxy", "Use Cloudfetch proxy"),
  CF_PROXY_HOST("cfproxyhost", "Cloudfetch proxy host"),
  CF_PROXY_PORT("cfproxyport", "Cloudfetch proxy port"),
  CF_PROXY_AUTH("cfproxyauth", "Cloudfetch proxy authentication", "0"),
  CF_PROXY_USER("cfproxyuid", "Cloudfetch proxy username"),
  CF_PROXY_PWD("cfproxypwd", "Cloudfetch proxy password"),
  AUTH_FLOW("auth_flow", "Authentication flow"),
  OAUTH_REFRESH_TOKEN("Auth_RefreshToken", "OAuth2 Refresh Token"),
  OAUTH_REFRESH_TOKEN_2("OAuthRefreshToken", "OAuth2 Refresh Token"), // Same as OAUTH_REFRESH_TOKEN
  OAUTH_REDIRECT_URL_PORT("OAuth2RedirectUrlPort", "OAuth2 Redirect URL port", "8020"),
  PWD("pwd", "Password (used when AUTH_MECH = 3)", true),
  POLL_INTERVAL("asyncexecpollinterval", "Async execution poll interval", "200"),
  HTTP_PATH("httppath", "HTTP path", true),
  HTTP_HEADERS("http.header.", "Custom HTTP headers"),
  USE_THRIFT_CLIENT("usethriftclient", "Use Thrift client", "1"),
  RATE_LIMIT_RETRY_TIMEOUT("RateLimitRetryTimeout", "Rate limit retry timeout", "120"),
  JWT_KEY_FILE("Auth_JWT_Key_File", "JWT key file"),
  JWT_ALGORITHM("Auth_JWT_Alg", "JWT algorithm"),
  JWT_PASS_PHRASE("Auth_JWT_Key_Passphrase", "JWT key passphrase"),
  JWT_KID("Auth_KID", "JWT key ID"),
  USE_JWT_ASSERTION("UseJWTAssertion", "Use JWT assertion", "0"),
  OIDC_DISCOVERY_MODE("EnableOIDCDiscovery", "OIDC discovery mode", "1"),
  DISCOVERY_MODE("OAuthDiscoveryMode", "OAuth discovery mode", "1"), // Same as OIDC_DISCOVERY_MODE
  AUTH_SCOPE("Auth_Scope", "Authentication scope"),
  OIDC_DISCOVERY_ENDPOINT("OIDCDiscoveryEndpoint", "OIDC Discovery Endpoint"),
  DISCOVERY_URL("OAuthDiscoveryURL", "OAuth discovery URL"), // Same as OIDC_DISCOVERY_ENDPOINT
  IDENTITY_FEDERATION_CLIENT_ID(
      "Identity_Federation_Client_Id", "OAuth Client ID for Token Federation"),
  ENABLE_ARROW("EnableArrow", "Enable Arrow", "1"),
  DIRECT_RESULT("EnableDirectResults", "Enable direct results", "1"),
  LZ4_COMPRESSION_FLAG(
      "EnableQueryResultLZ4Compression", "Enable LZ4 compression"), // Backward compatibility
  COMPRESSION_FLAG("QueryResultCompressionType", "Query result compression type"),
  USER_AGENT_ENTRY("useragententry", "User agent entry"),
  USE_EMPTY_METADATA("useemptymetadata", "Use empty metadata"),
  TEMPORARILY_UNAVAILABLE_RETRY(
      "TemporarilyUnavailableRetry", "Retry on temporarily unavailable", "1"),
  TEMPORARILY_UNAVAILABLE_RETRY_TIMEOUT(
      "TemporarilyUnavailableRetryTimeout", "Retry timeout for temporarily unavailable", "900"),
  RATE_LIMIT_RETRY("RateLimitRetry", "Retry on rate limit", "1"),
  IDLE_HTTP_CONNECTION_EXPIRY("IdleHttpConnectionExpiry", "Idle HTTP connection expiry", "60"),
  SUPPORT_MANY_PARAMETERS("supportManyParameters", "Support many parameters", "0"),
  CLOUD_FETCH_THREAD_POOL_SIZE("cloudFetchThreadPoolSize", "Cloud fetch thread pool size", "16"),
  OAUTH_ENDPOINT("OAuth2ConnAuthAuthorizeEndpoint", "OAuth2 authorization endpoint"),
  AUTH_ENDPOINT(
      "OAuth2AuthorizationEndPoint", "OAuth2 authorization endpoint"), // Same as OAUTH_ENDPOINT
  OAUTH_TOKEN_ENDPOINT("OAuth2ConnAuthTokenEndpoint", "OAuth2 token endpoint"),
  TOKEN_ENDPOINT("OAuth2TokenEndpoint", "OAuth2 token endpoint"), // Same as OAUTH_TOKEN_ENDPOINT
  SSL("ssl", "Use SSL"),
  ALLOW_SELF_SIGNED_CERTS("AllowSelfSignedCerts", "Allow self signed certificates", "0"),
  SSL_TRUST_STORE("SSLTrustStore", "SSL trust store"),
  SSL_TRUST_STORE_PASSWORD("SSLTrustStorePwd", "SSL trust store password"),
  SSL_TRUST_STORE_TYPE("SSLTrustStoreType", "SSL trust store type", "JKS"),
  SSL_KEY_STORE("SSLKeyStore", "SSL key store"),
  SSL_KEY_STORE_PASSWORD("SSLKeyStorePwd", "SSL key store password"),
  SSL_KEY_STORE_TYPE("SSLKeyStoreType", "SSL key store type", "JKS"),
  SSL_KEY_STORE_PROVIDER("SSLKeyStoreProvider", "SSL key store provider"),
  SSL_TRUST_STORE_PROVIDER("SSLTrustStoreProvider", "SSL trust store provider"),
  USE_SYSTEM_TRUST_STORE("UseSystemTrustStore", "Use system trust store for SSL", "0"),
  CHECK_CERTIFICATE_REVOCATION("CheckCertRevocation", "Check certificate revocation", "1"),
  ACCEPT_UNDETERMINED_CERTIFICATE_REVOCATION(
      "AcceptUndeterminedRevocation", "Accept undetermined revocation", "0"),
  GOOGLE_SERVICE_ACCOUNT("GoogleServiceAccount", "Gcp service account email"),
  GOOGLE_CREDENTIALS_FILE("GoogleCredentialsFile", "path to gcp credentials json"),
  ENABLE_TELEMETRY(
      "EnableTelemetry",
      "flag to enable telemetry",
      "1"), // Note : telemetry enablement also depends on the server flag.
  TELEMETRY_BATCH_SIZE("TelemetryBatchSize", "Batch size for telemetry", "200"),
  MAX_BATCH_SIZE("MaxBatchSize", "Maximum batch size", "500"),
  BATCH_INSERT_SIZE("BatchInsertSize", "Maximum number of rows per batch insert execution", "1000"),
  ALLOWED_VOLUME_INGESTION_PATHS("VolumeOperationAllowedLocalPaths", ""),
  ALLOWED_STAGING_INGESTION_PATHS("StagingAllowedLocalPaths", ""),
  UC_INGESTION_RETRIABLE_HTTP_CODE(
      "UCIngestionRetriableHttpCode",
      "Retryable HTTP codes for UC Ingestion",
      "408,429,500,502,503,504"),
  VOLUME_OPERATION_RETRYABLE_HTTP_CODE(
      "VolumeOperationRetryableHttpCode",
      "Retryable HTTP codes for UC Ingestion",
      "408,429,500,502,503,504"),
  UC_INGESTION_RETRY_TIMEOUT(
      "UCIngestionRetryTimeout",
      "The retry timeout in minutes for UC Ingestion HTTP requests.",
      "15"),
  VOLUME_OPERATION_RETRY_TIMEOUT(
      "VolumeOperationRetryTimeout",
      "The retry timeout in minutes for UC Ingestion HTTP requests.",
      "15"),
  ENABLE_REQUEST_TRACING("EnableRequestTracing", "flag to enable request tracing", "0"),
  HTTP_CONNECTION_POOL_SIZE("HttpConnectionPoolSize", "Maximum HTTP connection pool size", "100"),
  ENABLE_SQL_EXEC_HYBRID_RESULTS(
      "EnableSQLExecHybridResults", "flag to enable hybrid results", "1"),
  ENABLE_SQL_EXEC_DIRECT_RESULTS(
      "EnableSQLExecDirectResults", "flag to enable direct results", "1"),
  ENABLE_COMPLEX_DATATYPE_SUPPORT(
      "EnableComplexDatatypeSupport",
      "flag to enable native support of complex data types as java objects",
      "0"),
  ROWS_FETCHED_PER_BLOCK(
      "RowsFetchedPerBlock",
      "The maximum number of rows that a query returns at a time.",
      "100000"), // works only for inline results.
  AZURE_WORKSPACE_RESOURCE_ID(
      "azure_workspace_resource_id", "Resource ID of Azure Databricks workspace"),
  AZURE_TENANT_ID("AzureTenantId", "Azure tenant ID"),
  DEFAULT_STRING_COLUMN_LENGTH(
      "DefaultStringColumnLength",
      "Maximum number of characters that can be contained in STRING columns",
      "255"),
  SOCKET_TIMEOUT("socketTimeout", "Socket timeout in seconds", "900"),
  TOKEN_CACHE_PASS_PHRASE("TokenCachePassPhrase", "Pass phrase to use for OAuth U2M Token Cache"),
  ENABLE_TOKEN_CACHE("EnableTokenCache", "Enable caching OAuth tokens", "1"),
  APPLICATION_NAME("ApplicationName", "Name of application using the driver", ""),
  CHUNK_READY_TIMEOUT_SECONDS(
      "ChunkReadyTimeoutSeconds",
      "Time limit for a chunk to be ready to consume when downloading",
      "0"),
  FORCE_ENABLE_TELEMETRY("ForceEnableTelemetry", "Force enable telemetry", "0"),
  TELEMETRY_FLUSH_INTERVAL(
      "TelemetryFlushInterval", "Flush interval in milliseconds", "300000"), // 5 MINUTES
  MAX_CONCURRENT_PRESIGNED_REQUESTS(
      "MaxVolumeOperationConcurrentPresignedRequests",
      "Maximum number of concurrent presigned requests",
      "50"),
  TELEMETRY_CIRCUIT_BREAKER_ENABLED(
      "TelemetryCircuitBreakerEnabled", "Enable circuit breaker for telemetry", "1"),
  HTTP_MAX_CONNECTIONS_PER_ROUTE(
      "HttpMaxConnectionsPerRoute", "Maximum connections per route for HTTP client", "1000"),
  HTTP_CONNECTION_REQUEST_TIMEOUT(
      "HttpConnectionRequestTimeout", "HTTP connection request timeout in seconds"),
  CLOUD_FETCH_SPEED_THRESHOLD(
      "CloudFetchSpeedThreshold", "Minimum expected download speed in MB/s", "0.1"),
  ENABLE_SHOW_COMMAND_FOR_GET_FUNCTIONS(
      "EnableShowCommandForGetFunctions", "Use SQL command to fetch function list", "0"),
  ENABLE_BATCHED_INSERTS("EnableBatchedInserts", "Enable batched INSERT optimization", "0"),
  ENABLE_SQL_VALIDATION_FOR_IS_VALID(
      "EnableSQLValidationForIsValid",
      "Enable SQL query execution for connection validation in isValid() method",
      "0"),
  IGNORE_TRANSACTIONS("IgnoreTransactions", "Ignore transaction-related method calls", "0"),
  FETCH_AUTOCOMMIT_FROM_SERVER(
      "FetchAutoCommitFromServer",
      "Fetch auto-commit state from server using SQL query instead of using cached value",
      "0"),
  ENABLE_METRIC_VIEW_METADATA("EnableMetricViewMetadata", "Enable metric view metadata", "0"),
  ENABLE_MULTIPLE_CATALOG_SUPPORT(
      "enableMultipleCatalogSupport", "Enable multiple catalog support", "1"),
  ENABLE_CLOUD_FETCH("EnableQueryResultDownload", "Enable Cloud Fetch", "1"),
  ENABLE_SEA_SYNC_METADATA(
      "EnableSeaSyncMetadata",
      "Enable x-databricks-sea-can-run-fully-sync header for synchronous metadata requests in SEA mode",
      "1"),
  DISABLE_OAUTH_REFRESH_TOKEN(
      "DisableOauthRefreshToken",
      "Disable requesting OAuth refresh tokens (omit offline_access unless explicitly provided)",
      "1");

  private final String paramName;
  private final String defaultValue;
  private final String description;
  private final boolean required;

  DatabricksJdbcUrlParams(
      String paramName, String description, String defaultValue, boolean required) {
    this.paramName = paramName;
    this.defaultValue = defaultValue;
    this.description = description;
    this.required = required;
  }

  DatabricksJdbcUrlParams(String paramName, String description, boolean required) {
    this(paramName, description, null, required);
  }

  DatabricksJdbcUrlParams(String paramName, String description, String defaultValue) {
    this(paramName, description, defaultValue, false);
  }

  DatabricksJdbcUrlParams(String paramName, String description) {
    this(paramName, description, null, false);
  }

  public String getParamName() {
    return paramName;
  }

  public String getDefaultValue() {
    return defaultValue;
  }

  public String getDescription() {
    return description;
  }

  public boolean isRequired() {
    return required;
  }

  public static DriverPropertyInfo getUrlParamInfo(
      DatabricksJdbcUrlParams param, boolean required) {
    DriverPropertyInfo info = new DriverPropertyInfo(param.getParamName(), null);
    info.required = required;
    info.description = param.getDescription();
    return info;
  }
}
