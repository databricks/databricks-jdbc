package com.databricks.jdbc.common;

import java.sql.DriverPropertyInfo;

/** Enum to hold all the Databricks JDBC URL parameters. */
public enum DatabricksJdbcUrlParams {
  LOG_LEVEL("loglevel", "Log level for debugging"),
  LOG_PATH("logpath", "Path to the log file"),
  LOG_FILE_SIZE("LogFileSize", "Maximum size of the log file", "10"), // 10 MB
  LOG_FILE_COUNT("LogFileCount", "Number of log files to retain", "10"),
  USER("user", "Username for authentication"),
  PASSWORD("password", "Password for authentication"),
  CLIENT_ID("OAuth2ClientId", "OAuth2 Client ID"),
  CLIENT_SECRET("OAuth2Secret", "OAuth2 Client Secret"),
  AUTH_MECH("authmech", "Authentication mechanism", true),
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
  USE_CF_PROXY("usecfproxy", "Use Cloudflare proxy"),
  CF_PROXY_HOST("cfproxyhost", "Cloudflare proxy host"),
  CF_PROXY_PORT("cfproxyport", "Cloudflare proxy port"),
  CF_PROXY_AUTH("cfproxyauth", "Cloudflare proxy authentication", "0"),
  CF_PROXY_USER("cfproxyuid", "Cloudflare proxy username"),
  CF_PROXY_PWD("cfproxypwd", "Cloudflare proxy password"),
  AUTH_FLOW("auth_flow", "Authentication flow"),
  OAUTH_REFRESH_TOKEN("Auth_RefreshToken", "OAuth2 Refresh Token"),
  OAUTH_REFRESH_TOKEN_2("OAuthRefreshToken", "OAuth2 Refresh Token"), // Same as OAUTH_REFRESH_TOKEN
  OAUTH_REDIRECT_URL_PORT("OAuth2RedirectUrlPort", "OAuth2 Redirect URL port", "8020"),
  PWD("pwd", "Password (used when AUTH_MECH = 3)", true),
  POLL_INTERVAL("asyncexecpollinterval", "Async execution poll interval", "200"),
  HTTP_PATH("httppath", "HTTP path", true),
  SSL("ssl", "Use SSL"),
  USE_THRIFT_CLIENT("usethriftclient", "Use Thrift client", "1"),
  RATE_LIMIT_RETRY_TIMEOUT("RateLimitRetryTimeout", "Rate limit retry timeout", "120"),
  JWT_KEY_FILE("Auth_JWT_Key_File", "JWT key file"),
  JWT_ALGORITHM("Auth_JWT_Alg", "JWT algorithm"),
  JWT_PASS_PHRASE("Auth_JWT_Key_Passphrase", "JWT key passphrase"),
  JWT_KID("Auth_KID", "JWT key ID"),
  USE_JWT_ASSERTION("UseJWTAssertion", "Use JWT assertion", "0"),
  OIDC_DISCOVERY_MODE("EnableOIDCDiscovery", "OIDC discovery mode", "1"),
  DISCOVERY_MODE("OAuthDiscoveryMode", "OAuth discovery mode", "1"), // Same as OIDC_DISCOVERY_MODE
  AUTH_SCOPE("Auth_Scope", "Authentication scope", "all-apis"),
  OIDC_DISCOVERY_ENDPOINT("OIDCDiscoveryEndpoint", "OIDC Discovery Endpoint"),
  DISCOVERY_URL("OAuthDiscoveryURL", "OAuth discovery URL"), // Same as OIDC_DISCOVERY_ENDPOINT
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
  SSL_TRUST_STORE("SSLTrustStore", "SSL trust store"),
  SSL_TRUST_STORE_PASSWORD("SSLTrustStorePwd", "SSL trust store password"),
  SSL_TRUST_STORE_TYPE("SSLTrustStoreType", "SSL trust store type", "JKS"),
  CHECK_CERTIFICATE_REVOCATION("CheckCertRevocation", "Check certificate revocation", "1"),
  ACCEPT_UNDETERMINED_CERTIFICATE_REVOCATION(
      "AcceptUndeterminedRevocation", "Accept undetermined revocation", "0"),
  GOOGLE_SERVICE_ACCOUNT("GoogleServiceAccount", "Gcp service account email"),
  GOOGLE_CREDENTIALS_FILE("GoogleCredentialsFile", "path to gcp credentials json"),
  ENABLE_TELEMETRY("EnableTelemetry", "flag to enable telemetry", "0"), // Disabled for now
  TELEMETRY_BATCH_SIZE("TelemetryBatchSize", "Batch size for telemetry", "200"),
  MAX_BATCH_SIZE("MaxBatchSize", "Maximum batch size", "500"),
  ALLOWED_VOLUME_INGESTION_PATHS("VolumeOperationAllowedLocalPaths", ""),
  ALLOWED_STAGING_INGESTION_PATHS("StagingAllowedLocalPaths", ""),
  UC_INGESTION_RETRIABLE_HTTP_CODE(
      "UCIngestionRetriableHttpCode", "Retriable HTTP codes for UC Ingestion", "408,502,503,504"),
  UC_INGESTION_RETRY_TIMEOUT(
      "UCIngestionRetryTimeout",
      "The retry timeout in minutes for UC Ingestion HTTP requests.",
      "15"),
  ENABLE_REQUEST_TRACING("EnableRequestTracing", "flag to enable request tracing", "0"),
  HTTP_CONNECTION_POOL_SIZE("HttpConnectionPoolSize", "Maximum HTTP connection pool size", "100"),
  ENABLE_SQL_EXEC_HYBRID_RESULTS(
      "EnableSQLExecHybridResults", "flag to enable hybrid results", "1"),
  ENABLE_COMPLEX_DATATYPE_SUPPORT(
      "EnableComplexDatatypeSupport",
      "flag to enable native support of complex data types as java objects",
      "0"),
  ROWS_FETCHED_PER_BLOCK(
      "RowsFetchedPerBlock",
      "The maximum number of rows that a query returns at a time.",
      "2000000"), // works only for inline results.
  AZURE_WORKSPACE_RESOURCE_ID(
      "azure_workspace_resource_id", "Resource ID of Azure Databricks workspace"),
  AZURE_TENANT_ID("AzureTenantId", "Azure tenant ID"),
  DEFAULT_STRING_COLUMN_LENGTH(
      "DefaultStringColumnLength",
      "Maximum number of characters that can be contained in STRING columns",
      "255");

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
