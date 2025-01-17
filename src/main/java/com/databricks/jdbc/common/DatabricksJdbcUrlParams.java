package com.databricks.jdbc.common;

/** Enum to hold all the Databricks JDBC URL parameters. */
public enum DatabricksJdbcUrlParams {
  LOG_LEVEL("loglevel"),
  LOG_PATH("logpath"),
  LOG_FILE_SIZE("LogFileSize", "10"), // 10 MB
  LOG_FILE_COUNT("LogFileCount", "10"),
  USER("user"),
  PASSWORD("password"),
  CLIENT_ID("OAuth2ClientId"),
  CLIENT_SECRET("OAuth2Secret"),
  AUTH_MECH("authmech"),
  AUTH_ACCESS_TOKEN("Auth_AccessToken"),
  CONN_CATALOG("conncatalog"),
  CONN_SCHEMA("connschema"),
  PROXY_HOST("proxyhost"),
  PROXY_PORT("proxyport"),
  PROXY_USER("proxyuid"),
  PROXY_PWD("proxypwd"),
  USE_PROXY("useproxy"),
  PROXY_AUTH("proxyauth"),
  NON_PROXY_HOSTS("proxyignorelist", ""),
  USE_SYSTEM_PROXY("usesystemproxy"),
  USE_CF_PROXY("usecfproxy"),
  CF_PROXY_HOST("cfproxyhost"),
  CF_PROXY_PORT("cfproxyport"),
  CF_PROXY_AUTH("cfproxyauth", "0"),
  CF_PROXY_USER("cfproxyuid"),
  CF_PROXY_PWD("cfproxypwd"),
  AUTH_FLOW("auth_flow"),
  CATALOG("catalog"),
  SCHEMA("schema"),
  OAUTH_REFRESH_TOKEN("OAuthRefreshToken"),
  PWD("pwd"), // Only used when AUTH_MECH = 3
  POLL_INTERVAL("asyncexecpollinterval", "200"),
  HTTP_PATH("httppath"),
  SSL("ssl"),
  USE_THRIFT_CLIENT("usethriftclient"),
  RATE_LIMIT_RETRY_TIMEOUT("RateLimitRetryTimeout", "120"),
  JWT_KEY_FILE("Auth_JWT_Key_File"),
  JWT_ALGORITHM("Auth_JWT_Alg"),
  JWT_PASS_PHRASE("Auth_JWT_Key_Passphrase"),
  JWT_KID("Auth_KID"),
  USE_JWT_ASSERTION("UseJWTAssertion", "0"),
  DISCOVERY_MODE("OAuthDiscoveryMode", "1"),
  AUTH_SCOPE("Auth_Scope", "all-apis"),
  DISCOVERY_URL("OAuthDiscoveryURL"),
  ENABLE_ARROW("EnableArrow", "1"),
  DIRECT_RESULT("EnableDirectResults", "1"),
  LZ4_COMPRESSION_FLAG("EnableQueryResultLZ4Compression"),
  COMPRESSION_FLAG("QueryResultCompressionType"),
  USER_AGENT_ENTRY("useragententry"),
  USE_EMPTY_METADATA("useemptymetadata"),
  TEMPORARILY_UNAVAILABLE_RETRY("TemporarilyUnavailableRetry", "1"),
  TEMPORARILY_UNAVAILABLE_RETRY_TIMEOUT("TemporarilyUnavailableRetryTimeout", "900"),
  RATE_LIMIT_RETRY("RateLimitRetry", "1"),
  IDLE_HTTP_CONNECTION_EXPIRY("IdleHttpConnectionExpiry", "60"),
  SUPPORT_MANY_PARAMETERS("supportManyParameters", "0"),
  CLOUD_FETCH_THREAD_POOL_SIZE("cloudFetchThreadPoolSize", "16"),
  TOKEN_ENDPOINT("OAuth2TokenEndpoint"),
  AUTH_ENDPOINT("OAuth2AuthorizationEndPoint"),
  SSL_TRUST_STORE("SSLTrustStore"),
  SSL_TRUST_STORE_PASSWORD("SSLTrustStorePwd"),
  SSL_TRUST_STORE_TYPE("SSLTrustStoreType", "JKS"),
  CHECK_CERTIFICATE_REVOCATION("CheckCertRevocation", "1"),
  ACCEPT_UNDETERMINED_CERTIFICATE_REVOCATION("AcceptUndeterminedRevocation", "0"),
  GOOGLE_SERVICE_ACCOUNT("GoogleServiceAccount"),
  GOOGLE_CREDENTIALS_FILE("GoogleCredentialsFile"),
  ENABLE_TELEMETRY("EnableTelemetry", "0"), // Disabled for now
  TELEMETRY_BATCH_SIZE("TelemetryBatchSize", "200"),
  MAX_BATCH_SIZE("MaxBatchSize", "500"),
  ALLOWED_VOLUME_INGESTION_PATHS("VolumeOperationAllowedLocalPaths"),
  ALLOWED_STAGING_INGESTION_PATHS("StagingAllowedLocalPaths");

  private final String paramName;
  private final String defaultValue;

  DatabricksJdbcUrlParams(String paramName) {
    this.paramName = paramName;
    this.defaultValue = null;
  }

  DatabricksJdbcUrlParams(String paramName, String defaultValue) {
    this.paramName = paramName;
    this.defaultValue = defaultValue;
  }

  public String getParamName() {
    return paramName;
  }

  public String getDefaultValue() {
    return defaultValue;
  }
}
