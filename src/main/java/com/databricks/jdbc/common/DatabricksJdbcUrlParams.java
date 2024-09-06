package com.databricks.jdbc.common;

/** Enum to hold all the Databricks JDBC URL parameters. */
public enum DatabricksJdbcUrlParams {
  LOG_LEVEL("loglevel"),
  LOG_PATH("logpath"),
  LOG_FILE_SIZE("LogFileSize"),
  LOG_FILE_COUNT("LogFileCount"),
  USER("user"),
  PASSWORD("password"),
  CLIENT_ID("OAuth2ClientId"),
  CLIENT_SECRET("OAuth2Secret"),
  AUTH_MECH("authmech"),
  CONN_CATALOG("conncatalog"),
  CONN_SCHEMA("connschema"),
  PROXY_HOST("proxyhost"),
  PROXY_PORT("proxyport"),
  PROXY_USER("proxyuid"),
  PROXY_PWD("proxypwd"),
  USE_PROXY("useproxy"),
  PROXY_AUTH("proxyauth"),
  USE_SYSTEM_PROXY("usesystemproxy"),
  USE_CF_PROXY("usecfproxy"),
  CF_PROXY_HOST("cfproxyhost"),
  CF_PROXY_PORT("cfproxyport"),
  CF_PROXY_AUTH("cfproxyauth"),
  CF_PROXY_USER("cfproxyuid"),
  CF_PROXY_PWD("cfproxypwd"),
  AUTH_FLOW("auth_flow"),
  CATALOG("catalog"),
  SCHEMA("schema"),
  OAUTH_REFRESH_TOKEN("OAuthRefreshToken"),
  PWD("pwd"), // Only used when AUTH_MECH = 3
  POLL_INTERVAL("asyncexecpollinterval"),
  HTTP_PATH("httppath"),
  SSL("ssl"),
  USE_THRIFT_CLIENT("usethriftclient"),
  RATE_LIMIT_RETRY_TIMEOUT("RateLimitRetryTimeout"),
  JWT_KEY_FILE("Auth_JWT_Key_File"),
  JWT_ALGORITHM("Auth_JWT_Alg"),
  JWT_PASS_PHRASE("Auth_JWT_Key_Passphrase"),
  JWT_KID("Auth_KID"),
  USE_JWT_ASSERTION("UseJWTAssertion"),
  DISCOVERY_MODE("OAuthDiscoveryMode"),
  AUTH_SCOPE("Auth_Scope"),
  DISCOVERY_URL("OAuthDiscoveryURL"),
  ENABLE_ARROW("EnableArrow"),
  DIRECT_RESULT("EnableDirectResults"),
  LZ4_COMPRESSION_FLAG("EnableQueryResultLZ4Compression"), // Backward compatibility
  COMPRESSION_FLAG("QueryResultCompressionType"),
  USER_AGENT_ENTRY("useragententry"),
  USE_LEGACY_METADATA("uselegacymetadata"),
  TEMPORARILY_UNAVAILABLE_RETRY("TemporarilyUnavailableRetry"),
  TEMPORARILY_UNAVAILABLE_RETRY_TIMEOUT("TemporarilyUnavailableRetryTimeout"),
  RATE_LIMIT_RETRY("RateLimitRetry"),
  IDLE_HTTP_CONNECTION_EXPIRY("IdleHttpConnectionExpiry"),
  SUPPORT_MANY_PARAMETERS("supportManyParameters"),
  CLOUD_FETCH_THREAD_POOL_SIZE("cloudFetchThreadPoolSize"),
  ENABLE_TELEMETRY("enableTelemetry"),
  TOKEN_ENDPOINT("OAuth2TokenEndpoint"),
  AUTH_ENDPOINT("OAuth2AuthorizationEndPoint");

  private final String paramName;

  DatabricksJdbcUrlParams(String paramName) {
    this.paramName = paramName;
  }

  public String getParamName() {
    return paramName;
  }
}
