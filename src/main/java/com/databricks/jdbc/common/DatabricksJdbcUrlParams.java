package com.databricks.jdbc.common;

import java.sql.DriverPropertyInfo;

/** Enum to hold all the Databricks JDBC URL parameters. */
public enum DatabricksJdbcUrlParams {
  LOG_LEVEL("loglevel", "Log level for debugging", false),
  LOG_PATH("logpath", "Path to the log file", false),
  LOG_FILE_SIZE("LogFileSize", "Maximum size of the log file", "10", false), // 10 MB
  LOG_FILE_COUNT("LogFileCount", "Number of log files to retain", "10", false),
  USER("user", "Username for authentication", true),
  PASSWORD("password", "Password for authentication", true),
  CLIENT_ID("OAuth2ClientId", "OAuth2 Client ID", true),
  CLIENT_SECRET("OAuth2Secret", "OAuth2 Client Secret", true),
  AUTH_MECH("authmech", "Authentication mechanism", true),
  AUTH_ACCESS_TOKEN("Auth_AccessToken", "OAuth2 Access Token", true),
  CONN_CATALOG("conncatalog", "Connection catalog", false),
  CONN_SCHEMA("connschema", "Connection schema", false),
  PROXY_HOST("proxyhost", "Proxy host", false),
  PROXY_PORT("proxyport", "Proxy port", false),
  PROXY_USER("proxyuid", "Proxy username", false),
  PROXY_PWD("proxypwd", "Proxy password", false),
  USE_PROXY("useproxy", "Use proxy", false),
  PROXY_AUTH("proxyauth", "Proxy authentication", false),
  NON_PROXY_HOSTS("proxyignorelist", "Non-proxy hosts", "", false),
  USE_SYSTEM_PROXY("usesystemproxy", "Use system proxy", false),
  USE_CF_PROXY("usecfproxy", "Use Cloudflare proxy", false),
  CF_PROXY_HOST("cfproxyhost", "Cloudflare proxy host", false),
  CF_PROXY_PORT("cfproxyport", "Cloudflare proxy port", false),
  CF_PROXY_AUTH("cfproxyauth", "Cloudflare proxy authentication", "0", false),
  CF_PROXY_USER("cfproxyuid", "Cloudflare proxy username", false),
  CF_PROXY_PWD("cfproxypwd", "Cloudflare proxy password", false),
  AUTH_FLOW("auth_flow", "Authentication flow", true),
  CATALOG("catalog", "Catalog name", false),
  SCHEMA("schema", "Schema name", false),
  OAUTH_REFRESH_TOKEN("OAuthRefreshToken", "OAuth2 Refresh Token", true),
  PWD("pwd", "Password (used when AUTH_MECH = 3)", true),
  POLL_INTERVAL("asyncexecpollinterval", "Async execution poll interval", "200", false),
  HTTP_PATH("httppath", "HTTP path", false),
  SSL("ssl", "Use SSL", false),
  USE_THRIFT_CLIENT("usethriftclient", "Use Thrift client", false),
  RATE_LIMIT_RETRY_TIMEOUT("RateLimitRetryTimeout", "Rate limit retry timeout", "120", false),
  JWT_KEY_FILE("Auth_JWT_Key_File", "JWT key file", true),
  JWT_ALGORITHM("Auth_JWT_Alg", "JWT algorithm", true),
  JWT_PASS_PHRASE("Auth_JWT_Key_Passphrase", "JWT key passphrase", true),
  JWT_KID("Auth_KID", "JWT key ID", true),
  USE_JWT_ASSERTION("UseJWTAssertion", "Use JWT assertion", "0", false),
  DISCOVERY_MODE("OAuthDiscoveryMode", "OAuth discovery mode", "1", false),
  AUTH_SCOPE("Auth_Scope", "Authentication scope", "all-apis", false),
  DISCOVERY_URL("OAuthDiscoveryURL", "OAuth discovery URL", false),
  ENABLE_ARROW("EnableArrow", "Enable Arrow", "1", false),
  DIRECT_RESULT("EnableDirectResults", "Enable direct results", "1", false),
  LZ4_COMPRESSION_FLAG(
      "EnableQueryResultLZ4Compression", "Enable LZ4 compression", false), // Backward compatibility
  COMPRESSION_FLAG("QueryResultCompressionType", "Query result compression type", false),
  USER_AGENT_ENTRY("useragententry", "User agent entry", false),
  USE_EMPTY_METADATA("useemptymetadata", "Use empty metadata", false),
  TEMPORARILY_UNAVAILABLE_RETRY(
      "TemporarilyUnavailableRetry", "Retry on temporarily unavailable", "1", false),
  TEMPORARILY_UNAVAILABLE_RETRY_TIMEOUT(
      "TemporarilyUnavailableRetryTimeout",
      "Retry timeout for temporarily unavailable",
      "900",
      false),
  RATE_LIMIT_RETRY("RateLimitRetry", "Retry on rate limit", "1", false),
  IDLE_HTTP_CONNECTION_EXPIRY(
      "IdleHttpConnectionExpiry", "Idle HTTP connection expiry", "60", false),
  SUPPORT_MANY_PARAMETERS("supportManyParameters", "Support many parameters", "0", false),
  CLOUD_FETCH_THREAD_POOL_SIZE(
      "cloudFetchThreadPoolSize", "Cloud fetch thread pool size", "16", false),
  TOKEN_ENDPOINT("OAuth2TokenEndpoint", "OAuth2 token endpoint", false),
  AUTH_ENDPOINT("OAuth2AuthorizationEndPoint", "OAuth2 authorization endpoint", false),
  SSL_TRUST_STORE("SSLTrustStore", "SSL trust store", false),
  SSL_TRUST_STORE_PROVIDER("SSLTrustStoreProvider", "SSL trust store provider", false),
  SSL_TRUST_STORE_PASSWORD("SSLTrustStorePwd", "SSL trust store password", false),
  SSL_TRUST_STORE_TYPE("SSLTrustStoreType", "SSL trust store type", "JKS", false),
  CHECK_CERTIFICATE_REVOCATION(
      "CheckCertificateRevocation", "Check certificate revocation", "1", false),
  ACCEPT_UNDETERMINED_CERTIFICATE_REVOCATION(
      "AcceptUndeterminedRevocation", "Accept undetermined revocation", "0", false),
  MAX_BATCH_SIZE("MaxBatchSize", "Maximum batch size", "500", false);

  private final String paramName;
  private final String defaultValue;
  private final String description;
  private final boolean required;

  DatabricksJdbcUrlParams(String paramName, String description, boolean required) {
    this.paramName = paramName;
    this.defaultValue = null;
    this.description = description;
    this.required = required;
  }

  DatabricksJdbcUrlParams(
      String paramName, String description, String defaultValue, boolean required) {
    this.paramName = paramName;
    this.defaultValue = defaultValue;
    this.description = description;
    this.required = required;
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
