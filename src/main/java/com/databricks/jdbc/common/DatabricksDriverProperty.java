package com.databricks.jdbc.common;

import java.sql.DriverPropertyInfo;

public enum DatabricksDriverProperty {
  LOG_LEVEL("loglevel", "Log level for JDBC driver logging", false),
  LOG_PATH("logpath", "Path to log files. Picks up default path if not provided", false),
  LOG_FILE_SIZE("LogFileSize", "Maximum size for log file in MB", false),
  LOG_FILE_COUNT("LogFileCount", "Maximum number of log files", false),
  USER("user", "Databricks username", true),
  PASSWORD("password", "Databricks access token", true),
  CLIENT_ID("OAuth2ClientId", "Databricks Service Principal client ID", false),
  CLIENT_SECRET(
      "OAuth2Secret",
      "Databricks Service Principal client secret",
      false), // required when authmech=11 and auth_flow!=0
  AUTH_MECH("authmech", "Authentication mechanism", true),
  AUTH_ACCESS_TOKEN("Auth_AccessToken", "Access token for OAuth U2M", false),
  CONN_CATALOG("conncatalog", "Default catalog for the connection", false),
  CONN_SCHEMA("connschema", "Default schema for the connection", false),
  PROXY_HOST("proxyhost", "Proxy host address", false), // if useproxy is true
  PROXY_PORT("proxyport", "Proxy port", false), // if useproxy is true
  PROXY_USER("proxyuid", "Proxy username", false), // if useproxy is true
  PROXY_PWD("proxypwd", "Proxy password", false), // if useproxy is true
  USE_PROXY("useproxy", "Enable or disable proxy", false),
  PROXY_AUTH("proxyauth", "Proxy authentication method", false), // if useproxy is true
  NON_PROXY_HOSTS("proxyignorelist", "List of hosts to bypass proxy", false),
  USE_SYSTEM_PROXY("usesystemproxy", "Use system proxy settings", false),
  USE_CF_PROXY("usecfproxy", "Enable Cloudflare proxy", false),
  CF_PROXY_HOST("cfproxyhost", "Cloudflare proxy host", false), // if usecfproxy is true
  CF_PROXY_PORT("cfproxyport", "Cloudflare proxy port", false), // if usecfproxy is true
  CF_PROXY_AUTH("cfproxyauth", "Cloudflare proxy auth method", false), // if usecfproxy is true
  CF_PROXY_USER("cfproxyuid", "Cloudflare proxy username", false), // if usecfproxy is true
  CF_PROXY_PWD("cfproxypwd", "Cloudflare proxy password", false), // if usecfproxy is true
  AUTH_FLOW("auth_flow", "Type of OAuth authentication flow", false), // if authmech=11
  CATALOG("catalog", "Default catalog to connect to", false),
  SCHEMA("schema", "Default schema to connect to", false),
  OAUTH_REFRESH_TOKEN(
      "OAuthRefreshToken",
      "OAuth refresh token",
      false), // if authmech=11, authflow=0 and you want to use refresh token
  PWD("pwd", "Password for PAT auth when AUTH_MECH is 3", false),
  POLL_INTERVAL("asyncexecpollinterval", "Polling interval for async execution", false),
  HTTP_PATH("httppath", "HTTP path for the databricks cluster or warehouse", true),
  SSL("ssl", "Enable SSL connection", false),
  USE_THRIFT_CLIENT("usethriftclient", "Enable Thrift client usage", false),
  RATE_LIMIT_RETRY_TIMEOUT("RateLimitRetryTimeout", "Timeout for rate limit retry", false),
  JWT_KEY_FILE("Auth_JWT_Key_File", "Path to JWT key file", false), // if usejwtassertion is true
  JWT_ALGORITHM("Auth_JWT_Alg", "Algorithm for JWT", false), // if usejwtassertion is true
  JWT_PASS_PHRASE(
      "Auth_JWT_Key_Passphrase", "Passphrase for JWT key", false), // if usejwtassertion is true
  JWT_KID("Auth_KID", "Key ID for JWT", false), // if usejwtassertion is true
  USE_JWT_ASSERTION("usejwtassertion", "Enable JWT assertion", false),
  DISCOVERY_MODE("OAuthDiscoveryMode", "Mode for OAuth discovery", false),
  AUTH_SCOPE("Auth_Scope", "Scope for OAuth authentication", false), // if usejwtassertion is true
  DISCOVERY_URL("OAuthDiscoveryURL", "URL for OAuth discovery", false),
  ENABLE_ARROW("EnableArrow", "Enable Arrow format for results", false),
  DIRECT_RESULT("EnableDirectResults", "Enable direct results fetching", false),
  LZ4_COMPRESSION_FLAG("EnableQueryResultLZ4Compression", "Enable LZ4 compression", false),
  COMPRESSION_FLAG("QueryResultCompressionType", "Compression type for query results", false),
  USER_AGENT_ENTRY("useragententry", "User agent entry", false),
  USE_EMPTY_METADATA("useemptymetadata", "Enable empty metadata", false),
  TEMPORARILY_UNAVAILABLE_RETRY(
      "TemporarilyUnavailableRetry", "Retry if temporarily unavailable", false),
  TEMPORARILY_UNAVAILABLE_RETRY_TIMEOUT(
      "TemporarilyUnavailableRetryTimeout", "Timeout for temporary retry", false),
  RATE_LIMIT_RETRY("RateLimitRetry", "Enable rate limit retry", false),
  IDLE_HTTP_CONNECTION_EXPIRY(
      "IdleHttpConnectionExpiry", "Expiry time for idle HTTP connections", false),
  SUPPORT_MANY_PARAMETERS("supportManyParameters", "Support for many parameters", false),
  CLOUD_FETCH_THREAD_POOL_SIZE(
      "cloudFetchThreadPoolSize", "Size of thread pool for cloud fetch", false),
  TOKEN_ENDPOINT(
      "OAuth2TokenEndpoint",
      "Token endpoint for OAuth",
      false), // if authmch=11, authflow=0 and you want to use refresh token
  SSL_TRUST_STORE("SSLTrustStore", "Path to SSL trust store", false),
  SSL_TRUST_STORE_PROVIDER("SSLTrustStoreProvider", "Provider for SSL trust store", false),
  SSL_TRUST_STORE_PASSWORD("SSLTrustStorePwd", "Password for SSL trust store", false),
  SSL_TRUST_STORE_TYPE("SSLTrustStoreType", "Type of SSL trust store", false),
  CHECK_CERTIFICATE_REVOCATION(
      "CheckCertificateRevocation", "Enable certificate revocation check", false),
  ACCEPT_UNDETERMINED_REVOCATION(
      "AcceptUndeterminedRevocation", "Accept undetermined certificate revocation", false),
  MAX_BATCH_SIZE("MaxBatchSize", "Maximum batch size for execution", false);

  private final String name;
  private final String description;
  private final boolean required;

  DatabricksDriverProperty(String name, String description, boolean required) {
    this.name = name;
    this.description = description;
    this.required = required;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public boolean isRequired() {
    return required;
  }

  public static DriverPropertyInfo getDriverPropertyInfo(
      DatabricksDriverProperty property, boolean required) {
    DriverPropertyInfo info = new DriverPropertyInfo(property.getName(), null);
    info.required = required;
    info.description = property.getDescription();
    return info;
  }
}
