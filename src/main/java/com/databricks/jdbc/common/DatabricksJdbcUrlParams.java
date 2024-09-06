package com.databricks.jdbc.common;

/** Class to hold all the Databricks JDBC URL parameters. */
public class DatabricksJdbcUrlParams {
  public static final String LOG_LEVEL = "loglevel";
  public static final String LOG_PATH = "logpath";
  public static final String LOG_FILE_SIZE = "LogFileSize";
  public static final String LOG_FILE_COUNT = "LogFileCount";
  public static final String USER = "user";
  public static final String PASSWORD = "password";
  public static final String CLIENT_ID = "OAuth2ClientId";
  public static final String CLIENT_SECRET = "OAuth2Secret";
  public static final String AUTH_MECH = "authmech";
  public static final String CONN_CATALOG = "conncatalog";
  public static final String CONN_SCHEMA = "connschema";
  public static final String PROXY_HOST = "proxyhost";
  public static final String PROXY_PORT = "proxyport";
  public static final String PROXY_USER = "proxyuid";
  public static final String PROXY_PWD = "proxypwd";
  public static final String USE_PROXY = "useproxy";
  public static final String PROXY_AUTH = "proxyauth";
  public static final String USE_SYSTEM_PROXY = "usesystemproxy";
  public static final String USE_CF_PROXY = "usecfproxy";
  public static final String CF_PROXY_HOST = "cfproxyhost";
  public static final String CF_PROXY_PORT = "cfproxyport";
  public static final String CF_PROXY_AUTH = "cfproxyauth";
  public static final String CF_PROXY_USER = "cfproxyuid";
  public static final String CF_PROXY_PWD = "cfproxypwd";
  public static final String AUTH_FLOW = "auth_flow";
  public static final String OAUTH_REFRESH_TOKEN = "OAuthRefreshToken";

  /** Only used when AUTH_MECH = 3 */
  public static final String PWD = "pwd";

  public static final String POLL_INTERVAL = "asyncexecpollinterval";
  public static final String HTTP_PATH = "httppath";
  public static final String SSL = "ssl";
  public static final String USE_THRIFT_CLIENT = "usethriftclient";
  public static final String RATE_LIMIT_RETRY_TIMEOUT = "RateLimitRetryTimeout";
  public static final String JWT_KEY_FILE = "Auth_JWT_Key_File";
  public static final String JWT_ALGORITHM = "Auth_JWT_Alg";
  public static final String JWT_PASS_PHRASE = "Auth_JWT_Key_Passphrase";
  public static final String JWT_KID = "Auth_KID";
  public static final String USE_JWT_ASSERTION = "UseJWTAssertion";
  public static final String DISCOVERY_MODE = "OAuthDiscoveryMode";
  public static final String AUTH_SCOPE = "Auth_Scope";
  public static final String DISCOVERY_URL = "OAuthDiscoveryURL";
  public static final String ENABLE_ARROW = "EnableArrow";
  public static final String DIRECT_RESULT = "EnableDirectResults";

  /** Adding this for backward compatibility only */
  public static final String LZ4_COMPRESSION_FLAG = "EnableQueryResultLZ4Compression";

  public static final String COMPRESSION_FLAG = "QueryResultCompressionType";
  public static final String USER_AGENT_ENTRY = "useragententry";
  public static final String USE_LEGACY_METADATA = "uselegacymetadata";
  public static final String TEMPORARILY_UNAVAILABLE_RETRY = "TemporarilyUnavailableRetry";
  public static final String TEMPORARILY_UNAVAILABLE_RETRY_TIMEOUT =
      "TemporarilyUnavailableRetryTimeout";
  public static final String RATE_LIMIT_RETRY = "RateLimitRetry";
  public static final String IDLE_HTTP_CONNECTION_EXPIRY = "IdleHttpConnectionExpiry";
  public static final String SUPPORT_MANY_PARAMETERS = "supportManyParameters";
  public static final String CLOUD_FETCH_THREAD_POOL_SIZE = "cloudFetchThreadPoolSize";
  public static final String ENABLE_TELEMETRY = "enableTelemetry";
  public static final String TOKEN_ENDPOINT = "OAuth2TokenEndpoint";
  public static final String AUTH_ENDPOINT = "OAuth2AuthorizationEndPoint";
}
