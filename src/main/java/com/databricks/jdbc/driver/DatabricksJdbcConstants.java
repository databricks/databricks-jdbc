package com.databricks.jdbc.driver;

import com.databricks.jdbc.commons.LogLevel;
import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public final class DatabricksJdbcConstants {

  public static final Pattern JDBC_URL_PATTERN =
      Pattern.compile("jdbc:databricks://([^/;]*)(?::\\d+)?/*(.*)");

  public static final Pattern HTTP_WAREHOUSE_PATH_PATTERN = Pattern.compile(".*/warehouses/(.+)");

  public static final Pattern HTTP_ENDPOINT_PATH_PATTERN = Pattern.compile(".*/endpoints/(.+)");

  public static final Pattern HTTP_CLI_PATTERN = Pattern.compile(".*cliservice(.+)");

  public static final Pattern HTTP_PATH_CLI_PATTERN = Pattern.compile("cliservice");

  public static final Pattern TEST_PATH_PATTERN = Pattern.compile("jdbc:databricks://test");

  public static final Pattern BASE_PATTERN = Pattern.compile("jdbc:databricks://[^;]+(;[^;]*)?");

  public static final Pattern HTTP_CLUSTER_PATH_PATTERN = Pattern.compile(".*/o/(.+)/(.+)");

  public static final String JDBC_SCHEMA = "jdbc:databricks://";

  public static final LogLevel DEFAULT_LOG_LEVEL = LogLevel.INFO;

  public static final String LOG_LEVEL = "loglevel";

  public static final String LOG_PATH = "logpath";

  public static final String DEFAULT_LOG_PATH = "logs/application.log";

  public static final String LOG_FILE_SIZE = "LogFileSize";

  public static final int DEFAULT_LOG_FILE_SIZE_IN_MB = 10;

  public static final String DEFAULT_LOG_PATTERN = "%d{yyyy-MM-dd HH:mm:ss} %p %c{1}:%L - %m%n";

  public static final String DEFAULT_FILE_LOG_PATTERN = "/%d{yyyy-MM-dd}-logfile-%i.log";

  public static final String DEFAULT_LOG_NAME_FILE = "logfile-0.log";

  public static final String LOG_FILE_COUNT = "LogFileCount";

  public static final int DEFAULT_LOG_FILE_COUNT = 10;

  public static final String URL_DELIMITER = ";";

  public static final String PORT_DELIMITER = ":";

  public static final String DEFAULT_SCHEMA = "default";

  public static final String DEFAULT_CATALOG = "hive_metastore";

  public static final String PAIR_DELIMITER = "=";

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

  public static final String USE_PROXY_AUTH = "proxyauth";

  public static final String USE_SYSTEM_PROXY = "usesystemproxy";

  public static final String USE_CF_PROXY = "usecfproxy";

  public static final String CF_PROXY_HOST = "cfproxyhost";

  public static final String CF_PROXY_PORT = "cfproxyport";

  public static final String USE_CF_PROXY_AUTH = "cfproxyauth";

  public static final String ENABLE_ARROW = "EnableArrow";

  public static final String CF_PROXY_USER = "cfproxyuid";

  public static final String CF_PROXY_PWD = "cfproxypwd";

  public static final String AUTH_FLOW = "auth_flow";

  /** Only used when AUTH_MECH = 3 */
  public static final String PWD = "pwd";

  public static final String POLL_INTERVAL = "asyncexecpollinterval";

  public static final int POLL_INTERVAL_DEFAULT = 200;

  public static final String AWS_CLIENT_ID = "databricks-sql-jdbc";

  public static final String AAD_CLIENT_ID = "96eecda7-19ea-49cc-abb5-240097d554f5";

  public static final String HTTP_PATH = "httppath";

  public static final String SSL = "ssl";

  public static final String HTTP_SCHEMA = "http://";

  public static final String HTTPS_SCHEMA = "https://";

  public static final String DIRECT_RESULT = "EnableDirectResults";

  public static final String LOGIN_TIMEOUT = "loginTimeout";

  public static final String U2M_AUTH_TYPE = "external-browser";

  public static final String M2M_AUTH_TYPE = "oauth-m2m";

  public static final String ACCESS_TOKEN_AUTH_TYPE = "pat";

  public static final String U2M_AUTH_REDIRECT_URL = "http://localhost:8020";

  public static final String SQL_SCOPE = "sql";

  public static final String OFFLINE_ACCESS_SCOPE = "offline_access";

  public static final String FULL_STOP = ".";

  public static final String EMPTY_STRING = "";

  public static final String IDENTIFIER_QUOTE_STRING = "`";

  public static final String CATALOG = "catalog";

  public static final String PROCEDURE = "procedure";

  public static final String SCHEMA = "schema";

  public static final String TABLE = "table";

  public static final String USER_NAME = "User";
  public static final String PORT = "port";
  public static final int DEFAULT_PORT = 443;

  /** Adding this for backward compatibility only */
  public static final String LZ4_COMPRESSION_FLAG = "EnableQueryResultLZ4Compression";

  public static final String COMPRESSION_FLAG = "QueryResultCompressionType";

  public static final String USER_AGENT_ENTRY = "useragententry";

  public static final String DEFAULT_USER_AGENT = "DatabricksJDBCDriverOSS";

  public static final String CLIENT_USER_AGENT_PREFIX = "Java";

  public static final String USER_AGENT_SEA_CLIENT = "SQLExecHttpClient/HC";

  public static final String USER_AGENT_THRIFT_CLIENT = "THttpClient/HC";

  public static final String ALLOWED_VOLUME_INGESTION_PATHS =
      "allowlistedVolumeOperationLocalFilePaths";

  public static final String VOLUME_OPERATION_STATUS_COLUMN_NAME = "operation_status";

  public static final Map<String, String> ALLOWED_SESSION_CONF_TO_DEFAULT_VALUES_MAP =
      // This map comes from
      // https://docs.databricks.com/en/sql/language-manual/sql-ref-parameters.html
      Map.of(
          "ANSI_MODE", "TRUE",
          "ENABLE_PHOTON", "TRUE",
          "LEGACY_TIME_PARSER_POLICY", "EXCEPTION",
          "MAX_FILE_PARTITION_BYTES", "128m",
          "READ_ONLY_EXTERNAL_METASTORE", "FALSE",
          "STATEMENT_TIMEOUT", "172800",
          "TIMEZONE", "UTC",
          "USE_CACHED_RESULT", "TRUE");

  public static final Set<String> ALLOWED_CLIENT_INFO_PROPERTIES =
      Set.of(ALLOWED_VOLUME_INGESTION_PATHS);

  @VisibleForTesting public static final String IS_FAKE_SERVICE_TEST_PROP = "isFakeServiceTest";

  @VisibleForTesting public static final String FAKE_SERVICE_URI_PROP_SUFFIX = ".fakeServiceURI";

  /** Enum for the services that can be replaced with a fake service in integration tests. */
  @VisibleForTesting
  public enum FakeServiceType {
    SQL_EXEC,
    CLOUD_FETCH,
    SQL_GATEWAY,
    CLOUD_FETCH_SQL_GATEWAY
  }

  public static final String USE_THRIFT_CLIENT = "usethriftclient";

  public static final String USE_LEGACY_METADATA = "uselegacymetadata";

  static final String TEMPORARILY_UNAVAILABLE_RETRY = "TemporarilyUnavailableRetry";
  static final String TEMPORARILY_UNAVAILABLE_RETRY_TIMEOUT = "TemporarilyUnavailableRetryTimeout";
  public static final String DEFAULT_TEMPORARILY_UNAVAILABLE_RETRY_TIMEOUT = "900";
  static final String RATE_LIMIT_RETRY = "RateLimitRetry";
  static final String RATE_LIMIT_RETRY_TIMEOUT = "RateLimitRetryTimeout";
  public static final String DEFAULT_RATE_LIMIT_RETRY_TIMEOUT = "120";
  static final String IDLE_HTTP_CONNECTION_EXPIRY = "IdleHttpConnectionExpiry";
  public static final String DEFAULT_IDLE_HTTP_CONNECTION_EXPIRY = "60";
  public static final String CLOUD_FETCH_THREAD_POOL_SIZE = "cloudFetchThreadPoolSize";
  public static final int CLOUD_FETCH_THREAD_POOL_SIZE_DEFAULT = 16;

  public static final Pattern SELECT_PATTERN =
      Pattern.compile("^(\\s*\\()*\\s*SELECT", Pattern.CASE_INSENSITIVE);
  public static final Pattern SHOW_PATTERN =
      Pattern.compile("^(\\s*\\()*\\s*SHOW", Pattern.CASE_INSENSITIVE);
  public static final Pattern DESCRIBE_PATTERN =
      Pattern.compile("^(\\s*\\()*\\s*DESCRIBE", Pattern.CASE_INSENSITIVE);
  public static final Pattern EXPLAIN_PATTERN =
      Pattern.compile("^(\\s*\\()*\\s*EXPLAIN", Pattern.CASE_INSENSITIVE);
  public static final Pattern WITH_PATTERN =
      Pattern.compile("^(\\s*\\()*\\s*WITH", Pattern.CASE_INSENSITIVE);
  public static final Pattern SET_PATTERN =
      Pattern.compile("^(\\s*\\()*\\s*SET", Pattern.CASE_INSENSITIVE);
  public static final Pattern MAP_PATTERN =
      Pattern.compile("^(\\s*\\()*\\s*MAP", Pattern.CASE_INSENSITIVE);
  public static final Pattern FROM_PATTERN =
      Pattern.compile("^(\\s*\\()*\\s*FROM\\s*\\(", Pattern.CASE_INSENSITIVE);
  public static final Pattern VALUES_PATTERN =
      Pattern.compile("^(\\s*\\()*\\s*VALUES", Pattern.CASE_INSENSITIVE);
  public static final Pattern UNION_PATTERN =
      Pattern.compile("\\s+UNION\\s+", Pattern.CASE_INSENSITIVE);
  public static final Pattern INTERSECT_PATTERN =
      Pattern.compile("\\s+INTERSECT\\s+", Pattern.CASE_INSENSITIVE);
  public static final Pattern EXCEPT_PATTERN =
      Pattern.compile("\\s+EXCEPT\\s+", Pattern.CASE_INSENSITIVE);
  public static final Pattern DECLARE_PATTERN =
      Pattern.compile("^(\\s*\\()*\\s*DECLARE", Pattern.CASE_INSENSITIVE);
  static final int DBSQL_MIN_MAJOR_VERSION_FOR_NEW_METADATA = 2024;
  static final int DBSQL_MIN_MINOR_VERSION_FOR_NEW_METADATA = 30;

  public static final int DEFAULT_RETRY_COUNT = 5;
}
