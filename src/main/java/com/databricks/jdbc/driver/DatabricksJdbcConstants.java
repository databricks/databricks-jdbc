package com.databricks.jdbc.driver;

import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public final class DatabricksJdbcConstants {

  static final Pattern JDBC_URL_PATTERN =
      Pattern.compile("jdbc:databricks://([^/;]*)(?::\\d+)?/*(.*)");
  static final Pattern HTTP_WAREHOUSE_PATH_PATTERN = Pattern.compile(".*/warehouses/(.+)");
  static final Pattern TEST_PATH_PATTERN = Pattern.compile("jdbc:databricks://test");
  static final Pattern HTTP_CLUSTER_PATH_PATTERN = Pattern.compile(".*/o/(.+)/(.+)");
  public static final String JDBC_SCHEMA = "jdbc:databricks://";
  static final String DEFAULT_LOG_LEVEL = "INFO";
  static final String LOG_LEVEL = "loglevel";
  static final String LOG_PATH = "logpath";
  static final String SYSTEM_LOG_LEVEL_CONFIG = "defaultLogLevel";
  static final String SYSTEM_LOG_FILE_CONFIG = "defaultLogFile";
  public static final String URL_DELIMITER = ";";
  public static final String PORT_DELIMITER = ":";
  static final String DEFAULT_SCHEMA = "default";
  static final String DEFAULT_CATALOG = "SPARK";
  public static final String PAIR_DELIMITER = "=";
  static final String TOKEN = "token";
  public static final String USER = "user";
  public static final String PASSWORD = "password";

  static final String CLIENT_ID = "databricks_client_id";

  static final String CLIENT_SECRET = "databricks_client_secret";

  public static final String AUTH_MECH = "authmech";

  static final String CONN_CATALOG = "conncatalog";

  static final String CONN_SCHEMA = "connschema";

  static final String PROXY_HOST = "proxyhost";
  static final String PROXY_PORT = "proxyport";
  static final String PROXY_USER = "proxyuid";
  static final String PROXY_PWD = "proxypwd";
  static final String USE_PROXY = "useproxy";
  static final String USE_PROXY_AUTH = "proxyauth";
  static final String USE_SYSTEM_PROXY = "usesystemproxy";
  static final String USE_CF_PROXY = "usecfproxy";
  static final String CF_PROXY_HOST = "cfproxyhost";
  static final String CF_PROXY_PORT = "cfproxyport";
  static final String USE_CF_PROXY_AUTH = "cfproxyauth";
  static final String CF_PROXY_USER = "cfproxyuid";
  static final String CF_PROXY_PWD = "cfproxypwd";

  static final String AUTH_FLOW = "auth_flow";

  // Only used when AUTH_MECH = 3
  static final String UID = "uid";
  static final String PWD = "pwd";

  static final String AWS_CLIENT_ID = "databricks-sql-jdbc";

  static final String AAD_CLIENT_ID = "96eecda7-19ea-49cc-abb5-240097d554f5";

  public static final String HTTP_PATH = "httppath";
  static final String HTTPS_SCHEMA = "https://";
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
  static final int DEFAULT_PORT = 443;

  static final String LZ4_COMPRESSION_FLAG =
      "EnableQueryResultLZ4Compression"; // Adding this for backward compatibility only
  static final String COMPRESSION_FLAG = "QueryResultCompressionType";
  static final String USER_AGENT_ENTRY = "useragententry";
  public static final String DEFAULT_USER_AGENT = "DatabricksDatabricksJDBCDriverOSS";
  static final String CLIENT_USER_AGENT_PREFIX = "Java";
  static final String USER_AGENT_SEA_CLIENT = "SQLExecHttpClient/HC";
  static final String USER_AGENT_THRIFT_CLIENT = "THttpClient/HC";
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
}
