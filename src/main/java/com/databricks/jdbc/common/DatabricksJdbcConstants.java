package com.databricks.jdbc.common;

import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public final class DatabricksJdbcConstants {
  public static final Pattern JDBC_URL_PATTERN =
      Pattern.compile(
          "jdbc:databricks://"
              + // Protocol prefix
              "([^/;]+)"
              + // Host[:Port] (captured)
              "(?:/([^;]*))?"
              + // Optional Schema (captured without /)
              "(?:;(.*))?"); // Optional Property=Value pairs (captured without leading ;)
  public static final Pattern HTTP_WAREHOUSE_PATH_PATTERN = Pattern.compile(".*/warehouses/(.+)");
  public static final Pattern HTTP_ENDPOINT_PATH_PATTERN = Pattern.compile(".*/endpoints/(.+)");
  public static final Pattern HTTP_CLI_PATTERN = Pattern.compile(".*cliservice(.+)");
  public static final Pattern HTTP_PATH_CLI_PATTERN = Pattern.compile("cliservice");
  public static final Pattern TEST_PATH_PATTERN = Pattern.compile("jdbc:databricks://test");
  public static final Pattern BASE_PATTERN = Pattern.compile("jdbc:databricks://[^;]+(;[^;]*)*");
  public static final Pattern HTTP_CLUSTER_PATH_PATTERN = Pattern.compile(".*/o/(.+)/(.+)");
  public static final String JDBC_SCHEMA = "jdbc:databricks://";
  public static final LogLevel DEFAULT_LOG_LEVEL = LogLevel.OFF;
  public static final String USER_AGENT_DELIMITER = " ";
  public static final String URL_DELIMITER = ";";
  public static final String PORT_DELIMITER = ":";
  public static final String DEFAULT_SCHEMA = "default";
  public static final String PAIR_DELIMITER = "=";
  public static final String SCHEMA_DELIMITER = "://";
  public static final String PKIX = "PKIX";
  public static final String TLS = "TLS";
  public static final String HTTP = "http";
  public static final String HTTPS = "https";
  public static final String HTTP_SCHEMA = HTTP + SCHEMA_DELIMITER;
  public static final String HTTPS_SCHEMA = HTTPS + SCHEMA_DELIMITER;
  public static final String LOGIN_TIMEOUT = "loginTimeout";
  public static final String U2M_AUTH_TYPE = "external-browser";
  public static final String M2M_AUTH_TYPE = "oauth-m2m";
  public static final String AZURE_MSI_AUTH_TYPE = "azure-msi";
  public static final String M2M_AZURE_CLIENT_SECRET_AUTH_TYPE = "azure-client-secret";
  public static final String ACCESS_TOKEN_AUTH_TYPE = "pat";
  public static final String VALID_UID_VALUE = "token";
  public static final String SQL_SCOPE = "sql";
  public static final String OFFLINE_ACCESS_SCOPE = "offline_access";
  public static final String FULL_STOP = ".";
  public static final String COMMA = ",";
  public static final String PIPE = "|";
  public static final String ASTERISK = "*";
  public static final String EMPTY_STRING = "";
  public static final String IDENTIFIER_QUOTE_STRING = "`";
  public static final String BACKWARD_SLASH = "\\";
  public static final String CATALOG = "catalog";
  public static final String PROCEDURE = "procedure";
  public static final String SCHEMA = "schema";
  public static final String TABLE = "table";
  public static final String USER_NAME = "User";
  public static final String PORT = "port";
  public static final int DEFAULT_PORT = 443;
  public static final String THRIFT_ERROR_MESSAGE_HEADER = "X-Thriftserver-Error-Message";
  public static final String ALLOWED_VOLUME_INGESTION_PATHS = "VolumeOperationAllowedLocalPaths";
  public static final String ENABLE_VOLUME_OPERATIONS = "enableVolumeOperations";
  public static final String ALLOWED_STAGING_INGESTION_PATHS = "StagingAllowedLocalPaths";
  public static final String VOLUME_OPERATION_STATUS_COLUMN_NAME = "operation_status";
  public static final String VOLUME_OPERATION_STATUS_SUCCEEDED = "SUCCEEDED";
  public static final int VOLUME_OPERATION_MAX_RETRIES = 3;
  public static final int UUID_LENGTH = 16;

  public static final String ARROW_METADATA_KEY = "Spark:DataType:SqlName";
  public static final Map<String, String> ALLOWED_SESSION_CONF_TO_DEFAULT_VALUES_MAP =
      // This map comes from
      // https://docs.databricks.com/en/sql/language-manual/sql-ref-parameters.html
      Map.of(
          "ANSI_MODE", "true",
          "ENABLE_PHOTON", "true",
          "LEGACY_TIME_PARSER_POLICY", "Exception",
          "MAX_FILE_PARTITION_BYTES", "128m",
          "READ_ONLY_EXTERNAL_METASTORE", "false",
          "STATEMENT_TIMEOUT", "0",
          "TIMEZONE", "UTC",
          "USE_CACHED_RESULT", "true",
          "QUERY_TAGS", "");
  public static final Set<String> ALLOWED_CLIENT_INFO_PROPERTIES =
      Set.of(
          ALLOWED_VOLUME_INGESTION_PATHS,
          ENABLE_VOLUME_OPERATIONS,
          ALLOWED_STAGING_INGESTION_PATHS,
          DatabricksJdbcUrlParams.AUTH_ACCESS_TOKEN.getParamName(),
          DatabricksJdbcUrlParams.APPLICATION_NAME.getParamName());
  public static final Map<String, String> JSON_HTTP_HEADERS =
      Map.of(
          "Accept", "application/json",
          "Content-Type", "application/json");
  @VisibleForTesting public static final String IS_FAKE_SERVICE_TEST_PROP = "isFakeServiceTest";
  @VisibleForTesting public static final String FAKE_SERVICE_URI_PROP_SUFFIX = ".fakeServiceURI";
  public static final String IS_JDBC_TEST_ENV = "IS_JDBC_TEST_ENV";
  public static final String AWS_CLIENT_ID = "databricks-sql-jdbc";
  public static final String GCP_CLIENT_ID = "databricks-sql-jdbc";
  public static final String AAD_CLIENT_ID = "databricks-sql-jdbc";
  public static final String GCP_GOOGLE_CREDENTIALS_AUTH_TYPE = "google-credentials";
  public static final String GCP_GOOGLE_ID_AUTH_TYPE = "google-id";
  public static final String DEFAULT_HTTP_EXCEPTION_SQLSTATE = "08000";
  public static final int TEMPORARY_REDIRECT_STATUS_CODE = 307;
  public static final String REDACTED_TOKEN = "****";
  public static final int MAX_DEFAULT_STRING_COLUMN_LENGTH = 32767;
  public static final int DEFUALT_STRING_COLUMN_LENGTH = 255;
  public static final int DEFAULT_MAX_CONCURRENT_PRESIGNED_REQUESTS = 50;

  /** Default retryable HTTP codes for UC Volume operations. */
  public static final List<Integer> DEFAULT_UC_INGESTION_RETRYABLE_HTTP_CODES =
      Arrays.asList(408, 429, 500, 502, 503, 504);

  /** Default retry timeout in seconds for UC Volume operations. */
  public static final int DEFAULT_UC_INGESTION_RETRY_TIMEOUT_SECONDS = 900; // 15 minutes

  public static final String INVALID_SESSION_STATE_MSG = "invalid session";

  /** Enum for the services that can be replaced with a fake service in integration tests. */
  @VisibleForTesting
  public enum FakeServiceType {
    SQL_EXEC,
    CLOUD_FETCH,
    SQL_GATEWAY,
    THRIFT_SERVER,
    CLOUD_FETCH_SQL_GATEWAY,
    CLOUD_FETCH_THRIFT_SERVER,
    CLOUD_FETCH_UC_VOLUME,
    JWT_TOKEN_ENDPOINT
  }

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
  public static final Pattern PUT_PATTERN =
      Pattern.compile("^(\\s*\\()*\\s*PUT", Pattern.CASE_INSENSITIVE);
  public static final Pattern GET_PATTERN =
      Pattern.compile("^(\\s*\\()*\\s*GET", Pattern.CASE_INSENSITIVE);
  public static final Pattern REMOVE_PATTERN =
      Pattern.compile("^(\\s*\\()*\\s*REMOVE", Pattern.CASE_INSENSITIVE);
  public static final Pattern LIST_PATTERN =
      Pattern.compile("^(\\s*\\()*\\s*LIST", Pattern.CASE_INSENSITIVE);
  public static final Pattern INSERT_PATTERN =
      Pattern.compile("^(\\s*\\()*\\s*INSERT\\s+INTO", Pattern.CASE_INSENSITIVE);
  public static final Pattern CALL_PATTERN =
      Pattern.compile("^(\\s*\\()*\\s*CALL", Pattern.CASE_INSENSITIVE);

  /** Maximum number of parameters allowed in a single Databricks query */
  public static final int MAX_QUERY_PARAMETERS = 256;

  // Regex: match queries starting with "BEGIN" but not followed by "TRANSACTION"
  // (?i)         -> case-insensitive
  // ^\s*BEGIN    -> string starts with BEGIN (allow leading whitespace)
  // (?!\s*TRANSACTION\b) -> negative lookahead: not followed by optional spaces + "TRANSACTION"
  public static final Pattern BEGIN_PATTERN_FOR_SQL_SCRIPT =
      Pattern.compile("(?i)^\\s*BEGIN(?!\\s*TRANSACTION\\b)");
  public static final String DEFAULT_USERNAME =
      "token"; // This is for PAT. We do not support Basic Auth.
  public static final int DEFAULT_MAX_HTTP_CONNECTIONS_PER_ROUTE = 1000;
}
