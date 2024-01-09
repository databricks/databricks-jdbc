package com.databricks.jdbc.driver;

import java.util.regex.Pattern;

public final class DatabricksJdbcConstants {

  static final Pattern JDBC_URL_PATTERN =
      Pattern.compile("jdbc:databricks:\\/\\/([^/]*)(?::\\d+)?\\/*(.*)");
  static final Pattern HTTP_PATH_PATTERN = Pattern.compile(".*\\/warehouses\\/(.*)");
  static final Pattern HTTP_PATH_SQL_PATTERN = Pattern.compile("sql\\/(.*)");
  public static final String JDBC_SCHEMA = "jdbc:databricks://";
  static final String DEFAULT_LOG_LEVEL = "INFO";
  static final String LOG_LEVEL = "loglevel";
  static final String LOG_PATH = "logpath";
  static final String SYSTEM_LOG_LEVEL_CONFIG = "defaultLogLevel";
  static final String SYSTEM_LOG_FILE_CONFIG = "defaultLogFile";
  static final String URL_DELIMITER = ";";
  static final String PORT_DELIMITER = ":";
  static final String PAIR_DELIMITER = "=";
  static final String TOKEN = "token";
  public static final String USER = "user";
  public static final String PASSWORD = "password";

  static final String CLIENT_ID = "databricks_client_id";

  static final String CLIENT_SECRET = "databricks_client_secret";

  static final String AUTH_MECH = "authmech";

  static final String AUTH_FLOW = "auth_flow";

  // Only used when AUTH_MECH = 11
  static final String AUTH_ACCESSTOKEN = "auth_accesstoken";

  // Only used when AUTH_MECH = 3
  static final String UID = "uid";
  static final String PWD = "pwd";

  static final String AWS_CLIENT_ID = "databricks-sql-jdbc";

  static final String AAD_CLIENT_ID = "96eecda7-19ea-49cc-abb5-240097d554f5";

  static final String HTTP_PATH = "httppath";
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
  public static final String USER_NAME = "User";
  static final int DEFAULT_PORT = 443;

  static final String USER_AGENT_ENTRY = "useragententry";
  static final String DEFAULT_USER_AGENT = "databricks_jdbc_oss";
}
