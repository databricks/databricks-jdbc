package com.databricks.jdbc.driver;

import java.util.regex.Pattern;

public final class DatabricksJdbcConstants {

  static final Pattern JDBC_URL_PATTERN = Pattern.compile("jdbc:databricks:\\/\\/([^/]*)(?::\\d+)?\\/(.*)");
  static final Pattern HTTP_PATH_PATTERN = Pattern.compile(".*\\/warehouses\\/(.*)");
  static final Pattern HTTP_PATH_SQL_PATTERN = Pattern.compile("sql\\/(.*)");
  public static final String JDBC_SCHEMA = "jdbc:databricks://";
  static final String URL_DELIMITER = ";";
  public static final String PORT_DELIMITER = ":";
  public static final String PAIR_DELIMITER = "=";
  public static final String SLASH = "/";
  static final String TOKEN = "token";
  public static final String USER = "user";
  public static final String PASSWORD = "password";
  public static final String LOCALHOST = "localhost";
  static final String HTTP_PATH = "httpPath";
  public static final String HTTPS_SCHEMA = "https";
  public static final String HTTP_SCHEMA = "http";
  public static final String LOGIN_TIMEOUT = "loginTimeout";

  public static final String FULL_STOP = ".";
  public static final String EMPTY_STRING = "";
  public static final String IDENTIFIER_QUOTE_STRING = "`";
  public static final String CATALOG = "catalog";
  public static final String PROCEDURE = "procedure";
  public static final String SCHEMA = "schema";
  public static final String USER_NAME = "User";
  static final int DEFAULT_PORT = 443;
}
