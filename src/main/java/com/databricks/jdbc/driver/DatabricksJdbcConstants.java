package com.databricks.jdbc.driver;

import java.util.regex.Pattern;

public final class DatabricksJdbcConstants {

  static final Pattern JDBC_URL_PATTERN =
      Pattern.compile("jdbc:databricks:\\/\\/([^/]*)(?::\\d+)?\\/(.*)");
  static final Pattern HTTP_PATH_PATTERN = Pattern.compile(".*\\/warehouses\\/(.*)");
  static final Pattern HTTP_PATH_SQL_PATTERN = Pattern.compile("sql\\/(.*)");
  static final String JDBC_SCHEMA = "jdbc:databricks://";
  static final String URL_DELIMITER = ";";
  static final String PORT_DELIMITER = ":";
  static final String PAIR_DELIMITER = "=";
  static final String TOKEN = "token";
  static final String PASSWORD = "password";
  static final String HTTP_PATH = "httpPath";
  static final String HTTPS_SCHEMA = "https://";

  public static final String FULL_STOP = ".";
  public static final String EMPTY_STRING = "";
  public static final String IDENTIFIER_QUOTE_STRING = "`";
  public static final String CATALOG = "catalog";
  public static final String PROCEDURE = "procedure";
  public static final String SCHEMA = "schema";
  public static final String USER_NAME = "User";
  static final int DEFAULT_PORT = 443;
}
