package com.databricks.jdbc.driver;

import java.util.regex.Pattern;

final class DatabricksJdbcConstants {

  static final Pattern JDBC_URL_PATTERN = Pattern.compile("jdbc:databricks:\\/\\/([^/]*)(?::\\d+)?\\/(.*)");
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

  static final int DEFAULT_PORT = 443;
}
