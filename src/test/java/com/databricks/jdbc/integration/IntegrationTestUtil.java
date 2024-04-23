package com.databricks.jdbc.integration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

/** Utility class to support integration tests * */
public class IntegrationTestUtil {

  private static Connection JDBCConnection;

  public static String getDatabricksHost() {
    // includes port
    return System.getenv("DATABRICKS_HOST");
  }

  public static String getDatabricksBenchfoodHost() {
    // includes port
    return System.getenv("DATABRICKS_BENCHFOOD_HOST");
  }

  public static String getDatabricksToken() {
    return System.getenv("DATABRICKS_TOKEN");
  }

  public static String getDatabricksBenchfoodToken() {
    return System.getenv("DATABRICKS_BENCHFOOD_TOKEN");
  }

  public static String getDatabricksHTTPPath() {
    return System.getenv("DATABRICKS_HTTP_PATH");
  }

  public static String getDatabricksBenchfoodHTTPPath() {
    return System.getenv("DATABRICKS_BENCHFOOD_HTTP_PATH");
  }

  public static String getDatabricksCatalog() {
    return System.getenv("DATABRICKS_CATALOG");
  }

  public static String getDatabricksSchema() {
    return System.getenv("DATABRICKS_SCHEMA");
  }

  public static String getDatabricksUser() {
    return System.getenv("DATABRICKS_USER");
  }

  public static Connection getValidJDBCConnection() throws SQLException {
    // add support for properties
    return DriverManager.getConnection(getJDBCUrl(), getDatabricksUser(), getDatabricksToken());
  }

  public static Connection getBenchfoodJDBCConnection() throws SQLException {
    // add support for properties
    return DriverManager.getConnection(
        getBenchfoodJDBCUrl(), getDatabricksUser(), getDatabricksBenchfoodToken());
  }

  public static String getJDBCUrl() {
    String template = "jdbc:databricks://%s/default;ssl=1;AuthMech=3;httpPath=%s";
    String host = getDatabricksHost();
    String httpPath = getDatabricksHTTPPath();

    return String.format(template, host, httpPath);
  }

  public static String getBenchfoodJDBCUrl() {
    String template =
        "jdbc:databricks://%s/default;transportMode=http;ssl=1;AuthMech=3;httpPath=%s";
    String host = getDatabricksBenchfoodHost();
    String httpPath = getDatabricksBenchfoodHTTPPath();

    return String.format(template, host, httpPath);
  }

  public static boolean executeSQL(String sql) {
    try {
      if (JDBCConnection == null) JDBCConnection = getValidJDBCConnection();
      JDBCConnection.createStatement().execute(sql);
      return true;
    } catch (SQLException e) {
      System.out.println("Error executing SQL: " + e.getMessage());
      return false;
    }
  }

  public static ResultSet executeQuery(String sql) {
    try {
      if (JDBCConnection == null) JDBCConnection = getValidJDBCConnection();
      ResultSet rs = JDBCConnection.createStatement().executeQuery(sql);
      return rs;
    } catch (SQLException e) {
      System.out.println("Error executing SQL: " + e.getMessage());
      return null;
    }
  }

  public static void setupDatabaseTable(String tableName) {
    String tableDeletionSQL = "DROP TABLE IF EXISTS " + getFullyQualifiedTableName(tableName);

    executeSQL(tableDeletionSQL);

    String tableCreationSQL =
        "CREATE TABLE IF NOT EXISTS "
            + getFullyQualifiedTableName(tableName)
            + " (id INT PRIMARY KEY, col1 VARCHAR(255), col2 VARCHAR(255))";

    executeSQL(tableCreationSQL);
  }

  public static void setupDatabaseTable(String tableName, String tableCreationSQL) {
    String tableDeletionSQL = "DROP TABLE IF EXISTS " + getFullyQualifiedTableName(tableName);

    executeSQL(tableDeletionSQL);
    executeSQL(tableCreationSQL);
  }

  public static void deleteTable(String tableName) {
    String SQL = "DROP TABLE IF EXISTS " + getFullyQualifiedTableName(tableName);
    executeSQL(SQL);
  }

  public static String getFullyQualifiedTableName(String tableName) {
    return getDatabricksCatalog() + "." + getDatabricksSchema() + "." + tableName;
  }
}
