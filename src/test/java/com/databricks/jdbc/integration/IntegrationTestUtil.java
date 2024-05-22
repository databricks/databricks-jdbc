package com.databricks.jdbc.integration;

import static com.databricks.jdbc.driver.DatabricksJdbcConstants.FAKE_SERVICE_URI_PROP_SUFFIX;
import static com.databricks.jdbc.driver.DatabricksJdbcConstants.IS_FAKE_SERVICE_TEST_PROP;
import static com.databricks.jdbc.integration.fakeservice.FakeServiceExtension.TARGET_URI_PROP_SUFFIX;

import com.databricks.jdbc.driver.DatabricksJdbcConstants.FakeServiceType;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

/** Utility class to support integration tests * */
public class IntegrationTestUtil {

  private static Connection JDBCConnection;

  public static String getDatabricksHost() {
    if (Boolean.parseBoolean(System.getProperty(IS_FAKE_SERVICE_TEST_PROP))) {
      // Target base URL of the fake service type
      String serviceURI =
          System.getProperty(
              FakeServiceType.SQL_EXEC.name().toLowerCase() + TARGET_URI_PROP_SUFFIX);
      URI fakeServiceURI;
      try {
        // Fake service URL for the base URL
        fakeServiceURI = new URI(System.getProperty(serviceURI + FAKE_SERVICE_URI_PROP_SUFFIX));
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }

      return fakeServiceURI.getAuthority();
    }

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

  public static void resetJDBCConnection() {
    JDBCConnection = null;
  }

  public static String getJDBCUrl() {
    String template =
        Boolean.parseBoolean(System.getProperty(IS_FAKE_SERVICE_TEST_PROP))
            ? "jdbc:databricks://%s/default;transportMode=http;ssl=1;AuthMech=3;httpPath=%s"
            : "jdbc:databricks://%s/default;ssl=0;AuthMech=3;httpPath=%s";

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

  public static void insertTestDataForJoins(String table1Name, String table2Name) {
    // Insert data into the first table
    String insertTable1SQL1 =
        "INSERT INTO "
            + getFullyQualifiedTableName(table1Name)
            + " (id, col1, col2) VALUES (1, 'value1_table1', 'value2_table1')";
    executeSQL(insertTable1SQL1);

    String insertTable1SQL2 =
        "INSERT INTO "
            + getFullyQualifiedTableName(table1Name)
            + " (id, col1, col2) VALUES (2, 'value3_table1', 'value4_table1')";
    executeSQL(insertTable1SQL2);

    // Insert related data into the second table
    String insertTable2SQL1 =
        "INSERT INTO "
            + getFullyQualifiedTableName(table2Name)
            + " (id, col1, col2) VALUES (1, 'related_value1_table2', 'related_value2_table2')";
    executeSQL(insertTable2SQL1);

    String insertTable2SQL2 =
        "INSERT INTO "
            + getFullyQualifiedTableName(table2Name)
            + " (id, col1, col2) VALUES (2, 'related_value3_table2', 'related_value4_table2')";
    executeSQL(insertTable2SQL2);
  }

  public static void insertTestData(String tableName) {
    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, col1, col2) VALUES (1, 'value1', 'value2')";
    executeSQL(insertSQL);
  }
}
