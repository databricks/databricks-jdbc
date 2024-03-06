package com.databricks.jdbc.integration.errorhandling;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.core.DatabricksParsingException;
import com.databricks.jdbc.core.DatabricksSQLException;
import java.sql.*;
import org.junit.jupiter.api.Test;

public class ErrorHandlingIntegrationTests {
  @Test
  void testFailureToLoadDriver() {
    Exception exception =
        assertThrows(
            ClassNotFoundException.class,
            () -> {
              Class.forName("incorrect.DatabricksDriver.class");
            });
    assertTrue(exception.getMessage().contains("incorrect.DatabricksDriver.class"));
  }

  @Test
  void testInvalidURL() {
    Exception exception =
        assertThrows(
            DatabricksParsingException.class,
            () -> {
              Connection connection =
                  getConnection("jdbc:abcde://invalidhost:0000/db", "username", "password");
            });
    assertTrue(exception.getMessage().contains("Invalid url"));
  }

  @Test
  void testInvalidHostname() {
    SQLException e =
        assertThrows(
            SQLException.class,
            () -> {
              Connection connection =
                  getConnection(
                      "jdbc:databricks://e2-wrongfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;",
                      "username",
                      "password");
            });
    assertTrue(e.getMessage().contains("Invalid or unknown token or hostname provided"));
  }

  @Test
  void testQuerySyntaxError() {
    String tableName = "query_syntax_error_test_table";
    setupDatabaseTable(tableName);
    DatabricksSQLException e =
        assertThrows(
            DatabricksSQLException.class,
            () -> {
              Connection connection = getValidJDBCConnection();
              Statement statement = connection.createStatement();
              String sql =
                  "INSER INTO "
                      + getFullyQualifiedTableName(tableName)
                      + " (id, col1, col2) VALUES (1, 'value1', 'value2')";
              statement.executeQuery(sql);
            });
    assertTrue(e.getMessage().contains("Error occurred during statement execution"));
    deleteTable(tableName);
  }

  @Test
  void testAccessingClosedResultSet() {
    String tableName = "access_closed_result_set_test_table";
    setupDatabaseTable(tableName);
    executeSQL(
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, col1, col2) VALUES (1, 'value1', 'value2')");
    ResultSet resultSet = executeQuery("SELECT * FROM " + getFullyQualifiedTableName(tableName));
    try {
      resultSet.close();
      assertThrows(SQLException.class, resultSet::next);
    } catch (SQLException e) {
      fail("Unexpected exception: " + e.getMessage());
    }
    deleteTable(tableName);
  }

  @Test
  void testCallingUnsupportedSQLFeature() {
    String tableName = "unsupported_sql_feature_test_table";
    setupDatabaseTable(tableName);
    assertThrows(
        SQLFeatureNotSupportedException.class,
        () -> {
          Connection connection = getValidJDBCConnection();
          Statement statement = connection.createStatement();
          String sql = "SELECT * FROM " + getFullyQualifiedTableName(tableName);
          ResultSet resultSet = statement.executeQuery(sql);
          resultSet.first(); // Currently unsupported method
        });
    deleteTable(tableName);
  }

  private Connection getConnection(String url, String username, String password)
      throws SQLException {
    return DriverManager.getConnection(url, username, password);
  }
}
