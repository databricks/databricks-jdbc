package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.core.DatabricksParsingException;
import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.jdbc.core.DatabricksSQLFeatureNotSupportedException;
import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import java.sql.*;
import org.junit.jupiter.api.Test;

/** Integration tests for error handling scenarios. */
public class ErrorHandlingIntegrationTests extends AbstractFakeServiceIntegrationTests {

  @Test
  void testFailureToLoadDriver() {
    Exception exception =
        assertThrows(ClassNotFoundException.class, () -> Class.forName("incorrect.Driver.class"));
    assertTrue(exception.getMessage().contains("incorrect.Driver.class"));
  }

  @Test
  void testInvalidURL() {
    Exception exception =
        assertThrows(
            DatabricksParsingException.class,
            () -> getConnection("jdbc:abcde://invalidhost:0000/db"));
    assertTrue(exception.getMessage().contains("Invalid url"));
  }

  @Test
  void testInvalidHostname() {
    SQLException e =
        assertThrows(
            SQLException.class,
            () ->
                getConnection(
                    "jdbc:databricks://e2-wrongfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;"));
    assertTrue(e.getMessage().contains("Communication link failure. Failed to connect to server."));
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
    assertTrue(e.getMessage().contains("Syntax error"));
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
        DatabricksSQLFeatureNotSupportedException.class,
        () -> {
          Connection connection = getValidJDBCConnection();
          Statement statement = connection.createStatement();
          String sql = "SELECT * FROM " + getFullyQualifiedTableName(tableName);
          ResultSet resultSet = statement.executeQuery(sql);
          resultSet.first(); // Currently unsupported method
        });
    deleteTable(tableName);
  }

  private void getConnection(String url) throws SQLException {
    DriverManager.getConnection(url, "username", "password");
  }
}
