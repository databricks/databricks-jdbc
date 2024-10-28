package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksSQLFeatureNotSupportedException;
import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import java.sql.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Integration tests for error handling scenarios. */
public class ErrorHandlingIntegrationTests extends AbstractFakeServiceIntegrationTests {

  private Connection connection;

  @BeforeEach
  void setUp() throws SQLException {
    connection = getValidJDBCConnection();
  }

  @AfterEach
  void cleanUp() throws SQLException {
    if (connection != null) {
      connection.close();
    }
  }

  @Test
  void testFailureToLoadDriver() {
    Exception exception =
        assertThrows(ClassNotFoundException.class, () -> Class.forName("incorrect.Driver.class"));
    assertTrue(exception.getMessage().contains("incorrect.Driver.class"));
  }

  @Test
  void testInvalidURL() {
    Exception exception =
        assertThrows(SQLException.class, () -> getConnection("jdbc:abcde://invalidhost:0000/db"));
    assertTrue(exception.getMessage().contains("No suitable driver found for"));
  }

  @Test
  void testInvalidHostname() {
    SQLException e =
        assertThrows(
            SQLException.class,
            () ->
                getConnection(
                    "jdbc:databricks://e2-wrongfood.staging.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/791ba2a31c7fd70a;"));
    assertTrue(e.getMessage().contains("Communication link failure. Failed to connect to server"));
  }

  @Test
  void testQuerySyntaxError() {
    String tableName = "query_syntax_error_test_table";
    setupDatabaseTable(connection, tableName);
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
    deleteTable(connection, tableName);
  }

  @Test
  void testAccessingClosedResultSet() {
    String tableName = "access_closed_result_set_test_table";
    setupDatabaseTable(connection, tableName);
    executeSQL(
        connection,
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, col1, col2) VALUES (1, 'value1', 'value2')");
    ResultSet resultSet =
        executeQuery(connection, "SELECT * FROM " + getFullyQualifiedTableName(tableName));
    try {
      resultSet.close();
      assertThrows(SQLException.class, resultSet::next);
    } catch (SQLException e) {
      fail("Unexpected exception: " + e.getMessage());
    }
    deleteTable(connection, tableName);
  }

  @Test
  void testCallingUnsupportedSQLFeature() {
    String tableName = "unsupported_sql_feature_test_table";
    setupDatabaseTable(connection, tableName);
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> {
          Connection connection = getValidJDBCConnection();
          Statement statement = connection.createStatement();
          String sql = "SELECT * FROM " + getFullyQualifiedTableName(tableName);
          ResultSet resultSet = statement.executeQuery(sql);
          resultSet.first(); // Currently unsupported method
        });
    deleteTable(connection, tableName);
  }

  private void getConnection(String url) throws SQLException {
    DriverManager.getConnection(url, "username", "password");
  }
}
