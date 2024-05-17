package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.core.DatabricksParsingException;
import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.jdbc.core.DatabricksSQLFeatureNotSupportedException;
import com.databricks.jdbc.driver.DatabricksJdbcConstants;
import com.databricks.jdbc.integration.fakeservice.DatabricksWireMockExtension;
import com.databricks.jdbc.integration.fakeservice.FakeServiceExtension;
import com.databricks.jdbc.integration.fakeservice.StubMappingCredentialsCleaner;
import com.github.tomakehurst.wiremock.extension.Extension;
import java.sql.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Integration tests for error handling scenarios. */
public class ErrorHandlingIntegrationTests {

  /**
   * {@link FakeServiceExtension} for {@link DatabricksJdbcConstants.FakeServiceType#SQL_EXEC}.
   * Intercepts all requests to SQL Execution API.
   */
  @RegisterExtension
  private static final FakeServiceExtension sqlExecApiExtension =
      new FakeServiceExtension(
          new DatabricksWireMockExtension.Builder()
              .options(
                  wireMockConfig().dynamicPort().dynamicHttpsPort().extensions(getExtensions())),
          DatabricksJdbcConstants.FakeServiceType.SQL_EXEC,
          "https://" + System.getenv("DATABRICKS_HOST"));

  /**
   * {@link FakeServiceExtension} for {@link DatabricksJdbcConstants.FakeServiceType#CLOUD_FETCH}.
   * Intercepts all requests to Cloud Fetch API.
   */
  @RegisterExtension
  private static final FakeServiceExtension cloudFetchApiExtension =
      new FakeServiceExtension(
          new DatabricksWireMockExtension.Builder()
              .options(
                  wireMockConfig().dynamicPort().dynamicHttpsPort().extensions(getExtensions())),
          DatabricksJdbcConstants.FakeServiceType.CLOUD_FETCH,
          "https://dbstoragepzjc6kojqibtg.blob.core.windows.net");

  @Test
  void testFailureToLoadDriver() {
    Exception exception =
        assertThrows(
            ClassNotFoundException.class, () -> Class.forName("incorrect.DatabricksDriver.class"));
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

  private Connection getConnection(String url, String username, String password)
      throws SQLException {
    return DriverManager.getConnection(url, username, password);
  }

  /** Returns the extensions to be used for stubbing. */
  private static Extension[] getExtensions() {
    return new Extension[] {new StubMappingCredentialsCleaner()};
  }
}
