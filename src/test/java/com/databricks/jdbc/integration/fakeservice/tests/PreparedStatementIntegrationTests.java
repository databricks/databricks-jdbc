package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.jdbc.driver.DatabricksJdbcConstants;
import com.databricks.jdbc.integration.fakeservice.DatabricksWireMockExtension;
import com.databricks.jdbc.integration.fakeservice.FakeServiceExtension;
import com.databricks.jdbc.integration.fakeservice.StubMappingCredentialsCleaner;
import com.github.tomakehurst.wiremock.extension.Extension;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Integration tests for PreparedStatement operations. */
public class PreparedStatementIntegrationTests {

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

  private static void insertTestData(String tableName) {
    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, col1, col2) VALUES (1, 'value1', 'value2')";
    executeSQL(insertSQL);
  }

  @Test
  void testPreparedStatementExecution() throws SQLException {
    String tableName = "prepared_statement_test_table";
    setupDatabaseTable(tableName);
    insertTestData(tableName);

    String selectSQL = "SELECT * FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = ?";
    try (PreparedStatement statement = connection.prepareStatement(selectSQL)) {
      statement.setInt(1, 1);
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next(), "Should return at least one result");
        assertEquals("value1", resultSet.getString("col1"), "Column 'col1' should match");
        assertEquals("value2", resultSet.getString("col2"), "Column 'col2' should match");
      }
    }

    deleteTable(tableName);
  }

  @Test
  void testParameterBindingInPreparedStatement() throws SQLException {
    String tableName = "parameter_binding_test_table";
    setupDatabaseTable(tableName);

    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, col1, col2) VALUES (?, ?, ?)";
    try (PreparedStatement statement = connection.prepareStatement(insertSQL)) {
      statement.setInt(1, 2);
      statement.setString(2, "value1");
      statement.setString(3, "value2");
      int affectedRows = statement.executeUpdate();
      assertEquals(1, affectedRows, "One row should be inserted");
    }

    verifyInsertedData(tableName, 2, "value1", "value2");
    deleteTable(tableName);
  }

  @Test
  void testPreparedStatementComplexQueryExecution() throws SQLException {
    String tableName = "prepared_statement_complex_query_test_table";
    setupDatabaseTable(tableName);
    insertTestData(tableName);

    String updateSQL =
        "UPDATE " + getFullyQualifiedTableName(tableName) + " SET col1 = ? WHERE id = ?";
    try (PreparedStatement statement = connection.prepareStatement(updateSQL)) {
      statement.setString(1, "Updated value");
      statement.setInt(2, 1);
      int affectedRows = statement.executeUpdate();
      assertEquals(1, affectedRows, "One row should be updated");
    }

    verifyInsertedData(tableName, 1, "Updated value", "value2");
    deleteTable(tableName);
  }

  @Test
  void testHandlingNullValuesWithPreparedStatement() throws SQLException {
    String tableName = "prepared_statement_null_handling_test_table";
    setupDatabaseTable(tableName);

    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, col1, col2) VALUES (?, ?, ?)";
    try (PreparedStatement statement = connection.prepareStatement(insertSQL)) {
      statement.setInt(1, 6);
      statement.setNull(2, java.sql.Types.VARCHAR);
      statement.setString(3, "value1");
      int affectedRows = statement.executeUpdate();
      assertEquals(1, affectedRows, "One row should be inserted with a null col1");
    }

    verifyInsertedData(tableName, 6, null, "value1");
    deleteTable(tableName);
  }

  private void verifyInsertedData(
      String tableName, int id, String col1Expected, String col2Expected) throws SQLException {
    String selectSQL =
        "SELECT col1, col2 FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = ?";
    try (PreparedStatement statement = connection.prepareStatement(selectSQL)) {
      statement.setInt(1, id);
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next(), "Should return at least one result");
        assertEquals(
            col1Expected,
            resultSet.getString("col1"),
            "Column 'col1' should match expected value.");
        assertEquals(
            col2Expected,
            resultSet.getString("col2"),
            "Column 'col2' should match expected value.");
      }
    }
  }

  /** Returns the extensions to be used for stubbing. */
  private static Extension[] getExtensions() {
    return new Extension[] {new StubMappingCredentialsCleaner()};
  }
}
