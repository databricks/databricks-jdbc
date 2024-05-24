package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.client.impl.sdk.PathConstants.SESSION_PATH;
import static com.databricks.jdbc.client.impl.sdk.PathConstants.STATEMENT_PATH;
import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.driver.DatabricksJdbcConstants.FakeServiceType;
import com.databricks.jdbc.integration.fakeservice.DatabricksWireMockExtension;
import com.databricks.jdbc.integration.fakeservice.FakeServiceExtension;
import com.databricks.jdbc.integration.fakeservice.StubMappingCredentialsCleaner;
import com.github.tomakehurst.wiremock.client.CountMatchingStrategy;
import com.github.tomakehurst.wiremock.extension.Extension;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Integration tests for SQL statement execution.
 *
 * <p>TODO: Remove {@link com.databricks.jdbc.integration.execution.ExecutionIntegrationTests} once
 * {@link FakeServiceExtension} tests stabilize.
 */
public class ExecutionIntegrationTests {

  /**
   * {@link FakeServiceExtension} for {@link FakeServiceType#SQL_EXEC}. Intercepts all requests to
   * SQL Execution API.
   */
  @RegisterExtension
  private static final FakeServiceExtension sqlExecApiExtension =
      new FakeServiceExtension(
          new DatabricksWireMockExtension.Builder()
              .options(
                  wireMockConfig().dynamicPort().dynamicHttpsPort().extensions(getExtensions())),
          FakeServiceType.SQL_EXEC,
          "https://" + System.getenv("DATABRICKS_HOST"));

  /**
   * {@link FakeServiceExtension} for {@link FakeServiceType#CLOUD_FETCH}. Intercepts all requests
   * to Cloud Fetch API.
   */
  @RegisterExtension
  private static final FakeServiceExtension cloudFetchApiExtension =
      new FakeServiceExtension(
          new DatabricksWireMockExtension.Builder()
              .options(
                  wireMockConfig().dynamicPort().dynamicHttpsPort().extensions(getExtensions())),
          FakeServiceType.CLOUD_FETCH,
          "https://dbstoragepzjc6kojqibtg.blob.core.windows.net");

  @Test
  void testInsertStatement() throws SQLException {
    String tableName = "insert_test_table";
    setupDatabaseTable(tableName);
    String SQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, col1, col2) VALUES (1, 'value1', 'value2')";
    assertDoesNotThrow(() -> executeSQL(SQL), "Error executing SQL");

    ResultSet rs = executeQuery("SELECT * FROM " + getFullyQualifiedTableName(tableName));
    int rows = 0;
    while (rs != null && rs.next()) {
      rows++;
    }
    assertEquals(1, rows, "Expected 1 row, got " + rows);
    deleteTable(tableName);

    // A session request is sent
    sqlExecApiExtension.verify(1, postRequestedFor(urlEqualTo(SESSION_PATH)));

    // At least 5 statement requests are sent: drop, create, insert, select, drop
    // There can be more for retries
    sqlExecApiExtension.verify(
        new CountMatchingStrategy(CountMatchingStrategy.GREATER_THAN_OR_EQUAL, 5),
        postRequestedFor(urlEqualTo(STATEMENT_PATH)));
  }

  @Test
  void testUpdateStatement() throws SQLException {
    // Insert initial test data
    String tableName = "update_test_table";
    setupDatabaseTable(tableName);
    insertTestData(tableName);

    String updateSQL =
        "UPDATE "
            + getFullyQualifiedTableName(tableName)
            + " SET col1 = 'updatedValue1' WHERE id = 1";
    executeSQL(updateSQL);

    ResultSet rs =
        executeQuery("SELECT col1 FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = 1");
    assertTrue(
        rs.next() && "updatedValue1".equals(rs.getString("col1")),
        "Expected 'updatedValue1', got " + rs.getString("col1"));
    deleteTable(tableName);

    // A session request is sent
    sqlExecApiExtension.verify(1, postRequestedFor(urlEqualTo(SESSION_PATH)));

    // At least 6 statement requests are sent: drop, create, insert, update, select, drop
    // There can be more for retries
    sqlExecApiExtension.verify(
        new CountMatchingStrategy(CountMatchingStrategy.GREATER_THAN_OR_EQUAL, 6),
        postRequestedFor(urlEqualTo(STATEMENT_PATH)));
  }

  @Test
  void testDeleteStatement() throws SQLException {
    // Insert initial test data
    String tableName = "delete_test_table";
    setupDatabaseTable(tableName);
    insertTestData(tableName);

    String deleteSQL = "DELETE FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = 1";
    executeSQL(deleteSQL);

    ResultSet rs = executeQuery("SELECT * FROM " + getFullyQualifiedTableName(tableName));
    assertFalse(rs.next(), "Expected no rows after delete");
    deleteTable(tableName);

    // At least 6 statement requests are sent: drop, create, insert, delete, select, drop
    sqlExecApiExtension.verify(
        new CountMatchingStrategy(CountMatchingStrategy.GREATER_THAN_OR_EQUAL, 6),
        postRequestedFor(urlEqualTo(STATEMENT_PATH)));
  }

  @Test
  void testCompoundStatements() throws SQLException {
    // Insert for compound test
    String tableName = "compound_test_table";
    setupDatabaseTable(tableName);
    insertTestData(tableName);

    // Update operation as part of compound test
    String updateSQL =
        "UPDATE "
            + getFullyQualifiedTableName(tableName)
            + " SET col2 = 'updatedValue2' WHERE id = 1";
    executeSQL(updateSQL);

    // Verify update operation
    ResultSet rs =
        executeQuery("SELECT col2 FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = 1");
    assertTrue(
        rs.next() && "updatedValue2".equals(rs.getString("col2")),
        "Expected 'updatedValue2', got " + rs.getString("col2"));

    // Delete operation as part of compound test
    String deleteSQL = "DELETE FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = 1";
    executeSQL(deleteSQL);

    // Verify delete operation
    rs = executeQuery("SELECT * FROM " + getFullyQualifiedTableName(tableName));
    assertFalse(rs.next(), "Expected no rows after delete");
    deleteTable(tableName);

    // At least 8 statement requests are sent:
    // drop, create, insert, update, select, delete, select, drop
    // There can be more for retries
    sqlExecApiExtension.verify(
        new CountMatchingStrategy(CountMatchingStrategy.GREATER_THAN_OR_EQUAL, 8),
        postRequestedFor(urlEqualTo(STATEMENT_PATH)));
  }

  @Test
  void testComplexQueryJoins() throws SQLException {
    String table1Name = "table1_cqj";
    String table2Name = "table2_cqj";
    setupDatabaseTable(table1Name);
    setupDatabaseTable(table2Name);
    insertTestDataForJoins(table1Name, table2Name);

    String joinSQL =
        "SELECT t1.id, t2.col2 FROM "
            + getFullyQualifiedTableName(table1Name)
            + " t1 "
            + "JOIN "
            + getFullyQualifiedTableName(table2Name)
            + " t2 "
            + "ON t1.id = t2.id";
    ResultSet rs = executeQuery(joinSQL);
    assertTrue(rs.next(), "Expected at least one row from JOIN query");
    deleteTable(table1Name);
    deleteTable(table2Name);

    // At least 11 statement requests are sent:
    // drop table1, create table1, drop table2, create table2, insert table1, insert table1,
    // insert table2, insert table2, select join, drop table1, drop table2
    sqlExecApiExtension.verify(
        new CountMatchingStrategy(CountMatchingStrategy.GREATER_THAN_OR_EQUAL, 11),
        postRequestedFor(urlEqualTo(STATEMENT_PATH)));
  }

  @Test
  void testComplexQuerySubqueries() throws SQLException {
    String tableName = "subquery_test_table";
    setupDatabaseTable(tableName);
    insertTestData(tableName);

    String subquerySQL =
        "SELECT id FROM "
            + getFullyQualifiedTableName(tableName)
            + " WHERE id IN (SELECT id FROM "
            + getFullyQualifiedTableName(tableName)
            + " WHERE col1 = 'value1')";
    ResultSet rs = executeQuery(subquerySQL);
    assertTrue(rs.next(), "Expected at least one row from subquery");
    deleteTable(tableName);

    // At least 5 statement requests are sent: drop, create, insert, select, drop
    // There can be more for retries
    sqlExecApiExtension.verify(
        new CountMatchingStrategy(CountMatchingStrategy.GREATER_THAN_OR_EQUAL, 5),
        postRequestedFor(urlEqualTo(STATEMENT_PATH)));
  }

  /** Returns the extensions to be used for stubbing. */
  private static Extension[] getExtensions() {
    return new Extension[] {new StubMappingCredentialsCleaner()};
  }
}
