package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.common.DatabricksJdbcUrlParams;
import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import com.databricks.jdbc.integration.fakeservice.FakeServiceExtension;
import java.sql.*;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for NonRowcountQueryPrefixes configuration. When configured, certain DML
 * statement prefixes (e.g., INSERT, UPDATE, DELETE) are treated as returning result sets rather
 * than row counts, changing the behavior of execute() and executeQuery().
 */
public class NonRowcountQueryPrefixesIntegrationTests extends AbstractFakeServiceIntegrationTests {

  private Connection defaultConnection;
  private Connection configuredConnection;

  @BeforeEach
  void setUp() throws SQLException {
    defaultConnection = getValidJDBCConnection();
  }

  @AfterEach
  void cleanUp() throws SQLException {
    for (Connection conn : new Connection[] {defaultConnection, configuredConnection}) {
      if (conn != null) {
        if (((DatabricksConnection) conn).getConnectionContext().getClientType()
                == DatabricksClientType.THRIFT
            && getFakeServiceMode() == FakeServiceExtension.FakeServiceMode.REPLAY) {
          // Hacky fix
          // Wiremock has error in stub matching for close operation in THRIFT + REPLAY mode
        } else {
          conn.close();
        }
      }
    }
  }

  private Connection getConnectionWithPrefixes(String prefixes) throws SQLException {
    Properties props = new Properties();
    props.setProperty("NonRowcountQueryPrefixes", prefixes);
    props.put(DatabricksJdbcUrlParams.ENABLE_SQL_EXEC_HYBRID_RESULTS.getParamName(), "0");
    configuredConnection = getValidJDBCConnection(props);
    return configuredConnection;
  }

  @Test
  void testDefault_InsertReturnsNoResultSet() throws SQLException {
    String tableName = "nrqp_default_insert_table";
    setupDatabaseTable(defaultConnection, tableName);

    Statement stmt = defaultConnection.createStatement();
    boolean hasResultSet =
        stmt.execute(
            "INSERT INTO "
                + getFullyQualifiedTableName(tableName)
                + " (id, col1, col2) VALUES (1, 'a', 'b')");
    assertFalse(
        hasResultSet, "Without NonRowcountQueryPrefixes, INSERT execute() should return false");

    deleteTable(defaultConnection, tableName);
  }

  @Test
  void testConfigured_InsertReturnsResultSet() throws SQLException {
    Connection conn = getConnectionWithPrefixes("INSERT");
    String tableName = "nrqp_insert_rs_table";
    setupDatabaseTable(conn, tableName);

    Statement stmt = conn.createStatement();
    boolean hasResultSet =
        stmt.execute(
            "INSERT INTO "
                + getFullyQualifiedTableName(tableName)
                + " (id, col1, col2) VALUES (1, 'a', 'b')");
    assertTrue(hasResultSet, "With NonRowcountQueryPrefixes=INSERT, execute() should return true");

    deleteTable(conn, tableName);
  }

  @Test
  void testConfigured_UpdateReturnsResultSet() throws SQLException {
    Connection conn = getConnectionWithPrefixes("UPDATE");
    String tableName = "nrqp_update_rs_table";
    setupDatabaseTable(conn, tableName);
    insertTestData(conn, tableName);

    Statement stmt = conn.createStatement();
    boolean hasResultSet =
        stmt.execute(
            "UPDATE "
                + getFullyQualifiedTableName(tableName)
                + " SET col1 = 'updated' WHERE id = 1");
    assertTrue(hasResultSet, "With NonRowcountQueryPrefixes=UPDATE, execute() should return true");

    deleteTable(conn, tableName);
  }

  @Test
  void testConfigured_DeleteReturnsResultSet() throws SQLException {
    Connection conn = getConnectionWithPrefixes("DELETE");
    String tableName = "nrqp_delete_rs_table";
    setupDatabaseTable(conn, tableName);
    insertTestData(conn, tableName);

    Statement stmt = conn.createStatement();
    boolean hasResultSet =
        stmt.execute("DELETE FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = 1");
    assertTrue(hasResultSet, "With NonRowcountQueryPrefixes=DELETE, execute() should return true");

    deleteTable(conn, tableName);
  }

  @Test
  void testConfigured_MultiplePrefixes() throws SQLException {
    Connection conn = getConnectionWithPrefixes("INSERT,UPDATE,DELETE");
    String tableName = "nrqp_multi_prefix_table";
    setupDatabaseTable(conn, tableName);

    Statement stmt = conn.createStatement();

    // INSERT should return true
    boolean insertHasRs =
        stmt.execute(
            "INSERT INTO "
                + getFullyQualifiedTableName(tableName)
                + " (id, col1, col2) VALUES (1, 'a', 'b')");
    assertTrue(insertHasRs, "INSERT should return true with multi-prefix config");

    // UPDATE should return true
    boolean updateHasRs =
        stmt.execute(
            "UPDATE "
                + getFullyQualifiedTableName(tableName)
                + " SET col1 = 'updated' WHERE id = 1");
    assertTrue(updateHasRs, "UPDATE should return true with multi-prefix config");

    // DELETE should return true
    boolean deleteHasRs =
        stmt.execute("DELETE FROM " + getFullyQualifiedTableName(tableName) + " WHERE id = 1");
    assertTrue(deleteHasRs, "DELETE should return true with multi-prefix config");

    deleteTable(conn, tableName);
  }

  @Test
  void testConfigured_SelectStillReturnsResultSet() throws SQLException {
    Connection conn = getConnectionWithPrefixes("INSERT");
    Statement stmt = conn.createStatement();

    // SELECT should always return true regardless of NonRowcountQueryPrefixes
    boolean hasResultSet = stmt.execute("SELECT 1 AS num");
    assertTrue(hasResultSet, "SELECT should always return true for execute()");
  }

  @Test
  void testConfigured_ExecuteQuerySucceedsWithInsert() throws SQLException {
    Connection conn = getConnectionWithPrefixes("INSERT");
    String tableName = "nrqp_eq_insert_table";
    setupDatabaseTable(conn, tableName);

    Statement stmt = conn.createStatement();
    // With INSERT in NonRowcountQueryPrefixes, executeQuery should not throw
    ResultSet rs =
        assertDoesNotThrow(
            () ->
                stmt.executeQuery(
                    "INSERT INTO "
                        + getFullyQualifiedTableName(tableName)
                        + " (id, col1, col2) VALUES (1, 'a', 'b')"),
            "executeQuery() should succeed for INSERT when in NonRowcountQueryPrefixes");
    assertNotNull(rs, "executeQuery should return non-null ResultSet");

    deleteTable(conn, tableName);
  }

  @Test
  void testConfigured_CaseInsensitive() throws SQLException {
    Connection conn = getConnectionWithPrefixes("insert");
    String tableName = "nrqp_case_table";
    setupDatabaseTable(conn, tableName);

    Statement stmt = conn.createStatement();
    // Prefixes are compared case-insensitively (both prefix and query uppercased)
    boolean hasResultSet =
        stmt.execute(
            "INSERT INTO "
                + getFullyQualifiedTableName(tableName)
                + " (id, col1, col2) VALUES (1, 'a', 'b')");
    assertTrue(hasResultSet, "NonRowcountQueryPrefixes should be case-insensitive");

    deleteTable(conn, tableName);
  }
}
