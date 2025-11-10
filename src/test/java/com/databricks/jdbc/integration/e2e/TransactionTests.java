package com.databricks.jdbc.integration.e2e;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.exception.DatabricksTransactionException;
import java.sql.*;
import org.junit.jupiter.api.*;

/**
 * End-to-end integration tests for transaction APIs (setAutoCommit, getAutoCommit, commit,
 * rollback).
 *
 * <p>These tests require a DBSQL warehouse that supports Multi-Statement Transactions (MST).
 *
 * <p><b>Setup Instructions:</b>
 *
 * <p>Set the following environment variables before running:
 *
 * <ul>
 *   <li>DATABRICKS_HOST - Your workspace host (e.g., "your-workspace.cloud.databricks.com:443")
 *   <li>DATABRICKS_TOKEN - Your personal access token
 *   <li>DATABRICKS_HTTP_PATH - DBSQL warehouse HTTP path (e.g.,
 *       "/sql/1.0/warehouses/your-warehouse-id")
 *   <li>DATABRICKS_CATALOG - Catalog name (e.g., "main")
 *   <li>DATABRICKS_SCHEMA - Schema name (e.g., "default")
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>
 * export DATABRICKS_HOST="your-workspace.cloud.databricks.com:443"
 * export DATABRICKS_TOKEN="dapi..."
 * export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/abc123"
 * export DATABRICKS_CATALOG="main"
 * export DATABRICKS_SCHEMA="default"
 * mvn test -Dtest=TransactionTests
 * </pre>
 */
@SuppressWarnings("ALL")
public class TransactionTests {

  // Configuration from environment variables
  private static final String DATABRICKS_HOST = "e2-dogfood.staging.cloud.databricks.com";
  private static final String DATABRICKS_TOKEN = "token";
  private static final String DATABRICKS_HTTP_PATH = "sql/1.0/warehouses/58aa1b363649e722";
  private static final String DATABRICKS_CATALOG = "main";
  private static final String DATABRICKS_SCHEMA = "default";

  private static final String JDBC_URL =
      "jdbc:databricks://"
          + DATABRICKS_HOST
          + "/default;transportMode=http;ssl=1;AuthMech=3;httpPath="
          + DATABRICKS_HTTP_PATH;

  private static final String TEST_TABLE_NAME = "transaction_test_table";

  private Connection connection;

  @BeforeEach
  void setUp() throws SQLException {
    connection = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN);

    // Create test table
    String fullyQualifiedTableName =
        DATABRICKS_CATALOG + "." + DATABRICKS_SCHEMA + "." + TEST_TABLE_NAME;
    Statement stmt = connection.createStatement();
    stmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedTableName);
    stmt.execute(
        "CREATE TABLE IF NOT EXISTS "
            + fullyQualifiedTableName
            + " (id INT, value VARCHAR(255)) "
            + "USING DELTA "
            + "TBLPROPERTIES ('delta.feature.catalogOwned-preview' = 'supported')");
    stmt.close();
  }

  @AfterEach
  void cleanUp() throws SQLException {
    try {
      // Try to roll back any pending transaction
      if (connection != null && !connection.getAutoCommit()) {
        try {
          connection.rollback();
        } catch (SQLException e) {
          // Ignore - may not be in transaction
        }
        // Reset to autocommit mode
        try {
          connection.setAutoCommit(true);
        } catch (SQLException e) {
          // Ignore
        }
      }
    } finally {
      // Clean up test table
      if (connection != null) {
        try {
          String fullyQualifiedTableName =
              DATABRICKS_CATALOG + "." + DATABRICKS_SCHEMA + "." + TEST_TABLE_NAME;
          Statement stmt = connection.createStatement();
          stmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedTableName);
          stmt.close();
        } catch (SQLException e) {
          // Ignore cleanup errors
        }
        connection.close();
      }
    }
  }

  private String getFullyQualifiedTableName() {
    return DATABRICKS_CATALOG + "." + DATABRICKS_SCHEMA + "." + TEST_TABLE_NAME;
  }

  // ==================== SUCCESS SCENARIOS ====================

  @Test
  @DisplayName("Should default to autoCommit=true on new connection")
  void testDefaultAutoCommit() throws SQLException {
    assertTrue(connection.getAutoCommit(), "New connection should have autoCommit=true by default");
  }

  @Test
  @DisplayName("Should successfully set autoCommit to false")
  void testSetAutoCommitFalse() throws SQLException {
    connection.setAutoCommit(false);
    assertFalse(
        connection.getAutoCommit(), "AutoCommit should be false after setAutoCommit(false)");
  }

  @Test
  @DisplayName("Should successfully set autoCommit back to true")
  void testSetAutoCommitTrue() throws SQLException {
    // First disable
    connection.setAutoCommit(false);
    assertFalse(connection.getAutoCommit());

    // Then enable
    connection.setAutoCommit(true);
    assertTrue(connection.getAutoCommit(), "AutoCommit should be true after setAutoCommit(true)");
  }

  @Test
  @DisplayName("Should successfully commit a transaction with single INSERT")
  void testCommitSingleInsert() throws SQLException {
    // Start transaction
    connection.setAutoCommit(false);

    // Insert data
    Statement stmt = connection.createStatement();
    stmt.execute(
        "INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (1, 'test_value')");
    stmt.close();

    // Commit
    connection.commit();

    // Verify data is persisted (in new connection to ensure it's committed)
    try (Connection verifyConn = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN)) {
      Statement verifyStmt = verifyConn.createStatement();
      ResultSet rs =
          verifyStmt.executeQuery(
              "SELECT value FROM " + getFullyQualifiedTableName() + " WHERE id = 1");
      assertTrue(rs.next(), "Should find inserted row after commit");
      assertEquals("test_value", rs.getString(1), "Value should match inserted value");
      rs.close();
      verifyStmt.close();
    }
  }

  @Test
  @DisplayName("Should successfully commit a transaction with multiple INSERT(s)")
  void testCommitMultipleInserts() throws SQLException {
    connection.setAutoCommit(false);

    // Insert multiple rows
    Statement stmt = connection.createStatement();
    stmt.execute(
        "INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (1, 'value1')");
    stmt.execute(
        "INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (2, 'value2')");
    stmt.execute(
        "INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (3, 'value3')");
    stmt.close();

    connection.commit();

    // Verify all rows persisted
    try (Connection verifyConn = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN)) {
      Statement verifyStmt = verifyConn.createStatement();
      ResultSet rs =
          verifyStmt.executeQuery("SELECT COUNT(*) FROM " + getFullyQualifiedTableName());
      assertTrue(rs.next());
      assertEquals(3, rs.getInt(1), "Should have 3 rows after commit");
      rs.close();
      verifyStmt.close();
    }
  }

  @Test
  @DisplayName("Should successfully rollback a transaction")
  void testRollbackTransaction() throws SQLException {
    connection.setAutoCommit(false);

    // Insert data
    Statement stmt = connection.createStatement();
    stmt.execute(
        "INSERT INTO "
            + getFullyQualifiedTableName()
            + " (id, value) VALUES (100, 'rollback_test')");
    stmt.close();

    // Rollback
    connection.rollback();

    // Verify data is NOT persisted
    try (Connection verifyConn = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN)) {
      Statement verifyStmt = verifyConn.createStatement();
      ResultSet rs =
          verifyStmt.executeQuery(
              "SELECT COUNT(*) FROM " + getFullyQualifiedTableName() + " WHERE id = 100");
      assertTrue(rs.next());
      assertEquals(0, rs.getInt(1), "Rolled back data should not be persisted");
      rs.close();
      verifyStmt.close();
    }
  }

  @Test
  @DisplayName("Should execute multiple sequential transactions")
  void testMultipleSequentialTransactions() throws SQLException {
    // First transaction - commit
    connection.setAutoCommit(false);
    Statement stmt = connection.createStatement();
    stmt.execute("INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (1, 'txn1')");
    stmt.close();
    connection.commit();

    // Second transaction - commit
    stmt = connection.createStatement();
    stmt.execute("INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (2, 'txn2')");
    stmt.close();
    connection.commit();

    // Third transaction - rollback
    stmt = connection.createStatement();
    stmt.execute("INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (3, 'txn3')");
    stmt.close();
    connection.rollback();

    // Verify only first two transactions persisted
    try (Connection verifyConn = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN)) {
      Statement verifyStmt = verifyConn.createStatement();
      ResultSet rs =
          verifyStmt.executeQuery("SELECT COUNT(*) FROM " + getFullyQualifiedTableName());
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1), "Should have 2 committed rows");
      rs.close();
      verifyStmt.close();
    }
  }

  @Test
  @DisplayName("Should auto-start new transaction after commit")
  void testAutoStartTransactionAfterCommit() throws SQLException {
    connection.setAutoCommit(false);

    // First transaction
    Statement stmt = connection.createStatement();
    stmt.execute(
        "INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (1, 'first')");
    stmt.close();
    connection.commit();

    // Should be able to start new transaction immediately
    stmt = connection.createStatement();
    stmt.execute(
        "INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (2, 'second')");
    stmt.close();
    connection.rollback(); // Rollback the second one to test isolation

    // Verify only first transaction persisted
    try (Connection verifyConn = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN)) {
      Statement verifyStmt = verifyConn.createStatement();
      ResultSet rs =
          verifyStmt.executeQuery("SELECT COUNT(*) FROM " + getFullyQualifiedTableName());
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1), "Only first transaction should be committed");
      rs.close();
      verifyStmt.close();
    }
  }

  @Test
  @DisplayName("Should auto-start new transaction after rollback")
  void testAutoStartTransactionAfterRollback() throws SQLException {
    connection.setAutoCommit(false);

    // First transaction - rollback
    Statement stmt = connection.createStatement();
    stmt.execute(
        "INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (1, 'rolled_back')");
    stmt.close();
    connection.rollback();

    // Should be able to start new transaction immediately
    stmt = connection.createStatement();
    stmt.execute(
        "INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (2, 'committed')");
    stmt.close();
    connection.commit();

    // Verify only second transaction persisted
    try (Connection verifyConn = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN)) {
      Statement verifyStmt = verifyConn.createStatement();
      ResultSet rs =
          verifyStmt.executeQuery("SELECT COUNT(*) FROM " + getFullyQualifiedTableName());
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1), "Only second transaction should be committed");
      rs.close();
      verifyStmt.close();
    }
  }

  // ==================== FAILURE SCENARIOS ====================

  @Test
  @DisplayName("Should throw exception when committing without active transaction")
  void testCommitWithoutActiveTransaction() throws SQLException {
    // With autoCommit=true (no active transaction)
    assertTrue(connection.getAutoCommit());

    DatabricksTransactionException exception =
        assertThrows(
            DatabricksTransactionException.class,
            () -> connection.commit(),
            "COMMIT should throw exception when autocommit=true");

    assertTrue(
        exception.getMessage().contains("MULTI_STATEMENT_TRANSACTION_NO_ACTIVE_TRANSACTION")
            || exception.getMessage().contains("No active transaction"),
        "Exception message should indicate no active transaction");
  }

  @Test
  @DisplayName("Should safely handle ROLLBACK without active transaction (no-op)")
  void testRollbackWithoutActiveTransactionIsNoOp() throws SQLException {
    // With autoCommit=true (no active transaction)
    assertTrue(connection.getAutoCommit());

    // ROLLBACK is more forgiving than COMMIT - it should succeed as a no-op
    // when there's no active transaction
    assertDoesNotThrow(
        () -> connection.rollback(),
        "ROLLBACK should be a safe no-op when autocommit=true (no active transaction)");

    // Verify connection is still usable
    assertTrue(connection.getAutoCommit());
    assertFalse(connection.isClosed());
  }

  @Test
  @DisplayName("Should throw exception when changing autoCommit during active transaction")
  void testSetAutoCommitDuringTransaction() throws SQLException {
    connection.setAutoCommit(false);

    // Execute a statement to start a transaction
    Statement stmt = connection.createStatement();
    stmt.execute("INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (1, 'test')");
    stmt.close();

    // Try to change autoCommit - should fail
    DatabricksTransactionException exception =
        assertThrows(
            DatabricksTransactionException.class,
            () -> connection.setAutoCommit(true),
            "setAutoCommit should throw exception during active transaction");

    assertTrue(
        exception.getMessage().contains("AUTOCOMMIT_SET_TRUE_DURING_AUTOCOMMIT_TRANSACTION")
            || exception
                .getMessage()
                .contains("implicit transaction started by SET AUTOCOMMIT=FALSE"),
        "Exception message should indicate active transaction conflict");

    // Clean up
    connection.rollback();
  }

  @Test
  @DisplayName("Should throw exception for unsupported transaction isolation level")
  void testUnsupportedTransactionIsolation() {
    assertThrows(
        SQLException.class,
        () -> connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED),
        "Should throw exception for unsupported isolation level");

    assertThrows(
        SQLException.class,
        () -> connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED),
        "Should throw exception for unsupported isolation level");

    assertThrows(
        SQLException.class,
        () -> connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE),
        "Should throw exception for unsupported isolation level");
  }

  @Test
  @DisplayName("Should support REPEATABLE_READ transaction isolation level")
  void testSupportedTransactionIsolation() throws SQLException {
    // Databricks MST uses Snapshot isolation, which maps to REPEATABLE_READ in JDBC
    // - Reads are repeatable (pinned to table version at first access)
    // - Writes use Snapshot Isolation across tables
    // - Write Serializability within a single table
    connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

    // Verify it's set correctly
    assertEquals(
        Connection.TRANSACTION_REPEATABLE_READ,
        connection.getTransactionIsolation(),
        "Transaction isolation should be REPEATABLE_READ");
  }

  // ==================== EDGE CASES ====================

  @Test
  @DisplayName("Should rollback on query failure and recover")
  void testRollbackAfterQueryFailure() throws SQLException {
    connection.setAutoCommit(false);

    // Insert valid data
    Statement stmt = connection.createStatement();
    stmt.execute(
        "INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (1, 'before_error')");
    stmt.close();

    // Execute invalid SQL that will fail
    try {
      stmt = connection.createStatement();
      stmt.execute("INSERT INTO non_existent_table VALUES (1)");
      fail("Should have thrown SQLException for invalid table");
    } catch (SQLException e) {
      // Expected - transaction should now be in error state
    } finally {
      stmt.close();
    }

    // Rollback to recover
    connection.rollback();

    // Should be able to start new transaction
    stmt = connection.createStatement();
    stmt.execute(
        "INSERT INTO "
            + getFullyQualifiedTableName()
            + " (id, value) VALUES (2, 'after_recovery')");
    stmt.close();
    connection.commit();

    // Verify only the second insert persisted
    try (Connection verifyConn = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN)) {
      Statement verifyStmt = verifyConn.createStatement();
      ResultSet rs =
          verifyStmt.executeQuery("SELECT COUNT(*) FROM " + getFullyQualifiedTableName());
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1), "Only insert after rollback and recovery should be persisted");
      rs.close();
      verifyStmt.close();
    }
  }

  @Test
  @DisplayName("Should handle UPDATE operations in transaction")
  void testUpdateInTransaction() throws SQLException {
    // First insert a row with autocommit
    connection.setAutoCommit(true);
    Statement stmt = connection.createStatement();
    stmt.execute(
        "INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (1, 'original')");
    stmt.close();

    // Start transaction and update
    connection.setAutoCommit(false);
    stmt = connection.createStatement();
    stmt.execute("UPDATE " + getFullyQualifiedTableName() + " SET value = 'updated' WHERE id = 1");
    stmt.close();
    connection.commit();

    // Verify update persisted
    try (Connection verifyConn = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN)) {
      Statement verifyStmt = verifyConn.createStatement();
      ResultSet rs =
          verifyStmt.executeQuery(
              "SELECT value FROM " + getFullyQualifiedTableName() + " WHERE id = 1");
      assertTrue(rs.next());
      assertEquals("updated", rs.getString(1), "Value should be updated after commit");
      rs.close();
      verifyStmt.close();
    }
  }

  @Test
  @DisplayName("Should handle DELETE operations in transaction")
  void testDeleteInTransaction() throws SQLException {
    // First insert rows with autocommit
    connection.setAutoCommit(true);
    Statement stmt = connection.createStatement();
    stmt.execute("INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (1, 'row1')");
    stmt.execute("INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (2, 'row2')");
    stmt.close();

    // Start transaction and delete
    connection.setAutoCommit(false);
    stmt = connection.createStatement();
    stmt.execute("DELETE FROM " + getFullyQualifiedTableName() + " WHERE id = 1");
    stmt.close();
    connection.commit();

    // Verify delete persisted
    try (Connection verifyConn = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN)) {
      Statement verifyStmt = verifyConn.createStatement();
      ResultSet rs =
          verifyStmt.executeQuery("SELECT COUNT(*) FROM " + getFullyQualifiedTableName());
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1), "Should have 1 row remaining after delete");
      rs.close();
      verifyStmt.close();
    }
  }

  @Test
  @DisplayName("Should preserve exception details in DatabricksTransactionException")
  void testExceptionDetailsPreserved() throws SQLException {
    connection.setAutoCommit(false);

    // Insert to start transaction
    Statement stmt = connection.createStatement();
    stmt.execute("INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (1, 'test')");
    stmt.close();

    // Try to change autoCommit during transaction
    try {
      connection.setAutoCommit(true);
      fail("Should have thrown DatabricksTransactionException");
    } catch (DatabricksTransactionException e) {
      // Verify exception details are preserved
      assertNotNull(e.getMessage(), "Exception message should not be null");
      assertNotNull(e.getSQLState(), "SQL state should not be null");
      assertNotNull(e.getCause(), "Cause should not be null");

      // Verify original SQLException is the cause
      assertInstanceOf(SQLException.class, e.getCause(), "Cause should be a SQLException");
    }

    // Clean up
    connection.rollback();
  }

  // ==================== MULTI-TABLE TRANSACTIONS ====================

  @Test
  @DisplayName("Should successfully commit multi-table transaction")
  void testMultiTableTransactionCommit() throws SQLException {
    // Create second test table
    String table2Name = TEST_TABLE_NAME + "_2";
    String fullyQualifiedTable2Name =
        DATABRICKS_CATALOG + "." + DATABRICKS_SCHEMA + "." + table2Name;

    Statement stmt = connection.createStatement();
    stmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedTable2Name);
    stmt.execute(
        "CREATE TABLE IF NOT EXISTS "
            + fullyQualifiedTable2Name
            + " (id INT, category VARCHAR(255)) "
            + "USING DELTA "
            + "TBLPROPERTIES ('delta.feature.catalogOwned-preview' = 'supported')");
    stmt.close();

    try {
      // Start transaction
      connection.setAutoCommit(false);

      // Insert into first table
      stmt = connection.createStatement();
      stmt.execute(
          "INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (1, 'table1_data')");
      stmt.close();

      // Insert into second table
      stmt = connection.createStatement();
      stmt.execute(
          "INSERT INTO " + fullyQualifiedTable2Name + " (id, category) VALUES (1, 'category_a')");
      stmt.close();

      // Commit both
      connection.commit();

      connection.setAutoCommit(true);

      // Verify both tables have data
      Connection verifyConn = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN);
      try {
        Statement verifyStmt = verifyConn.createStatement();

        // Check table 1
        ResultSet rs1 =
            verifyStmt.executeQuery(
                "SELECT value FROM " + getFullyQualifiedTableName() + " WHERE id = 1");
        assertTrue(rs1.next(), "Should find row in table 1");
        assertEquals("table1_data", rs1.getString(1));
        rs1.close();

        // Check table 2
        ResultSet rs2 =
            verifyStmt.executeQuery(
                "SELECT category FROM " + fullyQualifiedTable2Name + " WHERE id = 1");
        assertTrue(rs2.next(), "Should find row in table 2");
        assertEquals("category_a", rs2.getString(1));
        rs2.close();

        verifyStmt.close();
      } finally {
        verifyConn.close();
      }
    } finally {
      // Cleanup second table
      stmt = connection.createStatement();
      stmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedTable2Name);
      stmt.close();
    }
  }

  @Test
  @DisplayName("Should rollback multi-table transaction atomically")
  void testMultiTableTransactionRollback() throws SQLException {
    // Create second test table
    String table2Name = TEST_TABLE_NAME + "_2";
    String fullyQualifiedTable2Name =
        DATABRICKS_CATALOG + "." + DATABRICKS_SCHEMA + "." + table2Name;

    Statement stmt = connection.createStatement();
    stmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedTable2Name);
    stmt.execute(
        "CREATE TABLE IF NOT EXISTS "
            + fullyQualifiedTable2Name
            + " (id INT, category VARCHAR(255)) "
            + "USING DELTA "
            + "TBLPROPERTIES ('delta.feature.catalogOwned-preview' = 'supported')");
    stmt.close();

    try {
      // Start transaction
      connection.setAutoCommit(false);

      // Insert into first table
      stmt = connection.createStatement();
      stmt.execute(
          "INSERT INTO "
              + getFullyQualifiedTableName()
              + " (id, value) VALUES (10, 'rollback_test1')");
      stmt.close();

      // Insert into second table
      stmt = connection.createStatement();
      stmt.execute(
          "INSERT INTO "
              + fullyQualifiedTable2Name
              + " (id, category) VALUES (10, 'rollback_test2')");
      stmt.close();

      // Rollback both
      connection.rollback();

      connection.setAutoCommit(true);

      // Verify neither table has the data
      Connection verifyConn = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN);
      try {
        Statement verifyStmt = verifyConn.createStatement();

        // Check table 1
        ResultSet rs1 =
            verifyStmt.executeQuery(
                "SELECT COUNT(*) FROM " + getFullyQualifiedTableName() + " WHERE id = 10");
        assertTrue(rs1.next());
        assertEquals(0, rs1.getInt(1), "Table 1 should not have rolled back data");
        rs1.close();

        // Check table 2
        ResultSet rs2 =
            verifyStmt.executeQuery(
                "SELECT COUNT(*) FROM " + fullyQualifiedTable2Name + " WHERE id = 10");
        assertTrue(rs2.next());
        assertEquals(0, rs2.getInt(1), "Table 2 should not have rolled back data");
        rs2.close();

        verifyStmt.close();
      } finally {
        verifyConn.close();
      }
    } finally {
      // Cleanup second table
      stmt = connection.createStatement();
      stmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedTable2Name);
      stmt.close();
    }
  }

  @Test
  @DisplayName("Should ensure atomicity with partial failure in multi-table transaction")
  void testMultiTableTransactionAtomicity() throws SQLException {
    // Create second test table
    String table2Name = TEST_TABLE_NAME + "_2";
    String fullyQualifiedTable2Name =
        DATABRICKS_CATALOG + "." + DATABRICKS_SCHEMA + "." + table2Name;

    Statement stmt = connection.createStatement();
    stmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedTable2Name);
    stmt.execute(
        "CREATE TABLE IF NOT EXISTS "
            + fullyQualifiedTable2Name
            + " (id INT, category VARCHAR(255)) "
            + "USING DELTA "
            + "TBLPROPERTIES ('delta.feature.catalogOwned-preview' = 'supported')");
    stmt.close();

    try {
      // Start transaction
      connection.setAutoCommit(false);

      // Insert into first table (will succeed)
      stmt = connection.createStatement();
      stmt.execute(
          "INSERT INTO "
              + getFullyQualifiedTableName()
              + " (id, value) VALUES (20, 'should_rollback')");
      stmt.close();

      // Try to insert into non-existent table (will fail)
      try {
        stmt = connection.createStatement();
        stmt.execute("INSERT INTO non_existent_table VALUES (1)");
        fail("Should have thrown SQLException for non-existent table");
      } catch (SQLException e) {
        // Expected - transaction is now in failed state
      } finally {
        stmt.close();
      }

      // Rollback to recover
      connection.rollback();

      connection.setAutoCommit(true);

      // Verify first table insert was also rolled back (atomicity)
      Connection verifyConn = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN);
      try {
        Statement verifyStmt = verifyConn.createStatement();
        ResultSet rs =
            verifyStmt.executeQuery(
                "SELECT COUNT(*) FROM " + getFullyQualifiedTableName() + " WHERE id = 20");
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1), "First insert should also be rolled back due to atomicity");
        rs.close();
        verifyStmt.close();
      } finally {
        verifyConn.close();
      }
    } finally {
      // Cleanup second table
      stmt = connection.createStatement();
      stmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedTable2Name);
      stmt.close();
    }
  }

  @Test
  @DisplayName("Should support cross-table MERGE in transaction")
  void testCrossTableMergeInTransaction() throws SQLException {
    // Create source and target tables
    String sourceTable = TEST_TABLE_NAME + "_source";
    String targetTable = TEST_TABLE_NAME + "_target";
    String fullyQualifiedSourceTable =
        DATABRICKS_CATALOG + "." + DATABRICKS_SCHEMA + "." + sourceTable;
    String fullyQualifiedTargetTable =
        DATABRICKS_CATALOG + "." + DATABRICKS_SCHEMA + "." + targetTable;

    Statement stmt = connection.createStatement();

    // Create source table
    stmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedSourceTable);
    stmt.execute(
        "CREATE TABLE IF NOT EXISTS "
            + fullyQualifiedSourceTable
            + " (id INT, value VARCHAR(255)) "
            + "USING DELTA "
            + "TBLPROPERTIES ('delta.feature.catalogOwned-preview' = 'supported')");

    // Create target table
    stmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedTargetTable);
    stmt.execute(
        "CREATE TABLE IF NOT EXISTS "
            + fullyQualifiedTargetTable
            + " (id INT, value VARCHAR(255)) "
            + "USING DELTA "
            + "TBLPROPERTIES ('delta.feature.catalogOwned-preview' = 'supported')");

    // Insert initial data
    stmt.execute(
        "INSERT INTO " + fullyQualifiedSourceTable + " (id, value) VALUES (1, 'new_value')");
    stmt.execute(
        "INSERT INTO " + fullyQualifiedTargetTable + " (id, value) VALUES (1, 'old_value')");
    stmt.close();

    try {
      // Start transaction
      connection.setAutoCommit(false);

      // Perform MERGE operation
      stmt = connection.createStatement();
      stmt.execute(
          "MERGE INTO "
              + fullyQualifiedTargetTable
              + " AS target "
              + "USING "
              + fullyQualifiedSourceTable
              + " AS source "
              + "ON target.id = source.id "
              + "WHEN MATCHED THEN UPDATE SET target.value = source.value");
      stmt.close();

      // Commit
      connection.commit();

      connection.setAutoCommit(true);

      // Verify MERGE succeeded
      Connection verifyConn = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN);
      try {
        Statement verifyStmt = verifyConn.createStatement();
        ResultSet rs =
            verifyStmt.executeQuery(
                "SELECT value FROM " + fullyQualifiedTargetTable + " WHERE id = 1");
        assertTrue(rs.next());
        assertEquals("new_value", rs.getString(1), "MERGE should have updated the value");
        rs.close();
        verifyStmt.close();
      } finally {
        verifyConn.close();
      }
    } finally {
      // Cleanup tables
      stmt = connection.createStatement();
      stmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedSourceTable);
      stmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedTargetTable);
      stmt.close();
    }
  }

  @Test
  @DisplayName("Should provide repeatable reads across multiple tables in transaction")
  void testRepeatableReadsAcrossMultipleTables() throws SQLException {
    // Create second test table
    String table2Name = TEST_TABLE_NAME + "_2";
    String fullyQualifiedTable2Name =
        DATABRICKS_CATALOG + "." + DATABRICKS_SCHEMA + "." + table2Name;

    Statement stmt = connection.createStatement();
    stmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedTable2Name);
    stmt.execute(
        "CREATE TABLE IF NOT EXISTS "
            + fullyQualifiedTable2Name
            + " (id INT, category VARCHAR(255)) "
            + "USING DELTA "
            + "TBLPROPERTIES ('delta.feature.catalogOwned-preview' = 'supported')");

    // Insert initial data
    stmt.execute(
        "INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (1, 'initial1')");
    stmt.execute(
        "INSERT INTO " + fullyQualifiedTable2Name + " (id, category) VALUES (1, 'initial2')");
    stmt.close();

    try {
      // Start transaction and read from both tables
      connection.setAutoCommit(false);

      stmt = connection.createStatement();
      ResultSet rs1 =
          stmt.executeQuery("SELECT value FROM " + getFullyQualifiedTableName() + " WHERE id = 1");
      assertTrue(rs1.next());
      String firstRead1 = rs1.getString(1);
      assertEquals("initial1", firstRead1);
      rs1.close();
      stmt.close();

      stmt = connection.createStatement();
      ResultSet rs2 =
          stmt.executeQuery("SELECT category FROM " + fullyQualifiedTable2Name + " WHERE id = 1");
      assertTrue(rs2.next());
      String firstRead2 = rs2.getString(1);
      assertEquals("initial2", firstRead2);
      rs2.close();
      stmt.close();

      // External connection modifies both tables
      Connection externalConn = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN);
      try {
        Statement externalStmt = externalConn.createStatement();
        externalStmt.execute(
            "UPDATE " + getFullyQualifiedTableName() + " SET value = 'modified1' WHERE id = 1");
        externalStmt.execute(
            "UPDATE " + fullyQualifiedTable2Name + " SET category = 'modified2' WHERE id = 1");
        externalStmt.close();
      } finally {
        externalConn.close();
      }

      // Read again in the same transaction - should see same values (repeatable read)
      stmt = connection.createStatement();
      ResultSet rs3 =
          stmt.executeQuery("SELECT value FROM " + getFullyQualifiedTableName() + " WHERE id = 1");
      assertTrue(rs3.next());
      String secondRead1 = rs3.getString(1);
      assertEquals(
          firstRead1, secondRead1, "Should see same value in transaction (repeatable read)");
      rs3.close();
      stmt.close();

      stmt = connection.createStatement();
      ResultSet rs4 =
          stmt.executeQuery("SELECT category FROM " + fullyQualifiedTable2Name + " WHERE id = 1");
      assertTrue(rs4.next());
      String secondRead2 = rs4.getString(1);
      assertEquals(
          firstRead2, secondRead2, "Should see same category in transaction (repeatable read)");
      rs4.close();
      stmt.close();

      connection.commit();
    } finally {
      connection.setAutoCommit(true);
      // Cleanup second table
      stmt = connection.createStatement();
      stmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedTable2Name);
      stmt.close();
    }
  }

  @Test
  @DisplayName(
      "Should demonstrate Snapshot Isolation (not full Serializable) via write skew anomaly "
          + "across multiple tables - concurrent transactions can violate integrity constraints")
  void testWriteSkewAnomalyProvesSnapshotIsolation() throws SQLException, InterruptedException {
    /*
     * This test demonstrates that Databricks MST uses Snapshot Isolation, NOT full Serializable.
     *
     * IMPORTANT: Databricks MST provides Write Serializability WITHIN a single table
     * (concurrent writes to the same table will cause ConcurrentAppendException).
     * However, it does NOT provide full SERIALIZABLE guarantees across multiple tables.
     *
     * Write Skew Anomaly Scenario (cross-table):
     * - Two separate account tables (checking and savings) with constraint: total >= 100
     * - Initial state: checking=100, savings=100 (total=200, constraint satisfied)
     * - Transaction 1: Reads both accounts (sees total=200), decides it's safe to withdraw 150
     *   from checking
     * - Transaction 2: Concurrently reads both accounts (sees total=200), decides it's safe to
     *   withdraw 150 from savings
     *
     * Result under Snapshot Isolation (REPEATABLE_READ):
     * - Both transactions succeed (no write-write conflict, different tables)
     * - Final state: checking=-50, savings=-50 (total=-100, CONSTRAINT VIOLATED!)
     *
     * Result under full Serializable:
     * - One transaction would be aborted to prevent constraint violation
     *
     * This test proves Databricks uses Snapshot Isolation (not full Serializable) by
     * demonstrating the write skew anomaly succeeds across tables.
     */

    // Create two separate account tables (checking and savings)
    String checkingTable = TEST_TABLE_NAME + "_checking";
    String savingsTable = TEST_TABLE_NAME + "_savings";
    String fullyQualifiedCheckingTable =
        DATABRICKS_CATALOG + "." + DATABRICKS_SCHEMA + "." + checkingTable;
    String fullyQualifiedSavingsTable =
        DATABRICKS_CATALOG + "." + DATABRICKS_SCHEMA + "." + savingsTable;

    Statement setupStmt = connection.createStatement();

    // Create checking account table
    setupStmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedCheckingTable);
    setupStmt.execute(
        "CREATE TABLE "
            + fullyQualifiedCheckingTable
            + " (account_id INT, balance INT) "
            + "USING DELTA "
            + "TBLPROPERTIES ('delta.feature.catalogOwned-preview' = 'supported')");
    setupStmt.execute("INSERT INTO " + fullyQualifiedCheckingTable + " VALUES (1, 100)");

    // Create savings account table
    setupStmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedSavingsTable);
    setupStmt.execute(
        "CREATE TABLE "
            + fullyQualifiedSavingsTable
            + " (account_id INT, balance INT) "
            + "USING DELTA "
            + "TBLPROPERTIES ('delta.feature.catalogOwned-preview' = 'supported')");
    setupStmt.execute("INSERT INTO " + fullyQualifiedSavingsTable + " VALUES (1, 100)");

    setupStmt.close();

    // Setup: Create two separate connections for concurrent transactions
    Connection conn1 = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN);
    Connection conn2 = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN);

    try {
      // Start both transactions
      conn1.setAutoCommit(false);
      conn2.setAutoCommit(false);

      // Transaction 1: Read total balance across both tables
      Statement stmt1 = conn1.createStatement();
      ResultSet rs1Checking =
          stmt1.executeQuery("SELECT balance FROM " + fullyQualifiedCheckingTable);
      rs1Checking.next();
      int checking1 = rs1Checking.getInt(1);
      rs1Checking.close();

      ResultSet rs1Savings =
          stmt1.executeQuery("SELECT balance FROM " + fullyQualifiedSavingsTable);
      rs1Savings.next();
      int savings1 = rs1Savings.getInt(1);
      rs1Savings.close();

      int total1 = checking1 + savings1;
      assertEquals(200, total1, "Transaction 1 should see total balance of 200");

      // Transaction 2: Read total balance across both tables (concurrent)
      Statement stmt2 = conn2.createStatement();
      ResultSet rs2Checking =
          stmt2.executeQuery("SELECT balance FROM " + fullyQualifiedCheckingTable);
      rs2Checking.next();
      int checking2 = rs2Checking.getInt(1);
      rs2Checking.close();

      ResultSet rs2Savings =
          stmt2.executeQuery("SELECT balance FROM " + fullyQualifiedSavingsTable);
      rs2Savings.next();
      int savings2 = rs2Savings.getInt(1);
      rs2Savings.close();

      int total2 = checking2 + savings2;
      assertEquals(200, total2, "Transaction 2 should see total balance of 200");

      // Both transactions see total=200 and decide it's "safe" to withdraw 150
      // (because 200 - 150 = 50 >= 100 constraint... or so they think)

      // Transaction 1: Withdraw 150 from checking account
      stmt1.execute("UPDATE " + fullyQualifiedCheckingTable + " SET balance = balance - 150");
      stmt1.close();

      // Transaction 2: Withdraw 150 from savings account (different table!)
      stmt2.execute("UPDATE " + fullyQualifiedSavingsTable + " SET balance = balance - 150");
      stmt2.close();

      // Commit both transactions
      // Under Snapshot Isolation: BOTH SUCCEED (writes to different tables)
      // Under full Serializable: ONE WOULD FAIL to prevent constraint violation
      conn1.commit(); // Should succeed
      conn2.commit(); // Should also succeed under Snapshot Isolation!

      conn1.setAutoCommit(true);
      conn2.setAutoCommit(true);

      // Verify the write skew anomaly occurred
      try (Connection verifyConn =
          DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN)) {
        Statement verifyStmt = verifyConn.createStatement();

        // Check checking account balance
        ResultSet rsChecking =
            verifyStmt.executeQuery("SELECT balance FROM " + fullyQualifiedCheckingTable);
        assertTrue(rsChecking.next());
        int finalChecking = rsChecking.getInt(1);
        assertEquals(-50, finalChecking, "Checking account should have -50 after withdrawal");
        rsChecking.close();

        // Check savings account balance
        ResultSet rsSavings =
            verifyStmt.executeQuery("SELECT balance FROM " + fullyQualifiedSavingsTable);
        assertTrue(rsSavings.next());
        int finalSavings = rsSavings.getInt(1);
        assertEquals(-50, finalSavings, "Savings account should have -50 after withdrawal");
        rsSavings.close();

        // Check total balance - CONSTRAINT VIOLATED!
        int finalTotal = finalChecking + finalSavings;

        // This assertion PROVES we have Snapshot Isolation, not full Serializable
        // Under full Serializable, the constraint (total >= 100) would have been enforced
        assertEquals(
            -100,
            finalTotal,
            "Total balance is -100, proving write skew anomaly occurred across tables. "
                + "This confirms Snapshot Isolation (REPEATABLE_READ), NOT full Serializable. "
                + "Databricks MST provides Write Serializability within a SINGLE table, "
                + "but NOT full serializability across multiple tables. "
                + "Under full Serializable isolation, one transaction would have been aborted "
                + "to prevent this cross-table constraint violation.");

        verifyStmt.close();
      }

    } finally {
      // Cleanup
      conn1.close();
      conn2.close();

      connection.setAutoCommit(true);
      Statement cleanupStmt = connection.createStatement();
      cleanupStmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedCheckingTable);
      cleanupStmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedSavingsTable);
      cleanupStmt.close();
    }
  }

  @Test
  @DisplayName(
      "Should demonstrate Write Serializability within a single table - "
          + "concurrent writes to the same table cause ConcurrentAppendException")
  void testWriteSerializabilityWithinSingleTable() throws SQLException {
    /*
     * This test demonstrates that Databricks MST provides Write Serializability WITHIN a single
     * table.
     *
     * Scenario:
     * - Two concurrent transactions write to the SAME table (even different rows)
     * - Transaction 1 commits first
     * - Transaction 2 attempts to commit
     *
     * Expected Result:
     * - Transaction 1 succeeds
     * - Transaction 2 FAILS with ConcurrentAppendException
     *
     * This proves Write Serializability within a single table, which is STRONGER than
     * Snapshot Isolation. Combined with the write skew test (which shows Snapshot Isolation
     * across tables), this confirms Databricks MST's hybrid isolation model:
     * - Within a table: Write Serializability
     * - Across tables: Snapshot Isolation (REPEATABLE_READ)
     */

    // Create a single table
    String accountsTable = TEST_TABLE_NAME + "_single_table";
    String fullyQualifiedAccountsTable =
        DATABRICKS_CATALOG + "." + DATABRICKS_SCHEMA + "." + accountsTable;

    Statement setupStmt = connection.createStatement();
    setupStmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedAccountsTable);
    setupStmt.execute(
        "CREATE TABLE "
            + fullyQualifiedAccountsTable
            + " (account_id INT, balance INT) "
            + "USING DELTA "
            + "TBLPROPERTIES ('delta.feature.catalogOwned-preview' = 'supported')");

    // Insert initial data: Two rows
    setupStmt.execute("INSERT INTO " + fullyQualifiedAccountsTable + " VALUES (1, 100), (2, 100)");
    setupStmt.close();

    // Setup: Create two separate connections for concurrent transactions
    Connection conn1 = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN);
    Connection conn2 = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN);

    try {
      // Start both transactions
      conn1.setAutoCommit(false);
      conn2.setAutoCommit(false);

      // Transaction 1: Read and update row 1
      Statement stmt1 = conn1.createStatement();
      ResultSet rs1 =
          stmt1.executeQuery(
              "SELECT balance FROM " + fullyQualifiedAccountsTable + " WHERE account_id = 1");
      rs1.next();
      int balance1 = rs1.getInt(1);
      assertEquals(100, balance1, "Initial balance for account 1 should be 100");
      rs1.close();

      // Update row 1
      stmt1.execute(
          "UPDATE "
              + fullyQualifiedAccountsTable
              + " SET balance = balance - 50 WHERE account_id = 1");
      stmt1.close();

      // Transaction 2: Read and update row 2 (different row, SAME table)
      Statement stmt2 = conn2.createStatement();
      ResultSet rs2 =
          stmt2.executeQuery(
              "SELECT balance FROM " + fullyQualifiedAccountsTable + " WHERE account_id = 2");
      rs2.next();
      int balance2 = rs2.getInt(1);
      assertEquals(100, balance2, "Initial balance for account 2 should be 100");
      rs2.close();

      // Update row 2 (different row than Transaction 1!)
      stmt2.execute(
          "UPDATE "
              + fullyQualifiedAccountsTable
              + " SET balance = balance - 30 WHERE account_id = 2");
      stmt2.close();

      // Transaction 1 commits first - should succeed
      conn1.commit();
      conn1.setAutoCommit(true);

      // Transaction 2 attempts to commit - should FAIL with ConcurrentAppendException
      // Even though it wrote to a different row, both transactions wrote to the SAME table
      SQLException thrownException =
          assertThrows(
              SQLException.class,
              () -> conn2.commit(),
              "Transaction 2 should fail with ConcurrentAppendException when committing "
                  + "concurrent writes to the same table");

      // Verify the exception is ConcurrentAppendException
      String exceptionMessage = thrownException.getMessage();
      assertTrue(
          exceptionMessage.contains("ConcurrentAppendException")
              || exceptionMessage.contains("DELTA_CONCURRENT_APPEND")
              || exceptionMessage.contains("Files were added")
              || exceptionMessage.contains("concurrent update"),
          "Exception should be ConcurrentAppendException. Got: " + exceptionMessage);

      // Rollback required after abort
      conn2.rollback();
      conn2.setAutoCommit(true);

      // Verify only Transaction 1's changes persisted
      try (Connection verifyConn =
          DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN)) {
        Statement verifyStmt = verifyConn.createStatement();

        // Check account 1 (modified by Transaction 1)
        ResultSet rsAccount1 =
            verifyStmt.executeQuery(
                "SELECT balance FROM " + fullyQualifiedAccountsTable + " WHERE account_id = 1");
        assertTrue(rsAccount1.next());
        int finalBalance1 = rsAccount1.getInt(1);
        assertEquals(
            50, finalBalance1, "Account 1 should have 50 (Transaction 1 committed successfully)");
        rsAccount1.close();

        // Check account 2 (attempted modification by Transaction 2, should be rolled back)
        ResultSet rsAccount2 =
            verifyStmt.executeQuery(
                "SELECT balance FROM " + fullyQualifiedAccountsTable + " WHERE account_id = 2");
        assertTrue(rsAccount2.next());
        int finalBalance2 = rsAccount2.getInt(1);
        assertEquals(
            100,
            finalBalance2,
            "Account 2 should still have 100 (Transaction 2 failed and rolled back)");
        rsAccount2.close();

        verifyStmt.close();
      }

    } finally {
      // Cleanup
      conn1.close();
      conn2.close();

      connection.setAutoCommit(true);
      Statement cleanupStmt = connection.createStatement();
      cleanupStmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedAccountsTable);
      cleanupStmt.close();
    }
  }
}
