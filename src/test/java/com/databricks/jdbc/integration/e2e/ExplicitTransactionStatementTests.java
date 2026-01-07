package com.databricks.jdbc.integration.e2e;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import org.junit.jupiter.api.*;

/**
 * End-to-end integration tests for explicit transaction SQL statements (BEGIN TRANSACTION, COMMIT,
 * ROLLBACK, SET AUTOCOMMIT).
 *
 * <p>These tests verify the behavior of explicit SQL transaction statements as opposed to JDBC API
 * methods (connection.commit(), connection.rollback(), etc.).
 *
 * <p><b>Important:</b> Databricks documents that there should be no mixing of explicit SQL
 * statements and JDBC transaction APIs. These tests focus purely on SQL statement behavior.
 *
 * <p><b>Two Transaction Modes (Mutually Exclusive):</b>
 *
 * <ul>
 *   <li><b>Explicit Transaction Mode:</b> BEGIN TRANSACTION → ... → COMMIT/ROLLBACK (when
 *       autocommit=true)
 *   <li><b>Implicit Transaction Mode:</b> SET AUTOCOMMIT = FALSE → ... → COMMIT/ROLLBACK
 *       (auto-starts transaction)
 * </ul>
 *
 * <p>These tests require a DBSQL warehouse that supports Multi-Statement Transactions (MST).
 */
@SuppressWarnings("ALL")
public class ExplicitTransactionStatementTests {

  private static final String DATABRICKS_HOST =
      "benchmarking-staging-aws-aux8.staging.cloud.databricks.com";
  private static final String DATABRICKS_TOKEN = "token";
  private static final String DATABRICKS_HTTP_PATH = "sql/1.0/warehouses/275c4479d5d48ce8";
  private static final String DATABRICKS_CATALOG = "main";
  private static final String DATABRICKS_SCHEMA = "default";

  private static final String JDBC_URL =
      "jdbc:databricks://"
          + DATABRICKS_HOST
          + "/default;transportMode=http;ssl=1;AuthMech=3;httpPath="
          + DATABRICKS_HTTP_PATH;

  private static final String TEST_TABLE_NAME = "explicit_txn_test_table";

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
        "CREATE TABLE "
            + fullyQualifiedTableName
            + " (id INT, value VARCHAR(255)) "
            + "USING DELTA "
            + "TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')");
    stmt.close();
  }

  @AfterEach
  void cleanUp() throws SQLException {
    if (connection != null) {
      try {
        // Attempt to rollback any pending transaction
        Statement stmt = connection.createStatement();
        try {
          stmt.execute("ROLLBACK");
        } catch (SQLException e) {
          // Ignore - may not be in transaction
        }
        stmt.close();
      } catch (SQLException e) {
        // Ignore
      }

      try {
        // Clean up test table
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

  private String getFullyQualifiedTableName() {
    return DATABRICKS_CATALOG + "." + DATABRICKS_SCHEMA + "." + TEST_TABLE_NAME;
  }

  // ===================================================================================
  // 1. BEGIN TRANSACTION Mode (Core)
  // ===================================================================================

  @Test
  @DisplayName("1.1 BEGIN TRANSACTION -> INSERT -> COMMIT should persist data")
  void testBeginTransactionCommit() throws SQLException {
    Statement stmt = connection.createStatement();

    // Start explicit transaction
    stmt.execute("BEGIN TRANSACTION");

    // Insert data
    stmt.execute(
        "INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (1, 'test_value')");

    // Commit transaction
    stmt.execute("COMMIT");
    stmt.close();

    // Verify data is persisted using a new connection
    try (Connection verifyConn = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN)) {
      Statement verifyStmt = verifyConn.createStatement();
      ResultSet rs =
          verifyStmt.executeQuery(
              "SELECT value FROM " + getFullyQualifiedTableName() + " WHERE id = 1");
      assertTrue(rs.next(), "Should find inserted row after COMMIT");
      assertEquals("test_value", rs.getString(1), "Value should match inserted value");
      rs.close();
      verifyStmt.close();
    }
  }

  @Test
  @DisplayName("1.2 BEGIN TRANSACTION -> INSERT -> ROLLBACK should discard data")
  void testBeginTransactionRollback() throws SQLException {
    Statement stmt = connection.createStatement();

    // Start explicit transaction
    stmt.execute("BEGIN TRANSACTION");

    // Insert data
    stmt.execute(
        "INSERT INTO "
            + getFullyQualifiedTableName()
            + " (id, value) VALUES (1, 'rollback_value')");

    // Rollback transaction
    stmt.execute("ROLLBACK");
    stmt.close();

    // Verify data is NOT persisted using a new connection
    try (Connection verifyConn = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN)) {
      Statement verifyStmt = verifyConn.createStatement();
      ResultSet rs =
          verifyStmt.executeQuery(
              "SELECT COUNT(*) FROM " + getFullyQualifiedTableName() + " WHERE id = 1");
      assertTrue(rs.next());
      assertEquals(0, rs.getInt(1), "Should not find any rows after ROLLBACK");
      rs.close();
      verifyStmt.close();
    }
  }

  @Test
  @DisplayName("1.3 Sequential BEGIN TRANSACTIONs with mixed COMMIT and ROLLBACK")
  void testSequentialBeginTransactions() throws SQLException {
    Statement stmt = connection.createStatement();

    // First transaction - COMMIT
    stmt.execute("BEGIN TRANSACTION");
    stmt.execute(
        "INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (1, 'committed1')");
    stmt.execute("COMMIT");

    // Second transaction - ROLLBACK
    stmt.execute("BEGIN TRANSACTION");
    stmt.execute(
        "INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (2, 'rolledback')");
    stmt.execute("ROLLBACK");

    // Third transaction - COMMIT
    stmt.execute("BEGIN TRANSACTION");
    stmt.execute(
        "INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (3, 'committed3')");
    stmt.execute("COMMIT");

    stmt.close();

    // Verify only committed transactions persisted (id=1 and id=3, but NOT id=2)
    try (Connection verifyConn = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN)) {
      Statement verifyStmt = verifyConn.createStatement();

      // Check total count
      ResultSet rsCount =
          verifyStmt.executeQuery("SELECT COUNT(*) FROM " + getFullyQualifiedTableName());
      assertTrue(rsCount.next());
      assertEquals(2, rsCount.getInt(1), "Should have exactly 2 committed rows");
      rsCount.close();

      // Check id=1 exists
      ResultSet rs1 =
          verifyStmt.executeQuery(
              "SELECT value FROM " + getFullyQualifiedTableName() + " WHERE id = 1");
      assertTrue(rs1.next(), "Row with id=1 should exist");
      assertEquals("committed1", rs1.getString(1));
      rs1.close();

      // Check id=2 does NOT exist
      ResultSet rs2 =
          verifyStmt.executeQuery(
              "SELECT COUNT(*) FROM " + getFullyQualifiedTableName() + " WHERE id = 2");
      assertTrue(rs2.next());
      assertEquals(0, rs2.getInt(1), "Row with id=2 should NOT exist (was rolled back)");
      rs2.close();

      // Check id=3 exists
      ResultSet rs3 =
          verifyStmt.executeQuery(
              "SELECT value FROM " + getFullyQualifiedTableName() + " WHERE id = 3");
      assertTrue(rs3.next(), "Row with id=3 should exist");
      assertEquals("committed3", rs3.getString(1));
      rs3.close();

      verifyStmt.close();
    }
  }

  // ===================================================================================
  // 2. BEGIN TRANSACTION Failure Scenarios
  // ===================================================================================

  @Test
  @DisplayName("2.1 BEGIN TRANSACTION should fail when autocommit is false")
  void testBeginTransactionFailsWhenAutocommitFalse() throws SQLException {
    Statement stmt = connection.createStatement();

    // Set autocommit to false (starts implicit transaction mode)
    stmt.execute("SET AUTOCOMMIT = FALSE");

    // Attempt BEGIN TRANSACTION - should fail because we're already in implicit transaction mode
    SQLException exception =
        assertThrows(
            SQLException.class,
            () -> stmt.execute("BEGIN TRANSACTION"),
            "BEGIN TRANSACTION should fail when autocommit is false");

    // Verify exception message indicates the conflict
    String message = exception.getMessage();
    assertTrue(
        message.contains("BEGIN")
            || message.contains("transaction")
            || message.contains("AUTOCOMMIT"),
        "Exception should indicate BEGIN TRANSACTION conflict with autocommit=false. Got: "
            + message);

    stmt.close();
  }

  @Test
  @DisplayName("2.2 Nested BEGIN TRANSACTION should fail")
  void testNestedBeginTransactionFails() throws SQLException {
    Statement stmt = connection.createStatement();

    // Start first transaction
    stmt.execute("BEGIN TRANSACTION");

    // Attempt nested BEGIN TRANSACTION - should fail
    SQLException exception =
        assertThrows(
            SQLException.class,
            () -> stmt.execute("BEGIN TRANSACTION"),
            "Nested BEGIN TRANSACTION should fail");

    // Verify exception message indicates nested transaction not supported
    String message = exception.getMessage();
    assertTrue(
        message.contains("BEGIN")
            || message.contains("transaction")
            || message.contains("nested")
            || message.contains("active"),
        "Exception should indicate nested transaction not supported. Got: " + message);

    // Cleanup - rollback the first transaction
    stmt.execute("ROLLBACK");
    stmt.close();
  }

  // ===================================================================================
  // 3. SET AUTOCOMMIT = FALSE Mode (Implicit Transactions)
  // ===================================================================================

  @Test
  @DisplayName("3.1 SET AUTOCOMMIT = FALSE -> INSERT -> COMMIT should persist data")
  void testSetAutocommitFalseCommit() throws SQLException {
    Statement stmt = connection.createStatement();

    // Enter implicit transaction mode
    stmt.execute("SET AUTOCOMMIT = FALSE");

    // Insert data (implicitly starts a transaction)
    stmt.execute(
        "INSERT INTO "
            + getFullyQualifiedTableName()
            + " (id, value) VALUES (1, 'implicit_commit')");

    // Commit transaction
    stmt.execute("COMMIT");
    stmt.close();

    // Verify data is persisted using a new connection
    try (Connection verifyConn = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN)) {
      Statement verifyStmt = verifyConn.createStatement();
      ResultSet rs =
          verifyStmt.executeQuery(
              "SELECT value FROM " + getFullyQualifiedTableName() + " WHERE id = 1");
      assertTrue(rs.next(), "Should find inserted row after COMMIT");
      assertEquals("implicit_commit", rs.getString(1), "Value should match inserted value");
      rs.close();
      verifyStmt.close();
    }
  }

  @Test
  @DisplayName("3.2 SET AUTOCOMMIT = FALSE -> INSERT -> ROLLBACK should discard data")
  void testSetAutocommitFalseRollback() throws SQLException {
    Statement stmt = connection.createStatement();

    // Enter implicit transaction mode
    stmt.execute("SET AUTOCOMMIT = FALSE");

    // Insert data (implicitly starts a transaction)
    stmt.execute(
        "INSERT INTO "
            + getFullyQualifiedTableName()
            + " (id, value) VALUES (1, 'implicit_rollback')");

    // Rollback transaction
    stmt.execute("ROLLBACK");
    stmt.close();

    // Verify data is NOT persisted using a new connection
    try (Connection verifyConn = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN)) {
      Statement verifyStmt = verifyConn.createStatement();
      ResultSet rs =
          verifyStmt.executeQuery(
              "SELECT COUNT(*) FROM " + getFullyQualifiedTableName() + " WHERE id = 1");
      assertTrue(rs.next());
      assertEquals(0, rs.getInt(1), "Should not find any rows after ROLLBACK");
      rs.close();
      verifyStmt.close();
    }
  }

  @Test
  @DisplayName("3.3 SET AUTOCOMMIT = FALSE with multiple sequential transactions")
  void testSetAutocommitFalseSequentialTransactions() throws SQLException {
    Statement stmt = connection.createStatement();

    // Enter implicit transaction mode
    stmt.execute("SET AUTOCOMMIT = FALSE");

    // First transaction - COMMIT
    stmt.execute(
        "INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (1, 'implicit1')");
    stmt.execute("COMMIT");

    // Second transaction - ROLLBACK (new transaction auto-starts after COMMIT)
    stmt.execute(
        "INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (2, 'implicit2')");
    stmt.execute("ROLLBACK");

    // Third transaction - COMMIT
    stmt.execute(
        "INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (3, 'implicit3')");
    stmt.execute("COMMIT");

    stmt.close();

    // Verify only committed transactions persisted (id=1 and id=3, but NOT id=2)
    try (Connection verifyConn = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN)) {
      Statement verifyStmt = verifyConn.createStatement();

      // Check total count
      ResultSet rsCount =
          verifyStmt.executeQuery("SELECT COUNT(*) FROM " + getFullyQualifiedTableName());
      assertTrue(rsCount.next());
      assertEquals(2, rsCount.getInt(1), "Should have exactly 2 committed rows");
      rsCount.close();

      // Check id=1 exists
      ResultSet rs1 =
          verifyStmt.executeQuery(
              "SELECT value FROM " + getFullyQualifiedTableName() + " WHERE id = 1");
      assertTrue(rs1.next(), "Row with id=1 should exist");
      assertEquals("implicit1", rs1.getString(1));
      rs1.close();

      // Check id=2 does NOT exist
      ResultSet rs2 =
          verifyStmt.executeQuery(
              "SELECT COUNT(*) FROM " + getFullyQualifiedTableName() + " WHERE id = 2");
      assertTrue(rs2.next());
      assertEquals(0, rs2.getInt(1), "Row with id=2 should NOT exist (was rolled back)");
      rs2.close();

      // Check id=3 exists
      ResultSet rs3 =
          verifyStmt.executeQuery(
              "SELECT value FROM " + getFullyQualifiedTableName() + " WHERE id = 3");
      assertTrue(rs3.next(), "Row with id=3 should exist");
      assertEquals("implicit3", rs3.getString(1));
      rs3.close();

      verifyStmt.close();
    }
  }

  // ===================================================================================
  // 4. SET AUTOCOMMIT Variations
  // ===================================================================================

  @Test
  @DisplayName("4.1 SET AUTOCOMMIT = TRUE should enable autocommit mode")
  void testSetAutocommitTrue() throws SQLException {
    Statement stmt = connection.createStatement();

    // First, enter implicit transaction mode
    stmt.execute("SET AUTOCOMMIT = FALSE");

    // Insert and commit to complete the transaction
    stmt.execute(
        "INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (1, 'before_true')");
    stmt.execute("COMMIT");

    // Now set autocommit back to true
    stmt.execute("SET AUTOCOMMIT = TRUE");

    // Insert data - should auto-commit immediately (no explicit COMMIT needed)
    stmt.execute(
        "INSERT INTO "
            + getFullyQualifiedTableName()
            + " (id, value) VALUES (2, 'auto_committed')");

    stmt.close();

    // Verify both rows are persisted (second one auto-committed)
    try (Connection verifyConn = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN)) {
      Statement verifyStmt = verifyConn.createStatement();

      ResultSet rs =
          verifyStmt.executeQuery("SELECT COUNT(*) FROM " + getFullyQualifiedTableName());
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1), "Should have 2 rows - both should be committed");
      rs.close();

      // Verify the auto-committed row exists
      ResultSet rs2 =
          verifyStmt.executeQuery(
              "SELECT value FROM " + getFullyQualifiedTableName() + " WHERE id = 2");
      assertTrue(rs2.next(), "Auto-committed row should exist");
      assertEquals("auto_committed", rs2.getString(1));
      rs2.close();

      verifyStmt.close();
    }
  }

  @Test
  @DisplayName("4.2 SET AUTOCOMMIT (without value) should return current autocommit value")
  void testSetAutocommitWithoutValue() throws SQLException {
    Statement stmt = connection.createStatement();

    // When autocommit is TRUE (default), SET AUTOCOMMIT should return TRUE
    ResultSet rs1 = stmt.executeQuery("SET AUTOCOMMIT");
    assertTrue(rs1.next(), "SET AUTOCOMMIT should return a result");
    // The result should indicate autocommit is TRUE (default)
    String value1 = rs1.getString(1);
    assertTrue(Boolean.parseBoolean(value1), "Should return a value");
    rs1.close();

    // Set autocommit to FALSE
    stmt.execute("SET AUTOCOMMIT = FALSE");

    // Now SET AUTOCOMMIT should return FALSE
    ResultSet rs2 = stmt.executeQuery("SET AUTOCOMMIT");
    assertTrue(rs2.next(), "SET AUTOCOMMIT should return a result");
    String value2 = rs2.getString(1);
    assertNotNull(value2, "Should return a value");
    rs2.close();

    // The values should be different (TRUE vs FALSE)
    assertNotEquals(value1, value2, "Autocommit values should differ after SET AUTOCOMMIT = FALSE");

    // Cleanup - need to commit or rollback since we're in implicit transaction mode
    stmt.execute("ROLLBACK");
    stmt.close();
  }

  @Test
  @DisplayName("4.3 SET AUTOCOMMIT = TRUE should fail during active transaction")
  void testSetAutocommitTrueDuringActiveTransaction() throws SQLException {
    Statement stmt = connection.createStatement();

    // Enter implicit transaction mode
    stmt.execute("SET AUTOCOMMIT = FALSE");

    // Execute a statement to start an active transaction
    stmt.execute(
        "INSERT INTO "
            + getFullyQualifiedTableName()
            + " (id, value) VALUES (1, 'in_transaction')");

    // Attempt to set autocommit to TRUE during active transaction - should fail
    SQLException exception =
        assertThrows(
            SQLException.class,
            () -> stmt.execute("SET AUTOCOMMIT = TRUE"),
            "SET AUTOCOMMIT = TRUE should fail during active transaction");

    // Verify exception message indicates the conflict
    String message = exception.getMessage();
    assertTrue(
        message.contains("AUTOCOMMIT")
            || message.contains("transaction")
            || message.contains("active"),
        "Exception should indicate SET AUTOCOMMIT = TRUE conflict with active transaction. Got: "
            + message);

    // Cleanup - rollback the transaction
    stmt.execute("ROLLBACK");
    stmt.close();
  }

  // ===================================================================================
  // 5. COMMIT / ROLLBACK Edge Cases
  // ===================================================================================

  @Test
  @DisplayName("5.1 COMMIT without active transaction should fail when autocommit is true")
  void testExplicitCommitWithoutActiveTransaction() throws SQLException {
    Statement stmt = connection.createStatement();

    // Connection starts with autocommit=true (default), so no active transaction
    // COMMIT should fail
    SQLException exception =
        assertThrows(
            SQLException.class,
            () -> stmt.execute("COMMIT"),
            "COMMIT should fail when there is no active transaction (autocommit=true)");

    // Verify exception message indicates no active transaction
    String message = exception.getMessage();
    assertTrue(
        message.contains("COMMIT")
            || message.contains("transaction")
            || message.contains("active")
            || message.contains("No active"),
        "Exception should indicate no active transaction. Got: " + message);

    stmt.close();
  }

  @Test
  @DisplayName("5.2 ROLLBACK without active transaction should be a safe no-op")
  void testExplicitRollbackWithoutActiveTransaction() throws SQLException {
    Statement stmt = connection.createStatement();

    // Connection starts with autocommit=true (default), so no active transaction
    // ROLLBACK should be a safe no-op (not throw an exception)
    assertDoesNotThrow(
        () -> stmt.execute("ROLLBACK"),
        "ROLLBACK should be a safe no-op when there is no active transaction");

    // Verify connection is still usable
    ResultSet rs = stmt.executeQuery("SELECT 1");
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    rs.close();

    stmt.close();
  }

  @Test
  @DisplayName("5.3 ROLLBACK after query failure should recover and allow new transaction")
  void testExplicitRollbackAfterQueryFailure() throws SQLException {
    Statement stmt = connection.createStatement();

    // Start explicit transaction
    stmt.execute("BEGIN TRANSACTION");

    // Insert valid data
    stmt.execute(
        "INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (1, 'before_error')");

    // Execute invalid SQL that will fail
    try {
      stmt.execute("INSERT INTO non_existent_table VALUES (1)");
      fail("Should have thrown SQLException for invalid table");
    } catch (SQLException e) {
      // Expected - transaction should now be in error state
    }

    // Rollback to recover
    stmt.execute("ROLLBACK");

    // Should be able to start new transaction and succeed
    stmt.execute("BEGIN TRANSACTION");
    stmt.execute(
        "INSERT INTO "
            + getFullyQualifiedTableName()
            + " (id, value) VALUES (2, 'after_recovery')");
    stmt.execute("COMMIT");

    stmt.close();

    // Verify only the second insert persisted (first was rolled back)
    try (Connection verifyConn = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN)) {
      Statement verifyStmt = verifyConn.createStatement();

      // Should have exactly 1 row
      ResultSet rsCount =
          verifyStmt.executeQuery("SELECT COUNT(*) FROM " + getFullyQualifiedTableName());
      assertTrue(rsCount.next());
      assertEquals(1, rsCount.getInt(1), "Should have exactly 1 row after recovery");
      rsCount.close();

      // Should be the row inserted after recovery
      ResultSet rs =
          verifyStmt.executeQuery(
              "SELECT value FROM " + getFullyQualifiedTableName() + " WHERE id = 2");
      assertTrue(rs.next(), "Row inserted after recovery should exist");
      assertEquals("after_recovery", rs.getString(1));
      rs.close();

      verifyStmt.close();
    }
  }

  // ===================================================================================
  // 6. Multi-Table Transactions
  // ===================================================================================

  @Test
  @DisplayName("6.1 BEGIN TRANSACTION with multi-table COMMIT should persist all data")
  void testBeginTransactionMultiTableCommit() throws SQLException {
    // Create second test table
    String table2Name = TEST_TABLE_NAME + "_2";
    String fullyQualifiedTable2Name =
        DATABRICKS_CATALOG + "." + DATABRICKS_SCHEMA + "." + table2Name;

    Statement stmt = connection.createStatement();
    stmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedTable2Name);
    stmt.execute(
        "CREATE TABLE "
            + fullyQualifiedTable2Name
            + " (id INT, category VARCHAR(255)) "
            + "USING DELTA "
            + "TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')");

    try {
      // Start explicit transaction
      stmt.execute("BEGIN TRANSACTION");

      // Insert into first table
      stmt.execute(
          "INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (1, 'table1_data')");

      // Insert into second table
      stmt.execute(
          "INSERT INTO " + fullyQualifiedTable2Name + " (id, category) VALUES (1, 'category_a')");

      // Commit both
      stmt.execute("COMMIT");

      stmt.close();

      // Verify both tables have data
      try (Connection verifyConn =
          DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN)) {
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
      }
    } finally {
      // Cleanup second table
      Statement cleanupStmt = connection.createStatement();
      cleanupStmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedTable2Name);
      cleanupStmt.close();
    }
  }

  @Test
  @DisplayName("6.2 BEGIN TRANSACTION with multi-table ROLLBACK should discard all data")
  void testBeginTransactionMultiTableRollback() throws SQLException {
    // Create second test table
    String table2Name = TEST_TABLE_NAME + "_2";
    String fullyQualifiedTable2Name =
        DATABRICKS_CATALOG + "." + DATABRICKS_SCHEMA + "." + table2Name;

    Statement stmt = connection.createStatement();
    stmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedTable2Name);
    stmt.execute(
        "CREATE TABLE "
            + fullyQualifiedTable2Name
            + " (id INT, category VARCHAR(255)) "
            + "USING DELTA "
            + "TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')");

    try {
      // Start explicit transaction
      stmt.execute("BEGIN TRANSACTION");

      // Insert into first table
      stmt.execute(
          "INSERT INTO "
              + getFullyQualifiedTableName()
              + " (id, value) VALUES (1, 'rollback_table1')");

      // Insert into second table
      stmt.execute(
          "INSERT INTO "
              + fullyQualifiedTable2Name
              + " (id, category) VALUES (1, 'rollback_table2')");

      // Rollback both
      stmt.execute("ROLLBACK");

      stmt.close();

      // Verify neither table has data
      try (Connection verifyConn =
          DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN)) {
        Statement verifyStmt = verifyConn.createStatement();

        // Check table 1 is empty
        ResultSet rs1 =
            verifyStmt.executeQuery("SELECT COUNT(*) FROM " + getFullyQualifiedTableName());
        assertTrue(rs1.next());
        assertEquals(0, rs1.getInt(1), "Table 1 should be empty after ROLLBACK");
        rs1.close();

        // Check table 2 is empty
        ResultSet rs2 = verifyStmt.executeQuery("SELECT COUNT(*) FROM " + fullyQualifiedTable2Name);
        assertTrue(rs2.next());
        assertEquals(0, rs2.getInt(1), "Table 2 should be empty after ROLLBACK");
        rs2.close();

        verifyStmt.close();
      }
    } finally {
      // Cleanup second table
      Statement cleanupStmt = connection.createStatement();
      cleanupStmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedTable2Name);
      cleanupStmt.close();
    }
  }

  @Test
  @DisplayName("6.3 BEGIN TRANSACTION multi-table atomicity - partial failure should rollback all")
  void testBeginTransactionMultiTableAtomicity() throws SQLException {
    // Create second test table
    String table2Name = TEST_TABLE_NAME + "_2";
    String fullyQualifiedTable2Name =
        DATABRICKS_CATALOG + "." + DATABRICKS_SCHEMA + "." + table2Name;

    Statement stmt = connection.createStatement();
    stmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedTable2Name);
    stmt.execute(
        "CREATE TABLE "
            + fullyQualifiedTable2Name
            + " (id INT, category VARCHAR(255)) "
            + "USING DELTA "
            + "TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')");

    try {
      // Start explicit transaction
      stmt.execute("BEGIN TRANSACTION");

      // Insert into first table (will succeed)
      stmt.execute(
          "INSERT INTO "
              + getFullyQualifiedTableName()
              + " (id, value) VALUES (1, 'should_rollback')");

      // Try to insert into non-existent table (will fail)
      try {
        stmt.execute("INSERT INTO non_existent_table VALUES (1)");
        fail("Should have thrown SQLException for non-existent table");
      } catch (SQLException e) {
        // Expected - transaction is now in failed state
      }

      // Rollback to recover
      stmt.execute("ROLLBACK");

      stmt.close();

      // Verify first table insert was also rolled back (atomicity)
      try (Connection verifyConn =
          DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN)) {
        Statement verifyStmt = verifyConn.createStatement();
        ResultSet rs =
            verifyStmt.executeQuery(
                "SELECT COUNT(*) FROM " + getFullyQualifiedTableName() + " WHERE id = 1");
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1), "First insert should also be rolled back due to atomicity");
        rs.close();
        verifyStmt.close();
      }
    } finally {
      // Cleanup second table
      Statement cleanupStmt = connection.createStatement();
      cleanupStmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedTable2Name);
      cleanupStmt.close();
    }
  }

  // ===================================================================================
  // 7. DML Operations in BEGIN TRANSACTION
  // ===================================================================================

  @Test
  @DisplayName("7.1 BEGIN TRANSACTION with UPDATE should persist changes on COMMIT")
  void testBeginTransactionUpdate() throws SQLException {
    Statement stmt = connection.createStatement();

    // First insert a row with autocommit (default)
    stmt.execute(
        "INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (1, 'original')");

    // Start explicit transaction and update
    stmt.execute("BEGIN TRANSACTION");
    stmt.execute("UPDATE " + getFullyQualifiedTableName() + " SET value = 'updated' WHERE id = 1");
    stmt.execute("COMMIT");

    stmt.close();

    // Verify update persisted
    try (Connection verifyConn = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN)) {
      Statement verifyStmt = verifyConn.createStatement();
      ResultSet rs =
          verifyStmt.executeQuery(
              "SELECT value FROM " + getFullyQualifiedTableName() + " WHERE id = 1");
      assertTrue(rs.next());
      assertEquals("updated", rs.getString(1), "Value should be updated after COMMIT");
      rs.close();
      verifyStmt.close();
    }
  }

  @Test
  @DisplayName("7.2 BEGIN TRANSACTION with DELETE should persist changes on COMMIT")
  void testBeginTransactionDelete() throws SQLException {
    Statement stmt = connection.createStatement();

    // First insert rows with autocommit (default)
    stmt.execute("INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (1, 'row1')");
    stmt.execute("INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (2, 'row2')");

    // Start explicit transaction and delete
    stmt.execute("BEGIN TRANSACTION");
    stmt.execute("DELETE FROM " + getFullyQualifiedTableName() + " WHERE id = 1");
    stmt.execute("COMMIT");

    stmt.close();

    // Verify delete persisted
    try (Connection verifyConn = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN)) {
      Statement verifyStmt = verifyConn.createStatement();
      ResultSet rs =
          verifyStmt.executeQuery("SELECT COUNT(*) FROM " + getFullyQualifiedTableName());
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1), "Should have 1 row remaining after DELETE");
      rs.close();

      // Verify the correct row remains
      ResultSet rs2 =
          verifyStmt.executeQuery(
              "SELECT value FROM " + getFullyQualifiedTableName() + " WHERE id = 2");
      assertTrue(rs2.next(), "Row with id=2 should still exist");
      assertEquals("row2", rs2.getString(1));
      rs2.close();

      verifyStmt.close();
    }
  }

  @Test
  @DisplayName("7.3 BEGIN TRANSACTION with MERGE should persist changes on COMMIT")
  void testBeginTransactionMerge() throws SQLException {
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
        "CREATE TABLE "
            + fullyQualifiedSourceTable
            + " (id INT, value VARCHAR(255)) "
            + "USING DELTA "
            + "TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')");

    // Create target table
    stmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedTargetTable);
    stmt.execute(
        "CREATE TABLE "
            + fullyQualifiedTargetTable
            + " (id INT, value VARCHAR(255)) "
            + "USING DELTA "
            + "TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')");

    // Insert initial data
    stmt.execute(
        "INSERT INTO " + fullyQualifiedSourceTable + " (id, value) VALUES (1, 'new_value')");
    stmt.execute(
        "INSERT INTO " + fullyQualifiedTargetTable + " (id, value) VALUES (1, 'old_value')");

    try {
      // Start explicit transaction
      stmt.execute("BEGIN TRANSACTION");

      // Perform MERGE operation
      stmt.execute(
          "MERGE INTO "
              + fullyQualifiedTargetTable
              + " AS target "
              + "USING "
              + fullyQualifiedSourceTable
              + " AS source "
              + "ON target.id = source.id "
              + "WHEN MATCHED THEN UPDATE SET target.value = source.value");

      // Commit
      stmt.execute("COMMIT");

      stmt.close();

      // Verify MERGE succeeded
      try (Connection verifyConn =
          DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN)) {
        Statement verifyStmt = verifyConn.createStatement();
        ResultSet rs =
            verifyStmt.executeQuery(
                "SELECT value FROM " + fullyQualifiedTargetTable + " WHERE id = 1");
        assertTrue(rs.next());
        assertEquals("new_value", rs.getString(1), "MERGE should have updated the value");
        rs.close();
        verifyStmt.close();
      }
    } finally {
      // Cleanup tables
      Statement cleanupStmt = connection.createStatement();
      cleanupStmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedSourceTable);
      cleanupStmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedTargetTable);
      cleanupStmt.close();
    }
  }

  // ===================================================================================
  // 8. Isolation Behavior
  // ===================================================================================

  @Test
  @DisplayName("8.1 BEGIN TRANSACTION should provide repeatable reads")
  void testBeginTransactionRepeatableRead() throws SQLException {
    Statement stmt = connection.createStatement();

    // Insert initial data
    stmt.execute(
        "INSERT INTO " + getFullyQualifiedTableName() + " (id, value) VALUES (1, 'initial')");

    // Start explicit transaction and read
    stmt.execute("BEGIN TRANSACTION");
    ResultSet rs1 =
        stmt.executeQuery("SELECT value FROM " + getFullyQualifiedTableName() + " WHERE id = 1");
    assertTrue(rs1.next());
    String firstRead = rs1.getString(1);
    assertEquals("initial", firstRead);
    rs1.close();

    // External connection modifies the data
    try (Connection externalConn =
        DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN)) {
      Statement externalStmt = externalConn.createStatement();
      externalStmt.execute(
          "UPDATE " + getFullyQualifiedTableName() + " SET value = 'modified' WHERE id = 1");
      externalStmt.close();
    }

    // Read again in the same transaction - should see same value (repeatable read)
    ResultSet rs2 =
        stmt.executeQuery("SELECT value FROM " + getFullyQualifiedTableName() + " WHERE id = 1");
    assertTrue(rs2.next());
    String secondRead = rs2.getString(1);
    assertEquals(firstRead, secondRead, "Should see same value in transaction (repeatable read)");
    rs2.close();

    stmt.execute("COMMIT");
    stmt.close();
  }

  @Test
  @DisplayName(
      "8.2 Concurrent BEGIN TRANSACTIONs on same table should cause ConcurrentAppendException")
  void testBeginTransactionWriteSerializabilitySingleTable() throws SQLException {
    // Create a single table for this test
    String accountsTable = TEST_TABLE_NAME + "_accounts";
    String fullyQualifiedAccountsTable =
        DATABRICKS_CATALOG + "." + DATABRICKS_SCHEMA + "." + accountsTable;

    Statement setupStmt = connection.createStatement();
    setupStmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedAccountsTable);
    setupStmt.execute(
        "CREATE TABLE "
            + fullyQualifiedAccountsTable
            + " (account_id INT, balance INT) "
            + "USING DELTA "
            + "TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')");

    // Insert initial data: Two rows
    setupStmt.execute("INSERT INTO " + fullyQualifiedAccountsTable + " VALUES (1, 100), (2, 100)");
    setupStmt.close();

    // Setup: Create two separate connections for concurrent transactions
    Connection conn1 = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN);
    Connection conn2 = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN);

    try {
      Statement stmt1 = conn1.createStatement();
      Statement stmt2 = conn2.createStatement();

      // Transaction 1: Begin and update row 1
      stmt1.execute("BEGIN TRANSACTION");
      stmt1.execute(
          "UPDATE "
              + fullyQualifiedAccountsTable
              + " SET balance = balance - 50 WHERE account_id = 1");

      // Transaction 2: Begin and update row 2 (different row, SAME table)
      stmt2.execute("BEGIN TRANSACTION");
      stmt2.execute(
          "UPDATE "
              + fullyQualifiedAccountsTable
              + " SET balance = balance - 30 WHERE account_id = 2");

      // Transaction 1 commits first - should succeed
      stmt1.execute("COMMIT");

      // Transaction 2 attempts to commit - should FAIL with ConcurrentAppendException
      SQLException thrownException =
          assertThrows(
              SQLException.class,
              () -> stmt2.execute("COMMIT"),
              "Transaction 2 should fail with ConcurrentAppendException");

      // Verify the exception is ConcurrentAppendException
      String exceptionMessage = thrownException.getMessage();
      assertTrue(
          exceptionMessage.contains("ConcurrentAppendException")
              || exceptionMessage.contains("DELTA_CONCURRENT")
              || exceptionMessage.contains("concurrent")
              || exceptionMessage.contains("conflict"),
          "Exception should be ConcurrentAppendException. Got: " + exceptionMessage);

      // Rollback required after abort
      stmt2.execute("ROLLBACK");

      stmt1.close();
      stmt2.close();

      // Verify only Transaction 1's changes persisted
      try (Connection verifyConn =
          DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN)) {
        Statement verifyStmt = verifyConn.createStatement();

        // Check account 1 (modified by Transaction 1)
        ResultSet rsAccount1 =
            verifyStmt.executeQuery(
                "SELECT balance FROM " + fullyQualifiedAccountsTable + " WHERE account_id = 1");
        assertTrue(rsAccount1.next());
        assertEquals(
            50, rsAccount1.getInt(1), "Account 1 should have 50 (Transaction 1 committed)");
        rsAccount1.close();

        // Check account 2 (Transaction 2 failed, should be unchanged)
        ResultSet rsAccount2 =
            verifyStmt.executeQuery(
                "SELECT balance FROM " + fullyQualifiedAccountsTable + " WHERE account_id = 2");
        assertTrue(rsAccount2.next());
        assertEquals(
            100, rsAccount2.getInt(1), "Account 2 should still have 100 (Transaction 2 failed)");
        rsAccount2.close();

        verifyStmt.close();
      }

    } finally {
      // Cleanup
      conn1.close();
      conn2.close();

      Statement cleanupStmt = connection.createStatement();
      cleanupStmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedAccountsTable);
      cleanupStmt.close();
    }
  }

  @Test
  @DisplayName("8.3 Write skew across tables proves Snapshot Isolation (not full Serializable)")
  void testBeginTransactionWriteSkewAcrossTables() throws SQLException {
    /*
     * This test demonstrates that Databricks MST uses Snapshot Isolation, NOT full Serializable.
     *
     * Write Skew Anomaly Scenario (cross-table):
     * - Two separate account tables (checking and savings) with constraint: total >= 100
     * - Initial state: checking=100, savings=100 (total=200, constraint satisfied)
     * - Transaction 1: Reads both, withdraws 150 from checking
     * - Transaction 2: Reads both, withdraws 150 from savings
     *
     * Result under Snapshot Isolation: BOTH SUCCEED (writes to different tables)
     * Result under full Serializable: ONE WOULD FAIL
     */

    // Create two separate account tables
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
            + "TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')");
    setupStmt.execute("INSERT INTO " + fullyQualifiedCheckingTable + " VALUES (1, 100)");

    // Create savings account table
    setupStmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedSavingsTable);
    setupStmt.execute(
        "CREATE TABLE "
            + fullyQualifiedSavingsTable
            + " (account_id INT, balance INT) "
            + "USING DELTA "
            + "TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')");
    setupStmt.execute("INSERT INTO " + fullyQualifiedSavingsTable + " VALUES (1, 100)");

    setupStmt.close();

    // Setup: Create two separate connections for concurrent transactions
    Connection conn1 = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN);
    Connection conn2 = DriverManager.getConnection(JDBC_URL, "token", DATABRICKS_TOKEN);

    try {
      Statement stmt1 = conn1.createStatement();
      Statement stmt2 = conn2.createStatement();

      // Transaction 1: Begin and read both accounts
      stmt1.execute("BEGIN TRANSACTION");
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

      assertEquals(200, checking1 + savings1, "Transaction 1 should see total balance of 200");

      // Transaction 2: Begin and read both accounts (concurrent)
      stmt2.execute("BEGIN TRANSACTION");
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

      assertEquals(200, checking2 + savings2, "Transaction 2 should see total balance of 200");

      // Transaction 1: Withdraw 150 from checking account
      stmt1.execute("UPDATE " + fullyQualifiedCheckingTable + " SET balance = balance - 150");

      // Transaction 2: Withdraw 150 from savings account (different table!)
      stmt2.execute("UPDATE " + fullyQualifiedSavingsTable + " SET balance = balance - 150");

      // Commit both transactions - under Snapshot Isolation, BOTH should succeed
      stmt1.execute("COMMIT");
      stmt2.execute("COMMIT");

      stmt1.close();
      stmt2.close();

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

        // Total balance is now -100, proving write skew anomaly
        int finalTotal = finalChecking + finalSavings;
        assertEquals(
            -100,
            finalTotal,
            "Total balance is -100, proving write skew anomaly occurred. "
                + "This confirms Snapshot Isolation, NOT full Serializable.");

        verifyStmt.close();
      }

    } finally {
      // Cleanup
      conn1.close();
      conn2.close();

      Statement cleanupStmt = connection.createStatement();
      cleanupStmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedCheckingTable);
      cleanupStmt.execute("DROP TABLE IF EXISTS " + fullyQualifiedSavingsTable);
      cleanupStmt.close();
    }
  }
}
