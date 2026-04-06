package com.databricks.jdbc.integration.e2e.mst;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import java.util.List;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;

/**
 * Abstract base class for MST (Multi-Statement Transaction) tests.
 *
 * <p>Parameterized by backend (SEA vs Thrift). Subclasses provide the transaction control mechanism
 * (JDBC API vs explicit SQL).
 *
 * <p>Contains 22 shared correctness tests that are mode-agnostic — they use abstract methods for
 * startTransaction/commitTransaction/rollbackTransaction.
 */
public abstract class AbstractMstTestBase {

  protected static final String TEST_TABLE = "mst_test_table";
  protected static final String TEST_TABLE_2 = "mst_test_table_2";

  protected Connection connection;
  protected int useThrift;
  protected String catalog;
  protected String schema;

  protected void initBackend(int useThrift) throws SQLException {
    this.useThrift = useThrift;
    this.catalog = getDatabricksCatalog();
    this.schema = getDatabricksSchema();
    this.connection = createConnection();
    createTestTable(connection, getFullyQualifiedTableName());
  }

  protected void cleanup() {
    try {
      if (connection != null && !connection.isClosed()) {
        try {
          if (!connection.getAutoCommit()) {
            connection.rollback();
            connection.setAutoCommit(true);
          }
        } catch (SQLException ignored) {
        }
        try (Statement stmt = connection.createStatement()) {
          stmt.execute("DROP TABLE IF EXISTS " + getFullyQualifiedTableName());
          stmt.execute("DROP TABLE IF EXISTS " + getFullyQualifiedTableName2());
        } catch (SQLException ignored) {
        }
        connection.close();
      }
    } catch (SQLException ignored) {
    }
  }

  // --- Abstract transaction control methods ---

  protected abstract void startTransaction(Connection conn) throws SQLException;

  protected abstract void commitTransaction(Connection conn) throws SQLException;

  protected abstract void rollbackTransaction(Connection conn) throws SQLException;

  // --- Helpers ---

  protected boolean isSEA() {
    return useThrift == 0;
  }

  protected boolean isThrift() {
    return useThrift == 1;
  }

  protected Connection createConnection() throws SQLException {
    return getValidJDBCConnection(
        List.of(
            List.of("UseThriftClient", String.valueOf(useThrift)),
            List.of("ignoreTransactions", "0")));
  }

  protected String getFullyQualifiedTableName() {
    return catalog + "." + schema + "." + TEST_TABLE;
  }

  protected String getFullyQualifiedTableName2() {
    return catalog + "." + schema + "." + TEST_TABLE_2;
  }

  protected void createTestTable(Connection conn, String fqTableName) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS " + fqTableName);
      stmt.execute(
          "CREATE TABLE "
              + fqTableName
              + " (id INT, value STRING) USING DELTA"
              + " TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')");
    }
  }

  /** Verify row count from a separate connection to avoid in-transaction caching. */
  protected int getRowCountFromSeparateConnection(String fqTableName) throws SQLException {
    try (Connection verifyConn = createConnection();
        Statement stmt = verifyConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + fqTableName)) {
      assertTrue(rs.next());
      return rs.getInt(1);
    }
  }

  // ========================== SHARED CORRECTNESS TESTS ==========================

  @Test
  void testCommitSingleInsert() throws SQLException {
    String fqTable = getFullyQualifiedTableName();
    startTransaction(connection);

    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO " + fqTable + " VALUES (1, 'committed')");
    }
    commitTransaction(connection);

    assertEquals(1, getRowCountFromSeparateConnection(fqTable));
  }

  @Test
  void testCommitMultipleInserts() throws SQLException {
    String fqTable = getFullyQualifiedTableName();
    startTransaction(connection);

    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO " + fqTable + " VALUES (1, 'a')");
      stmt.execute("INSERT INTO " + fqTable + " VALUES (2, 'b')");
      stmt.execute("INSERT INTO " + fqTable + " VALUES (3, 'c')");
    }
    commitTransaction(connection);

    assertEquals(3, getRowCountFromSeparateConnection(fqTable));
  }

  @Test
  void testRollbackSingleInsert() throws SQLException {
    String fqTable = getFullyQualifiedTableName();
    startTransaction(connection);

    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO " + fqTable + " VALUES (1, 'rolled_back')");
    }
    rollbackTransaction(connection);

    assertEquals(0, getRowCountFromSeparateConnection(fqTable));
  }

  @Test
  void testMultipleSequentialTransactions() throws SQLException {
    String fqTable = getFullyQualifiedTableName();

    // Txn 1: commit
    startTransaction(connection);
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO " + fqTable + " VALUES (1, 'txn1')");
    }
    commitTransaction(connection);

    // Txn 2: commit
    startTransaction(connection);
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO " + fqTable + " VALUES (2, 'txn2')");
    }
    commitTransaction(connection);

    // Txn 3: rollback
    startTransaction(connection);
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO " + fqTable + " VALUES (3, 'txn3')");
    }
    rollbackTransaction(connection);

    assertEquals(2, getRowCountFromSeparateConnection(fqTable));
  }

  @Test
  void testUpdateInTransaction() throws SQLException {
    String fqTable = getFullyQualifiedTableName();
    // Insert with autocommit
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO " + fqTable + " VALUES (1, 'original')");
    }

    startTransaction(connection);
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("UPDATE " + fqTable + " SET value = 'updated' WHERE id = 1");
    }
    commitTransaction(connection);

    try (Connection verifyConn = createConnection();
        Statement stmt = verifyConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT value FROM " + fqTable + " WHERE id = 1")) {
      assertTrue(rs.next());
      assertEquals("updated", rs.getString(1));
    }
  }

  @Test
  void testDeleteInTransaction() throws SQLException {
    String fqTable = getFullyQualifiedTableName();
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO " + fqTable + " VALUES (1, 'a')");
      stmt.execute("INSERT INTO " + fqTable + " VALUES (2, 'b')");
    }

    startTransaction(connection);
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("DELETE FROM " + fqTable + " WHERE id = 1");
    }
    commitTransaction(connection);

    assertEquals(1, getRowCountFromSeparateConnection(fqTable));
  }

  @Test
  void testMultiTableCommit() throws SQLException {
    String fqTable1 = getFullyQualifiedTableName();
    String fqTable2 = getFullyQualifiedTableName2();
    createTestTable(connection, fqTable2);

    startTransaction(connection);
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO " + fqTable1 + " VALUES (1, 'table1')");
      stmt.execute("INSERT INTO " + fqTable2 + " VALUES (1, 'table2')");
    }
    commitTransaction(connection);

    assertEquals(1, getRowCountFromSeparateConnection(fqTable1));
    assertEquals(1, getRowCountFromSeparateConnection(fqTable2));
  }

  @Test
  void testMultiTableRollback() throws SQLException {
    String fqTable1 = getFullyQualifiedTableName();
    String fqTable2 = getFullyQualifiedTableName2();
    createTestTable(connection, fqTable2);

    startTransaction(connection);
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO " + fqTable1 + " VALUES (1, 'table1')");
      stmt.execute("INSERT INTO " + fqTable2 + " VALUES (1, 'table2')");
    }
    rollbackTransaction(connection);

    assertEquals(0, getRowCountFromSeparateConnection(fqTable1));
    assertEquals(0, getRowCountFromSeparateConnection(fqTable2));
  }

  @Test
  void testMultiTableAtomicity() throws SQLException {
    String fqTable = getFullyQualifiedTableName();

    startTransaction(connection);
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO " + fqTable + " VALUES (1, 'should_rollback')");
      assertThrows(
          SQLException.class,
          () -> {
            try (Statement s = connection.createStatement()) {
              s.execute("INSERT INTO nonexistent_table_xyz VALUES (1, 'fail')");
            }
          });
    }
    rollbackTransaction(connection);

    assertEquals(0, getRowCountFromSeparateConnection(fqTable));
  }

  @Test
  void testCrossTableMerge() throws SQLException {
    String fqTable1 = getFullyQualifiedTableName();
    String fqTable2 = getFullyQualifiedTableName2();
    createTestTable(connection, fqTable2);

    // Setup: insert into target table
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO " + fqTable1 + " VALUES (1, 'old_value')");
      stmt.execute("INSERT INTO " + fqTable2 + " VALUES (1, 'source_val')");
      stmt.execute("INSERT INTO " + fqTable2 + " VALUES (2, 'new_row')");
    }

    startTransaction(connection);
    try (Statement stmt = connection.createStatement()) {
      stmt.execute(
          "MERGE INTO "
              + fqTable1
              + " t USING "
              + fqTable2
              + " s ON t.id = s.id"
              + " WHEN MATCHED THEN UPDATE SET t.value = s.value"
              + " WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)");
    }
    commitTransaction(connection);

    assertEquals(2, getRowCountFromSeparateConnection(fqTable1));
  }

  @Test
  void testRepeatableReads() throws SQLException {
    String fqTable = getFullyQualifiedTableName();
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO " + fqTable + " VALUES (1, 'initial')");
    }

    startTransaction(connection);
    String firstRead;
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT value FROM " + fqTable + " WHERE id = 1")) {
      assertTrue(rs.next());
      firstRead = rs.getString(1);
    }

    // External connection modifies data
    try (Connection extConn = createConnection();
        Statement stmt = extConn.createStatement()) {
      stmt.execute("UPDATE " + fqTable + " SET value = 'modified' WHERE id = 1");
    }

    // Re-read in same transaction — should see original value
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT value FROM " + fqTable + " WHERE id = 1")) {
      assertTrue(rs.next());
      assertEquals(firstRead, rs.getString(1), "Repeatable read: should see same value");
    }
    rollbackTransaction(connection);
  }

  @Test
  void testWriteConflictSingleTable() throws Exception {
    String fqTable = getFullyQualifiedTableName();
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO " + fqTable + " VALUES (1, 'initial')");
    }

    Connection conn1 = createConnection();
    Connection conn2 = createConnection();
    try {
      startTransaction(conn1);
      startTransaction(conn2);

      try (Statement s1 = conn1.createStatement()) {
        s1.execute("UPDATE " + fqTable + " SET value = 'conn1' WHERE id = 1");
      }
      try (Statement s2 = conn2.createStatement()) {
        s2.execute("UPDATE " + fqTable + " SET value = 'conn2' WHERE id = 1");
      }

      commitTransaction(conn1);
      assertThrows(SQLException.class, () -> commitTransaction(conn2));
    } finally {
      try {
        conn1.close();
      } catch (SQLException ignored) {
      }
      try {
        conn2.close();
      } catch (SQLException ignored) {
      }
    }
  }

  @Test
  void testWriteSkewProvesSnapshotIsolation() throws Exception {
    String fqTable1 = getFullyQualifiedTableName();
    String fqTable2 = getFullyQualifiedTableName2();
    createTestTable(connection, fqTable2);

    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO " + fqTable1 + " VALUES (1, 'account1')");
      stmt.execute("INSERT INTO " + fqTable2 + " VALUES (1, 'account2')");
    }

    Connection conn1 = createConnection();
    Connection conn2 = createConnection();
    try {
      startTransaction(conn1);
      startTransaction(conn2);

      // conn1 writes to table1, conn2 writes to table2 — no conflict
      try (Statement s1 = conn1.createStatement()) {
        s1.execute("UPDATE " + fqTable1 + " SET value = 'modified1' WHERE id = 1");
      }
      try (Statement s2 = conn2.createStatement()) {
        s2.execute("UPDATE " + fqTable2 + " SET value = 'modified2' WHERE id = 1");
      }

      // Both should succeed — proves Snapshot Isolation (not full Serializable)
      commitTransaction(conn1);
      commitTransaction(conn2);

      assertEquals(1, getRowCountFromSeparateConnection(fqTable1));
      assertEquals(1, getRowCountFromSeparateConnection(fqTable2));
    } finally {
      try {
        conn1.close();
      } catch (SQLException ignored) {
      }
      try {
        conn2.close();
      } catch (SQLException ignored) {
      }
    }
  }

  @Test
  void testCommitWithoutActiveTxnThrows() throws SQLException {
    assertThrows(
        SQLException.class,
        () -> commitTransaction(connection),
        "Commit without active transaction should throw");
  }

  @Test
  void testRollbackWithoutActiveTxnBehavior() throws SQLException {
    // JDBC API throws; explicit SQL ROLLBACK is a no-op
    // Subclasses may override if behavior differs
    try {
      rollbackTransaction(connection);
    } catch (SQLException e) {
      // Acceptable — JDBC API mode throws here
    }
  }

  @Test
  void testCloseConnectionImplicitRollback() throws SQLException {
    String fqTable = getFullyQualifiedTableName();
    Connection tempConn = createConnection();
    startTransaction(tempConn);
    try (Statement stmt = tempConn.createStatement()) {
      stmt.execute("INSERT INTO " + fqTable + " VALUES (1, 'should_rollback')");
    }
    tempConn.close();

    assertEquals(0, getRowCountFromSeparateConnection(fqTable));
  }

  @Test
  void testCloseConnectionDoesNotThrow() throws SQLException {
    String fqTable = getFullyQualifiedTableName();
    Connection tempConn = createConnection();
    startTransaction(tempConn);
    try (Statement stmt = tempConn.createStatement()) {
      stmt.execute("INSERT INTO " + fqTable + " VALUES (1, 'pending')");
    }
    assertDoesNotThrow(tempConn::close);
  }

  @Test
  void testEmptyTransactionRollback() throws SQLException {
    startTransaction(connection);
    assertDoesNotThrow(() -> rollbackTransaction(connection));
  }

  @Test
  void testReadOnlyTransaction() throws SQLException {
    String fqTable = getFullyQualifiedTableName();
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO " + fqTable + " VALUES (1, 'existing')");
    }

    startTransaction(connection);
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + fqTable)) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
    }
    commitTransaction(connection);

    assertEquals(1, getRowCountFromSeparateConnection(fqTable));
  }

  @Test
  void testRollbackAfterQueryFailure() throws SQLException {
    String fqTable = getFullyQualifiedTableName();

    startTransaction(connection);
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO " + fqTable + " VALUES (1, 'before_error')");
      assertThrows(SQLException.class, () -> stmt.execute("SELECT * FROM nonexistent_xyz"));
    }
    rollbackTransaction(connection);

    // New transaction should work cleanly
    startTransaction(connection);
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO " + fqTable + " VALUES (2, 'after_recovery')");
    }
    commitTransaction(connection);

    assertEquals(1, getRowCountFromSeparateConnection(fqTable));
  }

  @Test
  void testMultipleStatementsInSingleTxn() throws SQLException {
    String fqTable = getFullyQualifiedTableName();

    startTransaction(connection);
    try (Statement s1 = connection.createStatement()) {
      s1.execute("INSERT INTO " + fqTable + " VALUES (1, 'stmt1')");
    }
    try (Statement s2 = connection.createStatement()) {
      s2.execute("INSERT INTO " + fqTable + " VALUES (2, 'stmt2')");
    }
    try (Statement s3 = connection.createStatement()) {
      s3.execute("INSERT INTO " + fqTable + " VALUES (3, 'stmt3')");
    }
    commitTransaction(connection);

    assertEquals(3, getRowCountFromSeparateConnection(fqTable));
  }

  @Test
  void testPreparedStatementInsert() throws SQLException {
    String fqTable = getFullyQualifiedTableName();

    startTransaction(connection);
    try (PreparedStatement ps =
        connection.prepareStatement("INSERT INTO " + fqTable + " VALUES (?, ?)")) {
      ps.setInt(1, 1);
      ps.setString(2, "parameterized");
      ps.execute();
    }
    commitTransaction(connection);

    try (Connection verifyConn = createConnection();
        Statement stmt = verifyConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT value FROM " + fqTable + " WHERE id = 1")) {
      assertTrue(rs.next());
      assertEquals("parameterized", rs.getString(1));
    }
  }
}
