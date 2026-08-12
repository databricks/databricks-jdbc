package com.databricks.jdbc.integration.e2e.mst;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * MST transaction tests using the JDBC API (setAutoCommit/commit/rollback).
 *
 * <p>Inherits 22 shared correctness tests from AbstractMstTestBase. Adds 10 API-specific tests for
 * setAutoCommit, isolation level, auto-start, and PreparedStatement metadata.
 *
 * <p>Parameterized by backend: SEA (UseThriftClient=0) and Thrift (UseThriftClient=1).
 */
public class JdbcApiTransactionTests extends AbstractMstTestBase {

  static Stream<Arguments> backends() {
    return Stream.of(Arguments.of(0, "SEA"), Arguments.of(1, "Thrift"));
  }

  private void init(int useThrift) throws SQLException {
    initBackend(useThrift);
  }

  @AfterEach
  void tearDown() {
    cleanup();
  }

  // --- Transaction control via JDBC API ---

  @Override
  protected void startTransaction(Connection conn) throws SQLException {
    conn.setAutoCommit(false);
  }

  @Override
  protected void commitTransaction(Connection conn) throws SQLException {
    conn.commit();
  }

  @Override
  protected void rollbackTransaction(Connection conn) throws SQLException {
    conn.rollback();
  }

  // ========================== INHERITED TESTS (parameterized) ==========================
  // Each inherited test from AbstractMstTestBase needs a parameterized wrapper.

  @ParameterizedTest(name = "{1}")
  @MethodSource("backends")
  void testCommitSingleInsert(int useThrift, String backend) throws Exception {
    init(useThrift);
    super.testCommitSingleInsert();
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("backends")
  void testCommitMultipleInserts(int useThrift, String backend) throws Exception {
    init(useThrift);
    super.testCommitMultipleInserts();
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("backends")
  void testRollbackSingleInsert(int useThrift, String backend) throws Exception {
    init(useThrift);
    super.testRollbackSingleInsert();
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("backends")
  void testMultipleSequentialTransactions(int useThrift, String backend) throws Exception {
    init(useThrift);
    super.testMultipleSequentialTransactions();
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("backends")
  void testUpdateInTransaction(int useThrift, String backend) throws Exception {
    init(useThrift);
    super.testUpdateInTransaction();
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("backends")
  void testDeleteInTransaction(int useThrift, String backend) throws Exception {
    init(useThrift);
    super.testDeleteInTransaction();
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("backends")
  void testMultiTableCommit(int useThrift, String backend) throws Exception {
    init(useThrift);
    super.testMultiTableCommit();
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("backends")
  void testMultiTableRollback(int useThrift, String backend) throws Exception {
    init(useThrift);
    super.testMultiTableRollback();
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("backends")
  void testMultiTableAtomicity(int useThrift, String backend) throws Exception {
    init(useThrift);
    super.testMultiTableAtomicity();
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("backends")
  void testCrossTableMerge(int useThrift, String backend) throws Exception {
    init(useThrift);
    super.testCrossTableMerge();
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("backends")
  void testRepeatableReads(int useThrift, String backend) throws Exception {
    init(useThrift);
    super.testRepeatableReads();
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("backends")
  void testWriteConflictSingleTable(int useThrift, String backend) throws Exception {
    init(useThrift);
    super.testWriteConflictSingleTable();
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("backends")
  void testWriteSkewProvesSnapshotIsolation(int useThrift, String backend) throws Exception {
    init(useThrift);
    super.testWriteSkewProvesSnapshotIsolation();
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("backends")
  void testCommitWithoutActiveTxnThrows(int useThrift, String backend) throws Exception {
    init(useThrift);
    super.testCommitWithoutActiveTxnThrows();
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("backends")
  void testRollbackWithoutActiveTxnBehavior(int useThrift, String backend) throws Exception {
    init(useThrift);
    super.testRollbackWithoutActiveTxnBehavior();
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("backends")
  void testCloseConnectionImplicitRollback(int useThrift, String backend) throws Exception {
    init(useThrift);
    super.testCloseConnectionImplicitRollback();
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("backends")
  void testCloseConnectionDoesNotThrow(int useThrift, String backend) throws Exception {
    init(useThrift);
    super.testCloseConnectionDoesNotThrow();
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("backends")
  void testEmptyTransactionRollback(int useThrift, String backend) throws Exception {
    init(useThrift);
    super.testEmptyTransactionRollback();
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("backends")
  void testReadOnlyTransaction(int useThrift, String backend) throws Exception {
    init(useThrift);
    super.testReadOnlyTransaction();
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("backends")
  void testRollbackAfterQueryFailure(int useThrift, String backend) throws Exception {
    init(useThrift);
    super.testRollbackAfterQueryFailure();
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("backends")
  void testMultipleStatementsInSingleTxn(int useThrift, String backend) throws Exception {
    init(useThrift);
    super.testMultipleStatementsInSingleTxn();
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("backends")
  void testPreparedStatementInsert(int useThrift, String backend) throws Exception {
    init(useThrift);
    super.testPreparedStatementInsert();
  }

  // ========================== API-SPECIFIC TESTS ==========================

  @ParameterizedTest(name = "B.1 defaultAutoCommit [{1}]")
  @MethodSource("backends")
  void testDefaultAutoCommitIsTrue(int useThrift, String backend) throws SQLException {
    init(useThrift);
    assertTrue(connection.getAutoCommit(), "New connection should default to autoCommit=true");
  }

  @ParameterizedTest(name = "B.2 autoStartAfterCommit [{1}]")
  @MethodSource("backends")
  void testAutoStartAfterCommit(int useThrift, String backend) throws SQLException {
    init(useThrift);
    String fqTable = getFullyQualifiedTableName();

    connection.setAutoCommit(false);
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO " + fqTable + " VALUES (1, 'txn1')");
    }
    connection.commit();

    // Auto-starts new transaction — insert then rollback
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO " + fqTable + " VALUES (2, 'txn2')");
    }
    connection.rollback();

    assertEquals(1, getRowCountFromSeparateConnection(fqTable));
  }

  @ParameterizedTest(name = "B.3 autoStartAfterRollback [{1}]")
  @MethodSource("backends")
  void testAutoStartAfterRollback(int useThrift, String backend) throws SQLException {
    init(useThrift);
    String fqTable = getFullyQualifiedTableName();

    connection.setAutoCommit(false);
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO " + fqTable + " VALUES (1, 'txn1')");
    }
    connection.rollback();

    // Auto-starts new transaction — insert then commit
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO " + fqTable + " VALUES (2, 'txn2')");
    }
    connection.commit();

    assertEquals(1, getRowCountFromSeparateConnection(fqTable));
  }

  @ParameterizedTest(name = "B.4 setAutoCommitDuringActiveTxn [{1}]")
  @MethodSource("backends")
  void testSetAutoCommitDuringActiveTxnThrows(int useThrift, String backend) throws SQLException {
    init(useThrift);
    String fqTable = getFullyQualifiedTableName();

    connection.setAutoCommit(false);
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO " + fqTable + " VALUES (1, 'active_txn')");
    }
    assertThrows(SQLException.class, () -> connection.setAutoCommit(true));
  }

  @ParameterizedTest(name = "B.5 unsupportedIsolationLevel [{1}]")
  @MethodSource("backends")
  void testUnsupportedIsolationLevel(int useThrift, String backend) throws SQLException {
    init(useThrift);
    assertThrows(
        SQLException.class,
        () -> connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED));
    assertThrows(
        SQLException.class,
        () -> connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED));
    assertThrows(
        SQLException.class,
        () -> connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE));
  }

  @ParameterizedTest(name = "B.6 supportedIsolationLevel [{1}]")
  @MethodSource("backends")
  void testSupportedIsolationLevel(int useThrift, String backend) throws SQLException {
    init(useThrift);
    connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
    assertEquals(Connection.TRANSACTION_REPEATABLE_READ, connection.getTransactionIsolation());
  }

  @ParameterizedTest(name = "B.7 preparedStatementUpdate [{1}]")
  @MethodSource("backends")
  void testPreparedStatementUpdate(int useThrift, String backend) throws SQLException {
    init(useThrift);
    String fqTable = getFullyQualifiedTableName();

    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO " + fqTable + " VALUES (1, 'original')");
    }

    connection.setAutoCommit(false);
    try (PreparedStatement ps =
        connection.prepareStatement("UPDATE " + fqTable + " SET value = ? WHERE id = ?")) {
      ps.setString(1, "updated_ps");
      ps.setInt(2, 1);
      ps.execute();
    }
    connection.commit();

    try (Connection verifyConn = createConnection();
        Statement stmt = verifyConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT value FROM " + fqTable + " WHERE id = 1")) {
      assertTrue(rs.next());
      assertEquals("updated_ps", rs.getString(1));
    }
  }

  @ParameterizedTest(name = "B.8 preparedStatementReuse [{1}]")
  @MethodSource("backends")
  void testPreparedStatementReuseAcrossTransactions(int useThrift, String backend)
      throws SQLException {
    init(useThrift);
    // SEA closes PreparedStatement after execute — can't reuse across transactions
    Assumptions.assumeTrue(isThrift(), "SEA closes PreparedStatement after execute");
    String fqTable = getFullyQualifiedTableName();

    connection.setAutoCommit(false);
    try (PreparedStatement ps =
        connection.prepareStatement("INSERT INTO " + fqTable + " VALUES (?, ?)")) {
      // Txn 1
      ps.setInt(1, 1);
      ps.setString(2, "txn1");
      ps.execute();
      connection.commit();

      // Txn 2 — reuse same PreparedStatement
      ps.setInt(1, 2);
      ps.setString(2, "txn2");
      ps.execute();
      connection.commit();
    }

    assertEquals(2, getRowCountFromSeparateConnection(fqTable));
  }

  @ParameterizedTest(name = "B.9 getMetaDataAfterExecute [{1}]")
  @MethodSource("backends")
  void testPreparedStatementGetMetaDataAfterExecute(int useThrift, String backend)
      throws SQLException {
    init(useThrift);
    // SEA closes PreparedStatement after execute — getMetaData() fails
    Assumptions.assumeTrue(isThrift(), "SEA closes PreparedStatement after execute");
    String fqTable = getFullyQualifiedTableName();

    connection.setAutoCommit(false);
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO " + fqTable + " VALUES (1, 'data')");
    }

    try (PreparedStatement ps =
        connection.prepareStatement("SELECT * FROM " + fqTable + " WHERE id = ?")) {
      ps.setInt(1, 1);
      ps.execute();
      ResultSetMetaData rsmd = ps.getMetaData();
      assertNotNull(rsmd);
      assertTrue(rsmd.getColumnCount() >= 2);
    }
    connection.rollback();
  }

  @ParameterizedTest(name = "B.10 getParameterMetaData [{1}]")
  @MethodSource("backends")
  void testGetParameterMetaData(int useThrift, String backend) throws SQLException {
    init(useThrift);
    String fqTable = getFullyQualifiedTableName();

    connection.setAutoCommit(false);
    try (PreparedStatement ps =
        connection.prepareStatement("INSERT INTO " + fqTable + " VALUES (?, ?)")) {
      ParameterMetaData pmd = ps.getParameterMetaData();
      assertNotNull(pmd);
    }
    connection.rollback();
  }
}
