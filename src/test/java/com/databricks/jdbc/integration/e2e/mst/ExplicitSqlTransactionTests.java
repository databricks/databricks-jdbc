package com.databricks.jdbc.integration.e2e.mst;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * MST transaction tests using explicit SQL statements (BEGIN TRANSACTION / COMMIT / ROLLBACK).
 *
 * <p>Inherits 22 shared correctness tests from AbstractMstTestBase. Adds 7 SQL-specific tests for
 * BEGIN TRANSACTION edge cases and SET AUTOCOMMIT command.
 *
 * <p>Parameterized by backend: SEA (UseThriftClient=0) and Thrift (UseThriftClient=1).
 */
public class ExplicitSqlTransactionTests extends AbstractMstTestBase {

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

  // --- Transaction control via explicit SQL ---

  @Override
  protected void startTransaction(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("BEGIN TRANSACTION");
    }
  }

  @Override
  protected void commitTransaction(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("COMMIT");
    }
  }

  @Override
  protected void rollbackTransaction(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("ROLLBACK");
    }
  }

  // ========================== INHERITED TESTS (parameterized) ==========================

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
    // Explicit SQL ROLLBACK without active txn is a safe no-op
    assertDoesNotThrow(() -> rollbackTransaction(connection));
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

  // ========================== SQL-SPECIFIC TESTS ==========================

  @ParameterizedTest(name = "C.1 nestedBeginFails [{1}]")
  @MethodSource("backends")
  void testNestedBeginTransactionFails(int useThrift, String backend) throws SQLException {
    init(useThrift);
    executeSql(connection, "BEGIN TRANSACTION");
    assertThrows(SQLException.class, () -> executeSql(connection, "BEGIN TRANSACTION"));
    executeSql(connection, "ROLLBACK");
  }

  @ParameterizedTest(name = "C.2 beginFailsWhenAutocommitFalse [{1}]")
  @MethodSource("backends")
  void testBeginFailsWhenAutocommitFalse(int useThrift, String backend) throws SQLException {
    init(useThrift);
    executeSql(connection, "SET AUTOCOMMIT = FALSE");
    assertThrows(SQLException.class, () -> executeSql(connection, "BEGIN TRANSACTION"));
    executeSql(connection, "ROLLBACK");
    executeSql(connection, "SET AUTOCOMMIT = TRUE");
  }

  @ParameterizedTest(name = "C.3 setAutocommitFalseCommit [{1}]")
  @MethodSource("backends")
  void testSetAutocommitFalseCommit(int useThrift, String backend) throws SQLException {
    init(useThrift);
    String fqTable = getFullyQualifiedTableName();

    executeSql(connection, "SET AUTOCOMMIT = FALSE");
    executeSql(connection, "INSERT INTO " + fqTable + " VALUES (1, 'implicit_txn')");
    executeSql(connection, "COMMIT");

    assertEquals(1, getRowCountFromSeparateConnection(fqTable));
  }

  @ParameterizedTest(name = "C.4 setAutocommitFalseRollback [{1}]")
  @MethodSource("backends")
  void testSetAutocommitFalseRollback(int useThrift, String backend) throws SQLException {
    init(useThrift);
    String fqTable = getFullyQualifiedTableName();

    executeSql(connection, "SET AUTOCOMMIT = FALSE");
    executeSql(connection, "INSERT INTO " + fqTable + " VALUES (1, 'rolled_back')");
    executeSql(connection, "ROLLBACK");

    assertEquals(0, getRowCountFromSeparateConnection(fqTable));
  }

  @ParameterizedTest(name = "C.5 setAutocommitTrue [{1}]")
  @MethodSource("backends")
  void testSetAutocommitTrue(int useThrift, String backend) throws SQLException {
    init(useThrift);
    String fqTable = getFullyQualifiedTableName();

    executeSql(connection, "SET AUTOCOMMIT = FALSE");
    executeSql(connection, "INSERT INTO " + fqTable + " VALUES (1, 'committed')");
    executeSql(connection, "COMMIT");
    executeSql(connection, "SET AUTOCOMMIT = TRUE");
    // This INSERT should auto-commit
    executeSql(connection, "INSERT INTO " + fqTable + " VALUES (2, 'auto_committed')");

    assertEquals(2, getRowCountFromSeparateConnection(fqTable));
  }

  @ParameterizedTest(name = "C.6 setAutocommitWithoutValue [{1}]")
  @MethodSource("backends")
  void testSetAutocommitWithoutValue(int useThrift, String backend) throws SQLException {
    init(useThrift);
    try (ResultSet rs1 = executeQuery(connection, "SET AUTOCOMMIT")) {
      assertTrue(rs1.next());
      String value1 = rs1.getString(1);
      assertNotNull(value1);
    }

    executeSql(connection, "SET AUTOCOMMIT = FALSE");
    try (ResultSet rs2 = executeQuery(connection, "SET AUTOCOMMIT")) {
      assertTrue(rs2.next());
      String value2 = rs2.getString(1);
      assertNotNull(value2);
    }

    executeSql(connection, "ROLLBACK");
    executeSql(connection, "SET AUTOCOMMIT = TRUE");
  }

  @ParameterizedTest(name = "C.7 setAutocommitTrueDuringActiveTxn [{1}]")
  @MethodSource("backends")
  void testSetAutocommitTrueDuringActiveTxnFails(int useThrift, String backend)
      throws SQLException {
    init(useThrift);
    String fqTable = getFullyQualifiedTableName();

    executeSql(connection, "SET AUTOCOMMIT = FALSE");
    executeSql(connection, "INSERT INTO " + fqTable + " VALUES (1, 'active_txn')");
    assertThrows(SQLException.class, () -> executeSql(connection, "SET AUTOCOMMIT = TRUE"));
    executeSql(connection, "ROLLBACK");
    executeSql(connection, "SET AUTOCOMMIT = TRUE");
  }
}
