package com.databricks.jdbc.integration.e2e.mst;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for SQL introspection statements and API calls blocked by MSTCheckRule inside active
 * transactions.
 *
 * <p>Each test: start txn → INSERT → execute blocked operation → expect exception → verify txn is
 * aborted (subsequent INSERT also throws).
 *
 * <p>Blocked on both SEA and Thrift backends. Uses JDBC API mode for transaction control.
 */
public class MstBlockedSqlTests extends AbstractMstTestBase {

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

  /**
   * Helper: starts a txn, inserts a row, executes the blocked SQL, asserts exception, then verifies
   * the transaction is aborted by attempting another INSERT.
   */
  private void assertBlockedAndTxnAborted(String blockedSql) throws SQLException {
    String fqTable = getFullyQualifiedTableName();

    connection.setAutoCommit(false);
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO " + fqTable + " VALUES (1, 'before_blocked')");

      // Blocked SQL should throw
      assertThrows(
          SQLException.class,
          () -> stmt.execute(blockedSql),
          "Should be blocked in MST: " + blockedSql);

      // Transaction should be aborted — subsequent INSERT should also fail
      assertThrows(
          SQLException.class,
          () -> stmt.execute("INSERT INTO " + fqTable + " VALUES (2, 'after_blocked')"),
          "Transaction should be aborted after blocked SQL");
    }
    connection.rollback();
  }

  // ========================== BLOCKED SQL STATEMENTS ==========================

  @ParameterizedTest(name = "E.1 SHOW COLUMNS [{1}]")
  @MethodSource("backends")
  void testShowColumnsBlockedInMst(int useThrift, String backend) throws SQLException {
    init(useThrift);
    assertBlockedAndTxnAborted("SHOW COLUMNS IN " + getFullyQualifiedTableName());
  }

  @ParameterizedTest(name = "E.2 SHOW TABLES [{1}]")
  @MethodSource("backends")
  void testShowTablesBlockedInMst(int useThrift, String backend) throws SQLException {
    init(useThrift);
    assertBlockedAndTxnAborted("SHOW TABLES IN " + catalog + "." + schema);
  }

  @ParameterizedTest(name = "E.3 SHOW SCHEMAS [{1}]")
  @MethodSource("backends")
  void testShowSchemasBlockedInMst(int useThrift, String backend) throws SQLException {
    init(useThrift);
    assertBlockedAndTxnAborted("SHOW SCHEMAS IN " + catalog);
  }

  @ParameterizedTest(name = "E.4 SHOW CATALOGS [{1}]")
  @MethodSource("backends")
  void testShowCatalogsBlockedInMst(int useThrift, String backend) throws SQLException {
    init(useThrift);
    assertBlockedAndTxnAborted("SHOW CATALOGS");
  }

  @ParameterizedTest(name = "E.5 SHOW FUNCTIONS [{1}]")
  @MethodSource("backends")
  void testShowFunctionsBlockedInMst(int useThrift, String backend) throws SQLException {
    init(useThrift);
    assertBlockedAndTxnAborted("SHOW FUNCTIONS");
  }

  @ParameterizedTest(name = "E.6 DESCRIBE QUERY [{1}]")
  @MethodSource("backends")
  void testDescribeQueryBlockedInMst(int useThrift, String backend) throws SQLException {
    init(useThrift);
    assertBlockedAndTxnAborted("DESCRIBE QUERY SELECT * FROM " + getFullyQualifiedTableName());
  }

  @ParameterizedTest(name = "E.7 DESCRIBE TABLE EXTENDED [{1}]")
  @MethodSource("backends")
  void testDescribeTableExtendedBlockedInMst(int useThrift, String backend) throws SQLException {
    init(useThrift);
    assertBlockedAndTxnAborted("DESCRIBE TABLE EXTENDED " + getFullyQualifiedTableName());
  }

  @ParameterizedTest(name = "E.8 DESCRIBE TABLE [{1}]")
  @MethodSource("backends")
  void testDescribeTableBlockedInMst(int useThrift, String backend) throws SQLException {
    init(useThrift);
    assertBlockedAndTxnAborted("DESCRIBE TABLE " + getFullyQualifiedTableName());
  }

  @ParameterizedTest(name = "E.9 DESCRIBE COLUMN [{1}]")
  @MethodSource("backends")
  void testDescribeColumnBlockedInMst(int useThrift, String backend) throws SQLException {
    init(useThrift);
    assertBlockedAndTxnAborted("DESCRIBE " + getFullyQualifiedTableName() + " id");
  }

  @ParameterizedTest(name = "E.10 information_schema [{1}]")
  @MethodSource("backends")
  void testInformationSchemaBlockedInMst(int useThrift, String backend) throws SQLException {
    init(useThrift);
    assertBlockedAndTxnAborted("SELECT * FROM " + catalog + ".information_schema.columns LIMIT 1");
  }

  // ========================== BLOCKED API CALLS ==========================

  @ParameterizedTest(name = "E.11 setCatalog [{1}]")
  @MethodSource("backends")
  void testSetCatalogBlockedInMst(int useThrift, String backend) throws SQLException {
    init(useThrift);
    String fqTable = getFullyQualifiedTableName();

    connection.setAutoCommit(false);
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO " + fqTable + " VALUES (1, 'before_setCatalog')");
    }
    assertThrows(
        SQLException.class,
        () -> connection.setCatalog(catalog),
        "setCatalog() should be blocked in MST");
    connection.rollback();
  }

  @ParameterizedTest(name = "E.12 setSchema [{1}]")
  @MethodSource("backends")
  void testSetSchemaBlockedInMst(int useThrift, String backend) throws SQLException {
    init(useThrift);
    String fqTable = getFullyQualifiedTableName();

    connection.setAutoCommit(false);
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO " + fqTable + " VALUES (1, 'before_setSchema')");
    }
    assertThrows(
        SQLException.class,
        () -> connection.setSchema(schema),
        "setSchema() should be blocked in MST");
    connection.rollback();
  }
}
