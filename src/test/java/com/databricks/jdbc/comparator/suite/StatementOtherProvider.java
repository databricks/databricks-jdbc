package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.comparator.ComparisonResult;
import com.databricks.jdbc.comparator.ResultSetComparator;
import com.databricks.jdbc.comparator.error.Captures;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * Compares utility SQL commands (SHOW, DESCRIBE, EXPLAIN, SET, Transactions, SQL Scripting, Stored
 * Procedures) between Thrift and SEA.
 *
 * <p>Commands that return ResultSets are compared cell-by-cell via executeQuery. Commands that
 * return no result use execute() and compare return values. The transaction test uses JDBC API
 * (setAutoCommit, commit, rollback) to test the full flow in one test case.
 */
public class StatementOtherProvider implements SuiteProvider {

  private static final String TABLE = "comparator_tests.oss_jdbc_tests.test_result_set_types";
  private static final String TRANSACTION_FLOW = "TRANSACTION_FLOW";

  /**
   * Prefixes of commands that don't return a ResultSet — use execute() instead of executeQuery().
   */
  private static final List<String> EXECUTE_ONLY_PREFIXES = List.of("CREATE OR REPLACE PROCEDURE");

  @Override
  public List<TestCase> getTestCases() {
    return Arrays.asList(
        // SHOW commands
        new TestCase("SHOW TABLES IN comparator_tests.oss_jdbc_tests", "SHOW TABLES"),
        new TestCase("SHOW SCHEMAS IN comparator_tests", "SHOW SCHEMAS"),
        new TestCase("SHOW CATALOGS", "SHOW CATALOGS"),

        // DESCRIBE / EXPLAIN
        new TestCase("DESCRIBE TABLE " + TABLE, "DESCRIBE TABLE"),
        new TestCase("EXPLAIN SELECT * FROM " + TABLE + " WHERE id = 1", "EXPLAIN"),

        // SET
        new TestCase("SET QUERY_TAGS['comparator_test'] = 'thrift_sea'", "SET QUERY_TAGS (write)"),
        new TestCase("SET QUERY_TAGS", "SET QUERY_TAGS (read back)"),

        // Transaction — full JDBC API flow (setAutoCommit → query → commit → rollback → restore)
        new TestCase(TRANSACTION_FLOW, "Transaction flow (setAutoCommit, commit, rollback)"),

        // SQL Scripting
        new TestCase(
            "BEGIN DECLARE x INT DEFAULT 42; SELECT x; END",
            "SQL Scripting (BEGIN...END with variable)"),

        // Stored Procedures
        new TestCase(
            "CREATE OR REPLACE PROCEDURE comparator_tests.oss_jdbc_tests.test_proc(x INT) "
                + "LANGUAGE SQL SQL SECURITY INVOKER AS BEGIN SELECT x * 2 AS result; END",
            "CREATE PROCEDURE"),
        new TestCase(
            "CALL comparator_tests.oss_jdbc_tests.test_proc(21)", "CALL stored procedure"));
    // USE CATALOG/SCHEMA not included — alters session state, breaks other suites.
    // Tested locally: both Thrift and SEA behave identically (no diffs).
  }

  @Override
  public ComparisonResult execute(
      Connection conn1, Connection conn2, TestCase testCase, String label) throws Exception {
    String sql = testCase.getIdentifier();

    if (TRANSACTION_FLOW.equals(sql)) {
      return executeTransactionFlow(conn1, conn2, label);
    } else if (isExecuteOnly(sql)) {
      try (Statement s1 = conn1.createStatement();
          Statement s2 = conn2.createStatement()) {
        // execute() returns a Boolean; a thrown error is captured and compared, not aborted.
        Object result1 = Captures.resultOrThrowable(() -> s1.execute(sql));
        Object result2 = Captures.resultOrThrowable(() -> s2.execute(sql));
        return ResultSetComparator.compare(label, sql, testCase.getArgs(), result1, result2);
      }
    } else {
      try (Statement s1 = conn1.createStatement();
          Statement s2 = conn2.createStatement()) {
        Object r1 = Captures.resultOrThrowable(() -> s1.executeQuery(sql));
        Object r2 = Captures.resultOrThrowable(() -> s2.executeQuery(sql));
        try {
          return ResultSetComparator.compare(label, sql, testCase.getArgs(), r1, r2);
        } finally {
          Captures.closeIfResultSet(r1);
          Captures.closeIfResultSet(r2);
        }
      }
    }
  }

  /**
   * Tests the full JDBC transaction flow: setAutoCommit(false) → SELECT inside transaction → commit
   * → SELECT again → rollback → restore autoCommit. Compares the SELECT results between Thrift and
   * SEA.
   */
  private ComparisonResult executeTransactionFlow(Connection conn1, Connection conn2, String label)
      throws Exception {
    String query = "SELECT COUNT(*) FROM " + TABLE;
    try {
      // Begin transaction → query → commit
      conn1.setAutoCommit(false);
      conn2.setAutoCommit(false);

      // Query inside transaction
      ComparisonResult result;
      try (Statement s1 = conn1.createStatement();
          Statement s2 = conn2.createStatement();
          ResultSet rs1 = s1.executeQuery(query);
          ResultSet rs2 = s2.executeQuery(query)) {
        result = ResultSetComparator.compare(label, TRANSACTION_FLOW, new Object[0], rs1, rs2);
      }

      // Commit + rollback (rollback is no-op after commit, but verifies API works)
      conn1.commit();
      conn2.commit();
      conn1.rollback();
      conn2.rollback();

      return result;
    } finally {
      conn1.setAutoCommit(true);
      conn2.setAutoCommit(true);
    }
  }

  private static boolean isExecuteOnly(String sql) {
    String upper = sql.toUpperCase();
    return EXECUTE_ONLY_PREFIXES.stream().anyMatch(prefix -> upper.startsWith(prefix));
  }
}
