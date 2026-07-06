package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.comparator.ComparisonResult;
import com.databricks.jdbc.comparator.ResultSetComparator;
import com.databricks.jdbc.comparator.error.Captures;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * Negative utility-command cases — SHOW / DESCRIBE / EXPLAIN / SET against bad targets, plus a
 * couple of JDBC-API misuse cases ({@code getMoreResults} after results are consumed, {@code
 * getUpdateCount} before {@code execute}). Each side's outcome (value or thrown error) is compared.
 *
 * <p>Read-only: provokes errors without mutating server or session state, so it is safe on the
 * shared connections. {@code USE CATALOG/SCHEMA} is intentionally excluded (session-state mutation)
 * and belongs to the connection-state suite.
 */
public class NegativeStatementOtherProvider implements SuiteProvider {

  private static final String TABLE = "comparator_tests.oss_jdbc_tests.test_result_set_types";
  private static final String GET_MORE_RESULTS = "GET_MORE_RESULTS_AFTER_CONSUMED";
  private static final String GET_UPDATE_COUNT = "GET_UPDATE_COUNT_BEFORE_EXECUTE";

  @Override
  public List<TestCase> getTestCases() {
    return Arrays.asList(
        new TestCase(
            "SHOW TABLES IN comparator_tests.__no_such_schema__",
            "SHOW TABLES in a non-existent schema"),
        new TestCase(
            "SHOW SCHEMAS IN __no_such_catalog__", "SHOW SCHEMAS in a non-existent catalog"),
        new TestCase(
            "DESCRIBE TABLE comparator_tests.oss_jdbc_tests.__no_such_table__",
            "DESCRIBE a non-existent table"),
        new TestCase("SET = 'x'", "SET with an invalid (empty) parameter name"),
        new TestCase(
            "SET TIMEZONE = '__no_such_timezone__'",
            "SET a valid conf to an invalid value (bad timezone)"),
        new TestCase(
            "SELECT 1; SELECT 2", "multi-statement script (;-separated) in a single execute()"),
        new TestCase(
            "EXPLAIN SELECT __no_such_column__ FROM " + TABLE, "EXPLAIN of an invalid query"),
        new TestCase(GET_MORE_RESULTS, "getMoreResults() after all results are consumed"),
        new TestCase(GET_UPDATE_COUNT, "getUpdateCount() before execute()"));
  }

  @Override
  public ComparisonResult execute(
      Connection conn1, Connection conn2, TestCase testCase, String label) throws Exception {
    String id = testCase.getIdentifier();
    if (GET_MORE_RESULTS.equals(id)) {
      return compareApi(
          conn1, conn2, label, id, testCase, NegativeStatementOtherProvider::moreResults);
    }
    if (GET_UPDATE_COUNT.equals(id)) {
      return compareApi(
          conn1,
          conn2,
          label,
          id,
          testCase,
          NegativeStatementOtherProvider::updateCountBeforeExecute);
    }
    // SQL command cases: capture execute() per side and compare.
    try (Statement s1 = conn1.createStatement();
        Statement s2 = conn2.createStatement()) {
      Object r1 = Captures.resultOrThrowable(() -> s1.execute(id));
      Object r2 = Captures.resultOrThrowable(() -> s2.execute(id));
      return ResultSetComparator.compare(label, id, testCase.getArgs(), r1, r2);
    }
  }

  /** Runs a per-connection API action on both sides and compares the captured outcomes. */
  private ComparisonResult compareApi(
      Connection conn1,
      Connection conn2,
      String label,
      String id,
      TestCase testCase,
      ApiAction action) {
    Object r1 = Captures.resultOrThrowable(() -> action.run(conn1));
    Object r2 = Captures.resultOrThrowable(() -> action.run(conn2));
    try {
      return ResultSetComparator.compare(label, id, testCase.getArgs(), r1, r2);
    } catch (Exception e) {
      // ResultSetComparator only throws SQLException while comparing ResultSets; these API actions
      // return a Boolean/Integer or a Throwable, so this is unreachable in practice.
      throw new RuntimeException(e);
    } finally {
      Captures.closeIfResultSet(r1);
      Captures.closeIfResultSet(r2);
    }
  }

  @FunctionalInterface
  private interface ApiAction {
    Object run(Connection conn) throws Exception;
  }

  /** Consumes a small ResultSet fully, then calls getMoreResults() and returns its Boolean. */
  private static Object moreResults(Connection conn) throws Exception {
    try (Statement s = conn.createStatement()) {
      s.executeQuery("SELECT 1");
      // Drain is unnecessary for getMoreResults semantics; call it after the single result set.
      return s.getMoreResults();
    }
  }

  /** Calls getUpdateCount() on a fresh statement before any execute(). */
  private static Object updateCountBeforeExecute(Connection conn) throws Exception {
    try (Statement s = conn.createStatement()) {
      return s.getUpdateCount();
    }
  }
}
