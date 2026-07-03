package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.comparator.ComparisonResult;
import com.databricks.jdbc.comparator.ResultSetComparator;
import com.databricks.jdbc.comparator.error.Captures;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * Negative SELECT cases — inputs that should make {@code executeQuery} fail, so the two endpoints'
 * error behavior (class, SQLState, code, message) is compared instead of their success output.
 *
 * <p>Read-only: every case runs on the shared connections and provokes an error without mutating
 * server state, so it cannot poison later suites.
 */
public class NegativeStatementSelectProvider implements SuiteProvider {

  private static final String TABLE = "comparator_tests.oss_jdbc_tests.test_result_set_types";

  @Override
  public List<TestCase> getTestCases() {
    return Arrays.asList(
        new TestCase(
            "SELECT * FROM comparator_tests.oss_jdbc_tests.__no_such_table__",
            "SELECT from a non-existent table"),
        new TestCase("SELET 1", "Syntax error (SELET typo)"),
        new TestCase("SELECT 1/0", "Division by zero"),
        new TestCase("SELECT CAST('x' AS INT)", "Runtime cast failure (non-numeric to INT)"),
        new TestCase(
            "SELECT __no_such_column__ FROM " + TABLE, "Reference to a non-existent column"),
        // Wrong method: executeQuery on a statement that does not produce a ResultSet.
        new TestCase(
            "INSERT INTO " + TABLE + " (id) VALUES (1)",
            "Wrong method: executeQuery on an INSERT"));
  }

  @Override
  public ComparisonResult execute(
      Connection conn1, Connection conn2, TestCase testCase, String label) throws Exception {
    String query = testCase.getIdentifier();
    try (Statement stmt1 = conn1.createStatement();
        Statement stmt2 = conn2.createStatement()) {
      Object r1 = Captures.resultOrThrowable(() -> stmt1.executeQuery(query));
      Object r2 = Captures.resultOrThrowable(() -> stmt2.executeQuery(query));
      try {
        return ResultSetComparator.compare(label, query, testCase.getArgs(), r1, r2);
      } finally {
        Captures.closeIfResultSet(r1);
        Captures.closeIfResultSet(r2);
      }
    }
  }
}
