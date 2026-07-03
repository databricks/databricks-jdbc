package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.comparator.ComparisonResult;
import com.databricks.jdbc.comparator.error.CapturedOutcome;
import com.databricks.jdbc.comparator.error.Captures;
import com.databricks.jdbc.comparator.error.ErrorDiffs;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Negative row-limit cases — invalid {@code setMaxRows}/{@code setLargeMaxRows} values that should
 * be rejected, per JDBC ({@code setMaxRows} requires a non-negative limit). Compares how the two
 * endpoints surface the rejection (at the setter or at execute) via {@link ErrorDiffs}.
 *
 * <p>Read-only: sets a bad limit then runs a SELECT; safe on the shared connections. The whole
 * set-then-execute sequence is captured per side since the failure may originate at either point.
 */
public class NegativeStatementSelectTruncatedProvider implements SuiteProvider {

  private static final String TABLE = "comparator_tests.oss_jdbc_tests.test_result_set_types";
  private static final String QUERY = "SELECT * FROM " + TABLE + " ORDER BY id LIMIT 100";

  /** Applies a bad row-limit to a statement then runs the query; returns a ResultSet or throws. */
  @FunctionalInterface
  private interface LimitAction {
    Object run(Statement stmt) throws Exception;
  }

  private static final class Case {
    final String description;
    final LimitAction action;

    Case(String description, LimitAction action) {
      this.description = description;
      this.action = action;
    }
  }

  private static final List<Case> CASES =
      Arrays.asList(
          new Case(
              "setMaxRows(-1) — negative row limit",
              stmt -> {
                stmt.setMaxRows(-1);
                return stmt.executeQuery(QUERY);
              }),
          new Case(
              "setLargeMaxRows(-1) — negative large row limit",
              stmt -> {
                stmt.setLargeMaxRows(-1L);
                return stmt.executeQuery(QUERY);
              }),
          new Case(
              "setMaxRows(MIN_VALUE) — extreme negative row limit",
              stmt -> {
                stmt.setMaxRows(Integer.MIN_VALUE);
                return stmt.executeQuery(QUERY);
              }));

  @Override
  public List<TestCase> getTestCases() {
    return CASES.stream()
        .map(c -> new TestCase(c.description, c.description))
        .collect(Collectors.toList());
  }

  @Override
  public ComparisonResult execute(
      Connection conn1, Connection conn2, TestCase testCase, String label) throws Exception {
    Case c =
        CASES.stream()
            .filter(x -> x.description.equals(testCase.getIdentifier()))
            .findFirst()
            .orElseThrow(
                () -> new IllegalArgumentException("Unknown case: " + testCase.getIdentifier()));

    try (Statement stmt1 = conn1.createStatement();
        Statement stmt2 = conn2.createStatement()) {
      CapturedOutcome left = Captures.capture(() -> c.action.run(stmt1));
      CapturedOutcome right = Captures.capture(() -> c.action.run(stmt2));
      try {
        ComparisonResult result = new ComparisonResult(label, c.description, testCase.getArgs());
        ErrorDiffs.foldInto(result, left, right, "result ", "");
        return result;
      } finally {
        Captures.closeIfResultSet(left.value());
        Captures.closeIfResultSet(right.value());
      }
    }
  }
}
