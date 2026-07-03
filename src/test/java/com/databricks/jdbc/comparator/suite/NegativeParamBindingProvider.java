package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.comparator.ComparisonResult;
import com.databricks.jdbc.comparator.ResultSetComparator;
import com.databricks.jdbc.comparator.error.Captures;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;

/**
 * Negative parameter-binding cases — bad {@code setXxx} usage or missing bindings that should fail
 * at bind or execute time. The failure can originate at the setter or at {@code executeQuery}, so
 * the whole prepare → set → execute sequence is captured per side as one unit and the endpoints'
 * error behavior is compared.
 *
 * <p>Read-only: each case only reads (or fails before reading), so it is safe on the shared
 * connections.
 */
public class NegativeParamBindingProvider implements SuiteProvider {

  private static final String TABLE = "comparator_tests.oss_jdbc_tests.test_result_set_types";

  /** A binding action that may throw at set or execute time; returns a ResultSet on success. */
  @FunctionalInterface
  private interface BindAndRun {
    Object run(PreparedStatement ps) throws Exception;
  }

  private static final class Case {
    final String description;
    final String sql;
    final BindAndRun action;

    Case(String description, String sql, BindAndRun action) {
      this.description = description;
      this.sql = sql;
      this.action = action;
    }
  }

  private static final List<Case> CASES =
      Arrays.asList(
          new Case(
              "setInt at out-of-range parameter index (99)",
              "SELECT integer_column FROM " + TABLE + " WHERE integer_column = ?",
              ps -> {
                ps.setInt(99, 42);
                return ps.executeQuery();
              }),
          new Case(
              "Fewer parameters bound than placeholders",
              "SELECT * FROM " + TABLE + " WHERE integer_column = ? AND bigint_column = ?",
              ps -> {
                ps.setInt(1, 42); // second placeholder left unbound
                return ps.executeQuery();
              }),
          new Case(
              "Type mismatch: setString into a numeric column",
              "SELECT integer_column FROM " + TABLE + " WHERE integer_column = ?",
              ps -> {
                ps.setString(1, "not_a_number");
                return ps.executeQuery();
              }),
          new Case(
              "setBigDecimal exceeding column precision/scale",
              "SELECT decimal_column FROM " + TABLE + " WHERE decimal_column = ?",
              ps -> {
                ps.setBigDecimal(1, new BigDecimal("123456789012345.678901234567890"));
                return ps.executeQuery();
              }));

  @Override
  public List<TestCase> getTestCases() {
    return CASES.stream()
        .map(c -> new TestCase(c.description, c.description))
        .collect(java.util.stream.Collectors.toList());
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

    try (PreparedStatement ps1 = conn1.prepareStatement(c.sql);
        PreparedStatement ps2 = conn2.prepareStatement(c.sql)) {
      Object r1 = Captures.resultOrThrowable(() -> c.action.run(ps1));
      Object r2 = Captures.resultOrThrowable(() -> c.action.run(ps2));
      try {
        return ResultSetComparator.compare(label, c.description, testCase.getArgs(), r1, r2);
      } finally {
        Captures.closeIfResultSet(r1);
        Captures.closeIfResultSet(r2);
      }
    }
  }
}
