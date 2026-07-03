package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.comparator.ComparisonResult;
import com.databricks.jdbc.comparator.ResultSetComparator;
import com.databricks.jdbc.comparator.error.Captures;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;

/**
 * Negative prepared-statement metadata cases — {@code clearParameters()} then execute with a
 * placeholder unbound, and {@code getMetaData()} before execute on invalid SQL / a missing table /
 * a missing column. Each side's outcome (a metadata/result value or a thrown error) is compared.
 *
 * <p>Read-only and safe on the shared connections.
 */
public class NegativePreparedMetadataProvider implements SuiteProvider {

  private static final String TABLE = "comparator_tests.oss_jdbc_tests.test_result_set_types";

  @FunctionalInterface
  private interface PsAction {
    Object run(PreparedStatement ps) throws Exception;
  }

  private static final class Case {
    final String description;
    final String sql;
    final PsAction action;

    Case(String description, String sql, PsAction action) {
      this.description = description;
      this.sql = sql;
      this.action = action;
    }
  }

  private static final List<Case> CASES =
      Arrays.asList(
          new Case(
              "clearParameters() then execute with placeholder unbound",
              "SELECT integer_column FROM " + TABLE + " WHERE integer_column = ?",
              ps -> {
                ps.setInt(1, 42);
                ps.clearParameters();
                return ps.executeQuery();
              }),
          new Case(
              "getMetaData() before execute on invalid SQL",
              "SELET integer_column FROM " + TABLE,
              PreparedStatement::getMetaData),
          new Case(
              "getMetaData() before execute on a missing table",
              "SELECT * FROM comparator_tests.oss_jdbc_tests.__no_such_table__",
              PreparedStatement::getMetaData),
          new Case(
              "getMetaData() before execute on a missing column",
              "SELECT __no_such_column__ FROM " + TABLE,
              PreparedStatement::getMetaData));

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
