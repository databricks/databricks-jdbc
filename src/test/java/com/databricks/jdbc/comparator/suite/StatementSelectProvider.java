package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.comparator.ComparisonResult;
import com.databricks.jdbc.comparator.ResultSetComparator;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * Compares SELECT query results between Thrift and SEA connections at various result sizes.
 *
 * <p>Uses the real test_result_set_types table (32 columns, 150K+ rows) to exercise the full
 * storage → Arrow → JDBC pipeline including CloudFetch transitions.
 */
public class StatementSelectProvider implements SuiteProvider {

  private static final String TABLE = "comparator_tests.oss_jdbc_tests.test_result_set_types";

  @Override
  public List<TestCase> getTestCases() {
    return Arrays.asList(
        new TestCase(
            "SELECT * FROM " + TABLE + " WHERE 1=0", "Empty result (0 rows, 32 columns)", false),
        new TestCase(
            "SELECT * FROM " + TABLE + " WHERE id = 1",
            "Single row (all types, normal values)",
            false),
        new TestCase(
            "SELECT * FROM " + TABLE + " WHERE id <= 7 ORDER BY id",
            "Edge case rows (7 rows — normal, nulls, max, min, empty, special)",
            false),
        new TestCase(
            "SELECT * FROM " + TABLE + " ORDER BY id LIMIT 1000", "Inline result (1K rows)", false),
        new TestCase(
            "SELECT * FROM " + TABLE + " ORDER BY id LIMIT 30000",
            "CloudFetch result (30K rows)",
            true),
        new TestCase(
            "SELECT * FROM " + TABLE + " ORDER BY id", "CloudFetch result (150K+ rows)", true));
  }

  @Override
  public ComparisonResult execute(
      Connection conn1, Connection conn2, TestCase testCase, String label) throws Exception {
    String query = testCase.getIdentifier();
    try (Statement stmt1 = conn1.createStatement();
        Statement stmt2 = conn2.createStatement();
        ResultSet rs1 = stmt1.executeQuery(query);
        ResultSet rs2 = stmt2.executeQuery(query)) {
      assertCloudFetchExpectation(testCase, rs1, rs2);
      return ResultSetComparator.compare(label, query, testCase.getArgs(), rs1, rs2);
    }
  }
}
