package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.comparator.ComparisonResult;
import com.databricks.jdbc.comparator.ResultSetComparator;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * Compares SELECT query results with Statement truncation methods between Thrift and SEA.
 *
 * <p>Tests setMaxRows and setLargeMaxRows at both inline and CloudFetch sizes.
 */
public class StatementSelectTruncatedProvider implements SuiteProvider {

  private static final String TABLE = "comparator_tests.oss_jdbc_tests.test_result_set_types";
  private static final String QUERY = "SELECT * FROM " + TABLE;

  @Override
  public List<TestCase> getTestCases() {
    return Arrays.asList(
        // setMaxRows — inline
        new TestCase(
            "setMaxRows",
            new Object[] {"setMaxRows", 100},
            "setMaxRows(100) — inline, truncated from 150K to 100 rows",
            false),
        // setMaxRows — CloudFetch
        new TestCase(
            "setMaxRows",
            new Object[] {"setMaxRows", 20000},
            "setMaxRows(20000) — CloudFetch, truncated from 150K to 20K rows",
            true),
        // setLargeMaxRows — inline
        new TestCase(
            "setLargeMaxRows",
            new Object[] {"setLargeMaxRows", 100L},
            "setLargeMaxRows(100) — inline, truncated from 150K to 100 rows",
            false),
        // setLargeMaxRows — CloudFetch
        new TestCase(
            "setLargeMaxRows",
            new Object[] {"setLargeMaxRows", 20000L},
            "setLargeMaxRows(20000) — CloudFetch, truncated from 150K to 20K rows",
            true));
  }

  @Override
  public ComparisonResult execute(
      Connection conn1, Connection conn2, TestCase testCase, String label) throws Exception {
    Object[] args = testCase.getArgs();
    String method = (String) args[0];

    try (Statement stmt1 = conn1.createStatement();
        Statement stmt2 = conn2.createStatement()) {
      String query;
      switch (method) {
        case "setMaxRows":
          int maxRows = (int) args[1];
          stmt1.setMaxRows(maxRows);
          stmt2.setMaxRows(maxRows);
          query = QUERY;
          break;
        case "setLargeMaxRows":
          long largeMaxRows = (long) args[1];
          stmt1.setLargeMaxRows(largeMaxRows);
          stmt2.setLargeMaxRows(largeMaxRows);
          query = QUERY;
          break;
        default:
          throw new IllegalArgumentException("Unknown truncation method: " + method);
      }

      try (ResultSet rs1 = stmt1.executeQuery(query);
          ResultSet rs2 = stmt2.executeQuery(query)) {
        assertCloudFetchExpectation(testCase, rs1, rs2);
        return ResultSetComparator.compare(
            label, query + " [" + method + "=" + args[1] + "]", testCase.getArgs(), rs1, rs2);
      }
    }
  }
}
