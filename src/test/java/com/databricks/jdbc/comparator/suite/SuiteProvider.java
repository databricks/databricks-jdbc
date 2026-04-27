package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.api.impl.DatabricksResultSetMetaData;
import com.databricks.jdbc.comparator.ComparisonResult;
import com.databricks.jdbc.comparator.JDBCDriverComparisonTest;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.List;

/**
 * Defines test cases and execution logic for one category of comparator tests.
 *
 * <p>Each {@link com.databricks.jdbc.comparator.config.TestSuite} enum constant maps to one
 * SuiteProvider implementation. The provider is responsible for creating and closing any JDBC
 * resources (Statements, ResultSets) within {@link #execute}. Connections are managed externally
 * and must NOT be closed by the provider.
 */
public interface SuiteProvider {

  /** Returns all test cases for this suite. Empty list means the suite is skipped. */
  List<TestCase> getTestCases();

  /**
   * Executes a single test case against two connections and returns the comparison result.
   *
   * @param label context label for reporting (e.g., "STATEMENT_SELECT [Default | Thrift vs SEA]")
   */
  ComparisonResult execute(Connection conn1, Connection conn2, TestCase testCase, String label)
      throws Exception;

  /**
   * Asserts CloudFetch expectation on both ResultSets if set on the test case. Call after executing
   * queries but before consuming the ResultSets.
   */
  default void assertCloudFetchExpectation(TestCase testCase, ResultSet rs1, ResultSet rs2)
      throws Exception {
    Boolean expected = testCase.getExpectCloudFetch();
    if (expected == null) {
      return;
    }
    assertCloudFetchOnResultSet(
        expected, rs1, testCase, JDBCDriverComparisonTest.endpointFor("LEFT").getLabel());
    assertCloudFetchOnResultSet(
        expected, rs2, testCase, JDBCDriverComparisonTest.endpointFor("RIGHT").getLabel());
  }

  private static void assertCloudFetchOnResultSet(
      boolean expected, ResultSet rs, TestCase testCase, String label) throws Exception {
    ResultSetMetaData md = rs.getMetaData();
    if (md instanceof DatabricksResultSetMetaData) {
      boolean actual = ((DatabricksResultSetMetaData) md).getIsCloudFetchUsed();
      if (actual != expected) {
        throw new AssertionError(
            "CloudFetch expectation failed on "
                + label
                + ": expected="
                + expected
                + " actual="
                + actual
                + " for: "
                + testCase.getDescription());
      }
    }
  }
}
