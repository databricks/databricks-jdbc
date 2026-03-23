package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.comparator.ComparisonResult;
import java.sql.Connection;
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
}
