package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.comparator.ComparisonResult;
import com.databricks.jdbc.comparator.ResultSetComparator;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * Compares utility SQL commands (SHOW, DESCRIBE, EXPLAIN, SET) between Thrift and SEA.
 *
 * <p>All commands return ResultSets and are compared cell-by-cell via executeQuery.
 */
public class StatementOtherProvider implements SuiteProvider {

  private static final String TABLE = "comparator_tests.oss_jdbc_tests.test_result_set_types";

  @Override
  public List<TestCase> getTestCases() {
    return Arrays.asList(
        new TestCase("SHOW TABLES IN comparator_tests.oss_jdbc_tests", "SHOW TABLES"),
        new TestCase("SHOW SCHEMAS IN comparator_tests", "SHOW SCHEMAS"),
        new TestCase("SHOW CATALOGS", "SHOW CATALOGS"),
        new TestCase("DESCRIBE TABLE " + TABLE, "DESCRIBE TABLE"),
        new TestCase("EXPLAIN SELECT * FROM " + TABLE + " WHERE id = 1", "EXPLAIN"),
        new TestCase("SET QUERY_TAGS['comparator_test'] = 'thrift_sea'", "SET QUERY_TAGS (write)"),
        new TestCase("SET QUERY_TAGS", "SET QUERY_TAGS (read back)"));
    // USE CATALOG/SCHEMA not tested — pass-through commands that alter connection state
  }

  @Override
  public ComparisonResult execute(
      Connection conn1, Connection conn2, TestCase testCase, String label) throws Exception {
    String sql = testCase.getIdentifier();
    try (Statement s1 = conn1.createStatement();
        Statement s2 = conn2.createStatement();
        ResultSet rs1 = s1.executeQuery(sql);
        ResultSet rs2 = s2.executeQuery(sql)) {
      return ResultSetComparator.compare(label, sql, testCase.getArgs(), rs1, rs2);
    }
  }
}
