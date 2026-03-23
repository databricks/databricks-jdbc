package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.comparator.ComparisonResult;
import com.databricks.jdbc.comparator.ResultSetComparator;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;

/** Compares SELECT query results between Thrift and SEA connections. */
public class StatementSelectProvider implements SuiteProvider {

  @Override
  public List<TestCase> getTestCases() {
    return Collections.singletonList(
        new TestCase(
            "SELECT "
                + "CAST(id AS INT) AS int_col, "
                + "CAST(CONCAT('hello_', id) AS STRING) AS string_col, "
                + "CAST(id * 1.11 AS DOUBLE) AS double_col, "
                + "CAST(id % 2 = 0 AS BOOLEAN) AS bool_col, "
                + "CAST(DATE_ADD('2025-01-01', id) AS DATE) AS date_col, "
                + "CAST(id * 1.23 AS DECIMAL(10,2)) AS decimal_col "
                + "FROM (SELECT EXPLODE(SEQUENCE(1, 10)) AS id)",
            "Synthetic 6-type 10-row query"));
  }

  @Override
  public ComparisonResult execute(
      Connection conn1, Connection conn2, TestCase testCase, String label) throws Exception {
    String query = testCase.getIdentifier();
    try (Statement stmt1 = conn1.createStatement();
        Statement stmt2 = conn2.createStatement();
        ResultSet rs1 = stmt1.executeQuery(query);
        ResultSet rs2 = stmt2.executeQuery(query)) {
      return ResultSetComparator.compare(label, query, testCase.getArgs(), rs1, rs2);
    }
  }
}
