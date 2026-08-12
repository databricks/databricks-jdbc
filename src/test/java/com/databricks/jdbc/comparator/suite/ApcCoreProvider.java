package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.comparator.ComparisonResult;
import com.databricks.jdbc.comparator.ResultSetComparator;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/** Small self-contained SELECT suite for APC direct-Thrift versus SEA/Reyder parity. */
public class ApcCoreProvider implements SuiteProvider {

  @Override
  public List<TestCase> getTestCases() {
    return Arrays.asList(
        new TestCase("SELECT 1 AS value", "Single integer row"),
        new TestCase(
            "SELECT * FROM VALUES (1, 'alpha'), (2, 'beta') AS t(id, label) ORDER BY id",
            "Ordered literal rows"),
        new TestCase(
            "SELECT CAST(NULL AS STRING) AS null_value, true AS bool_value",
            "Null and boolean values"),
        new TestCase("SELECT 1 AS value WHERE false", "Empty result with a declared column"));
  }

  @Override
  public ComparisonResult execute(
      Connection conn1, Connection conn2, TestCase testCase, String label) throws Exception {
    String query = testCase.getIdentifier();
    try (Statement stmt1 = conn1.createStatement();
        Statement stmt2 = conn2.createStatement();
        ResultSet left = stmt1.executeQuery(query);
        ResultSet right = stmt2.executeQuery(query)) {
      return ResultSetComparator.compare(label, query, testCase.getArgs(), left, right);
    }
  }
}
