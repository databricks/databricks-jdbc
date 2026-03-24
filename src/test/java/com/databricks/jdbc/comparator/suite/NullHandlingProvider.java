package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.comparator.ComparisonResult;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Compares NULL handling between Thrift and SEA — verifies getObject() returns null and wasNull()
 * returns correct values for both NULL and non-NULL rows.
 */
public class NullHandlingProvider implements SuiteProvider {

  private static final String TABLE = "comparator_tests.oss_jdbc_tests.test_result_set_types";

  @Override
  public List<TestCase> getTestCases() {
    return Arrays.asList(
        new TestCase("NULL_ROW", "wasNull() on all-NULL row (id=4)"),
        new TestCase("NON_NULL_ROW", "wasNull() on non-NULL row (id=1)"));
  }

  @Override
  public ComparisonResult execute(
      Connection conn1, Connection conn2, TestCase testCase, String label) throws Exception {
    int rowId = testCase.getIdentifier().equals("NULL_ROW") ? 4 : 1;
    // Select non-interval columns to avoid IntervalConverter CloudFetch bug
    String sql =
        "SELECT id, varchar_column, boolean_column, integer_column, bigint_column, "
            + "smallint_column, tinyint_column, float_column, double_column, decimal_column, "
            + "date_column, timestamp_column, timestamp_ntz_column, binary_column "
            + "FROM "
            + TABLE
            + " WHERE id = "
            + rowId;

    List<String> differences = new ArrayList<>();

    try (Statement s1 = conn1.createStatement();
        Statement s2 = conn2.createStatement();
        ResultSet rs1 = s1.executeQuery(sql);
        ResultSet rs2 = s2.executeQuery(sql)) {

      if (!rs1.next() || !rs2.next()) {
        differences.add("No row returned for id=" + rowId);
      } else {
        ResultSetMetaData md = rs1.getMetaData();
        int colCount = md.getColumnCount();

        for (int i = 1; i <= colCount; i++) {
          String colName = md.getColumnName(i);

          Object val1 = rs1.getObject(i);
          boolean wasNull1 = rs1.wasNull();

          Object val2 = rs2.getObject(i);
          boolean wasNull2 = rs2.wasNull();

          if (wasNull1 != wasNull2) {
            differences.add(colName + ": wasNull() mismatch: " + wasNull1 + " vs " + wasNull2);
          }

          // Verify consistency: if value is null, wasNull should be true
          if (val1 == null && !wasNull1) {
            differences.add(colName + ": Thrift getObject()=null but wasNull()=false");
          }
          if (val2 == null && !wasNull2) {
            differences.add(colName + ": SEA getObject()=null but wasNull()=false");
          }
          if (val1 != null && wasNull1) {
            differences.add(colName + ": Thrift getObject()!=null but wasNull()=true");
          }
          if (val2 != null && wasNull2) {
            differences.add(colName + ": SEA getObject()!=null but wasNull()=true");
          }
        }
      }
    }

    ComparisonResult result =
        new ComparisonResult(label, testCase.getIdentifier(), testCase.getArgs());
    result.dataDifferences = differences;
    return result;
  }
}
