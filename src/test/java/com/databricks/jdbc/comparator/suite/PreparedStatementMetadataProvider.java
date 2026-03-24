package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.comparator.ComparisonResult;
import com.databricks.jdbc.comparator.ResultSetComparator;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Compares PreparedStatement metadata methods between Thrift and SEA.
 *
 * <p>Tests getMetaData() (before and after execution), getParameterMetaData(), and
 * clearParameters().
 */
public class PreparedStatementMetadataProvider implements SuiteProvider {

  private static final String TABLE = "comparator_tests.oss_jdbc_tests.test_result_set_types";
  // SELECT * with all settable types as parameters in WHERE for comprehensive metadata coverage
  private static final String QUERY =
      "SELECT * FROM "
          + TABLE
          + " WHERE integer_column = ?"
          + " AND bigint_column = ?"
          + " AND smallint_column = ?"
          + " AND tinyint_column = ?"
          + " AND float_column = ?"
          + " AND double_column = ?"
          + " AND decimal_column = ?"
          + " AND varchar_column = ?"
          + " AND boolean_column = ?"
          + " AND date_column = ?"
          + " AND timestamp_column = ?"
          + " AND timestamp_ntz_column = ?"
          + " AND ST_Equals(geometry_column, ST_GeomFromText(?))"
          + " AND ST_AsText(geography_column) = ?";

  @Override
  public List<TestCase> getTestCases() {
    return Arrays.asList(
        new TestCase("GET_METADATA_BEFORE_EXEC", "getMetaData() before execution"),
        new TestCase("GET_METADATA_AFTER_EXEC", "getMetaData() after execution"),
        new TestCase("GET_PARAMETER_METADATA", "getParameterMetaData() after setInt"),
        new TestCase("CLEAR_PARAMETERS", "clearParameters() then getParameterMetaData()"));
  }

  @Override
  public ComparisonResult execute(
      Connection conn1, Connection conn2, TestCase testCase, String label) throws Exception {
    String id = testCase.getIdentifier();
    List<String> differences = new ArrayList<>();

    switch (id) {
      case "GET_METADATA_BEFORE_EXEC":
        compareMetadataBeforeExec(conn1, conn2, differences);
        break;
      case "GET_METADATA_AFTER_EXEC":
        compareMetadataAfterExec(conn1, conn2, differences, label);
        break;
      case "GET_PARAMETER_METADATA":
        compareParameterMetadata(conn1, conn2, differences);
        break;
      case "CLEAR_PARAMETERS":
        compareClearParameters(conn1, conn2, differences);
        break;
      default:
        throw new IllegalArgumentException("Unknown test: " + id);
    }

    ComparisonResult result = new ComparisonResult(label, id, testCase.getArgs());
    result.dataDifferences = differences;
    return result;
  }

  /** Compares getMetaData() before executing the query (triggers DESCRIBE QUERY). */
  private void compareMetadataBeforeExec(
      Connection conn1, Connection conn2, List<String> differences) throws Exception {
    try (PreparedStatement ps1 = conn1.prepareStatement(QUERY);
        PreparedStatement ps2 = conn2.prepareStatement(QUERY)) {
      ComparisonResult result =
          ResultSetComparator.compareMetadata(
              "before exec", QUERY, new Object[] {}, ps1.getMetaData(), ps2.getMetaData());
      if (result.hasDifferences()) {
        differences.addAll(result.metadataDifferences);
      }
    }
  }

  /** Compares getMetaData() after executing — uses ResultSetComparator for metadata + data. */
  private void compareMetadataAfterExec(
      Connection conn1, Connection conn2, List<String> differences, String label) throws Exception {
    try (PreparedStatement ps1 = conn1.prepareStatement(QUERY);
        PreparedStatement ps2 = conn2.prepareStatement(QUERY)) {
      setAllParams(ps1);
      setAllParams(ps2);
      try (ResultSet rs1 = ps1.executeQuery();
          ResultSet rs2 = ps2.executeQuery()) {
        ComparisonResult result =
            ResultSetComparator.compare(label, QUERY, new Object[] {}, rs1, rs2);
        if (result.hasDifferences()) {
          differences.addAll(result.metadataDifferences);
          differences.addAll(result.dataDifferences);
        }
      }
    }
  }

  /** Compares getParameterMetaData() after setting a parameter. */
  private void compareParameterMetadata(
      Connection conn1, Connection conn2, List<String> differences) throws SQLException {
    try (PreparedStatement ps1 = conn1.prepareStatement(QUERY);
        PreparedStatement ps2 = conn2.prepareStatement(QUERY)) {
      setAllParams(ps1);
      setAllParams(ps2);

      ParameterMetaData pmd1 = ps1.getParameterMetaData();
      ParameterMetaData pmd2 = ps2.getParameterMetaData();

      if (pmd1.getParameterCount() != pmd2.getParameterCount()) {
        differences.add(
            "Parameter count mismatch: "
                + pmd1.getParameterCount()
                + " vs "
                + pmd2.getParameterCount());
      }

      for (int i = 1; i <= pmd1.getParameterCount(); i++) {
        compareField(
            differences,
            "param " + i + " type",
            pmd1.getParameterType(i),
            pmd2.getParameterType(i));
        compareField(
            differences,
            "param " + i + " typeName",
            pmd1.getParameterTypeName(i),
            pmd2.getParameterTypeName(i));
        compareField(
            differences,
            "param " + i + " className",
            pmd1.getParameterClassName(i),
            pmd2.getParameterClassName(i));
        compareField(
            differences,
            "param " + i + " mode",
            pmd1.getParameterMode(i),
            pmd2.getParameterMode(i));
        compareField(
            differences, "param " + i + " precision", pmd1.getPrecision(i), pmd2.getPrecision(i));
        compareField(differences, "param " + i + " scale", pmd1.getScale(i), pmd2.getScale(i));
        compareField(
            differences, "param " + i + " nullable", pmd1.isNullable(i), pmd2.isNullable(i));
      }
    }
  }

  /** Compares that clearParameters() works and parameter count resets. */
  private void compareClearParameters(Connection conn1, Connection conn2, List<String> differences)
      throws SQLException {
    try (PreparedStatement ps1 = conn1.prepareStatement(QUERY);
        PreparedStatement ps2 = conn2.prepareStatement(QUERY)) {
      setAllParams(ps1);
      setAllParams(ps2);

      int countBefore1 = ps1.getParameterMetaData().getParameterCount();
      int countBefore2 = ps2.getParameterMetaData().getParameterCount();

      // Clear
      ps1.clearParameters();
      ps2.clearParameters();

      int countAfter1 = ps1.getParameterMetaData().getParameterCount();
      int countAfter2 = ps2.getParameterMetaData().getParameterCount();

      compareField(differences, "param count before clear", countBefore1, countBefore2);
      compareField(differences, "param count after clear", countAfter1, countAfter2);
    }
  }

  /** Sets all 14 parameters matching row 1 values. */
  private void setAllParams(PreparedStatement ps) throws SQLException {
    ps.setInt(1, 42); // integer_column
    ps.setLong(2, 123456789012345L); // bigint_column
    ps.setShort(3, (short) 100); // smallint_column
    ps.setByte(4, (byte) 10); // tinyint_column
    ps.setFloat(5, 3.14f); // float_column
    ps.setDouble(6, 2.718281828); // double_column
    ps.setBigDecimal(7, new BigDecimal("99.99")); // decimal_column
    ps.setString(8, "hello"); // varchar_column
    ps.setBoolean(9, true); // boolean_column
    ps.setDate(10, Date.valueOf("2024-01-15")); // date_column
    ps.setTimestamp(11, Timestamp.valueOf("2024-01-15 10:30:00")); // timestamp_column
    ps.setTimestamp(12, Timestamp.valueOf("2024-01-15 10:30:00")); // timestamp_ntz_column
    ps.setString(13, "POINT (1.5 2.5)"); // geometry via ST_GeomFromText
    ps.setString(14, "POINT(1.5 2.5)"); // geography via ST_AsText
  }

  private void compareField(List<String> differences, String name, Object v1, Object v2) {
    if (!java.util.Objects.equals(v1, v2)) {
      differences.add(name + " mismatch: " + v1 + " vs " + v2);
    }
  }
}
