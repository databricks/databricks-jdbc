package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.comparator.ComparisonResult;
import com.databricks.jdbc.comparator.ResultSetComparator;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Compares PreparedStatement parameter binding for supported Databricks types between Thrift and
 * SEA.
 *
 * <p>Uses the real test_result_set_types table — each test filters on a typed column using the
 * appropriate setXxx method and compares the ResultSet.
 */
public class PreparedStatementTypesProvider implements SuiteProvider {

  private static final String TABLE = "comparator_tests.oss_jdbc_tests.test_result_set_types";

  @FunctionalInterface
  private interface ParamSetter {
    void set(PreparedStatement ps) throws SQLException;
  }

  /** Defines a single type test: which column to filter, how to set the parameter, description. */
  private static class TypeTest {
    final String column;
    final String description;
    final ParamSetter setter;
    final String sqlOverride;
    final Boolean expectCloudFetch;

    TypeTest(String column, String description, ParamSetter setter) {
      this(column, description, setter, null, null);
    }

    TypeTest(String column, String description, ParamSetter setter, String sqlOverride) {
      this(column, description, setter, sqlOverride, null);
    }

    TypeTest(
        String column,
        String description,
        ParamSetter setter,
        String sqlOverride,
        Boolean expectCloudFetch) {
      this.column = column;
      this.description = description;
      this.setter = setter;
      this.sqlOverride = sqlOverride;
      this.expectCloudFetch = expectCloudFetch;
    }
  }

  // All type tests defined in one place — column, description, and setter lambda
  private static final List<TypeTest> TYPE_TESTS =
      Arrays.asList(
          new TypeTest("integer_column", "setInt(42)", ps -> ps.setInt(1, 42)),
          new TypeTest(
              "bigint_column", "setLong(123456789012345)", ps -> ps.setLong(1, 123456789012345L)),
          new TypeTest("smallint_column", "setShort(100)", ps -> ps.setShort(1, (short) 100)),
          new TypeTest("tinyint_column", "setByte(10)", ps -> ps.setByte(1, (byte) 10)),
          new TypeTest("float_column", "setFloat(3.14)", ps -> ps.setFloat(1, 3.14f)),
          new TypeTest("double_column", "setDouble(2.718)", ps -> ps.setDouble(1, 2.718281828)),
          new TypeTest(
              "decimal_column",
              "setBigDecimal(99.99)",
              ps -> ps.setBigDecimal(1, new BigDecimal("99.99"))),
          new TypeTest("varchar_column", "setString(hello)", ps -> ps.setString(1, "hello")),
          new TypeTest("boolean_column", "setBoolean(true)", ps -> ps.setBoolean(1, true)),
          new TypeTest(
              "date_column",
              "setDate(2024-01-15)",
              ps -> ps.setDate(1, Date.valueOf("2024-01-15"))),
          new TypeTest(
              "timestamp_column",
              "setTimestamp(2024-01-15 10:30:00)",
              ps -> ps.setTimestamp(1, Timestamp.valueOf("2024-01-15 10:30:00"))),
          new TypeTest(
              "integer_column",
              "setNull (id=4, all nulls row)",
              ps -> ps.setInt(1, 4),
              "SELECT id, integer_column FROM "
                  + TABLE
                  + " WHERE integer_column IS NULL AND id = ?"),
          new TypeTest("varchar_column", "setObject(String)", ps -> ps.setObject(1, "hello")),
          new TypeTest("integer_column", "setObject(Integer)", ps -> ps.setObject(1, 42)),
          new TypeTest(
              "timestamp_ntz_column",
              "setTimestamp (TIMESTAMP_NTZ)",
              ps -> ps.setTimestamp(1, Timestamp.valueOf("2024-01-15 10:30:00"))),
          new TypeTest(
              "ym_interval_column",
              "setString (INTERVAL YEAR TO MONTH)",
              ps -> ps.setString(1, "2-6"),
              "SELECT ym_interval_column FROM "
                  + TABLE
                  + " WHERE ym_interval_column = CAST(? AS INTERVAL YEAR TO MONTH)"),
          new TypeTest(
              "dt_interval_column",
              "setString (INTERVAL DAY TO SECOND)",
              ps -> ps.setString(1, "3 12:30:15.000000000"),
              "SELECT dt_interval_column FROM "
                  + TABLE
                  + " WHERE dt_interval_column = CAST(? AS INTERVAL DAY TO SECOND)"),
          new TypeTest(
              "geometry_column",
              "setString (GEOMETRY via ST_GeomFromText)",
              ps -> ps.setString(1, "POINT (1.5 2.5)"),
              "SELECT geometry_column FROM "
                  + TABLE
                  + " WHERE ST_Equals(geometry_column, ST_GeomFromText(?))"),
          new TypeTest(
              "geography_column",
              "setString (GEOGRAPHY via ST_AsText)",
              ps -> ps.setString(1, "POINT(1.5 2.5)"),
              "SELECT geography_column FROM " + TABLE + " WHERE ST_AsText(geography_column) = ?"),
          new TypeTest(
              "id",
              "PreparedStatement CloudFetch (multi-type WHERE, large result)",
              ps -> {
                ps.setInt(1, 0);
                ps.setLong(2, 0L);
                ps.setShort(3, (short) 0);
                ps.setByte(4, (byte) 0);
                ps.setFloat(5, 0f);
                ps.setDouble(6, 0.0);
                ps.setBigDecimal(7, BigDecimal.ZERO);
                ps.setString(8, "bulk%");
                ps.setBoolean(9, true);
                ps.setDate(10, Date.valueOf("2019-01-01"));
                ps.setTimestamp(11, Timestamp.valueOf("2019-01-01 00:00:00"));
              },
              "SELECT id, varchar_column, `varchar-column`, boolean_column, "
                  + "integer_column, bigint_column, smallint_column, tinyint_column, "
                  + "float_column, double_column, decimal_column, date_column, "
                  + "timestamp_column, timestamp_ntz_column, binary_column, "
                  + "array_column, map_column, struct_column, variant_column, "
                  + "geometry_column, geography_column FROM "
                  + TABLE
                  + " WHERE integer_column > ?"
                  + " AND bigint_column > ?"
                  + " AND smallint_column >= ?"
                  + " AND tinyint_column >= ?"
                  + " AND float_column > ?"
                  + " AND double_column > ?"
                  + " AND decimal_column > ?"
                  + " AND varchar_column LIKE ?"
                  + " AND boolean_column = ?"
                  + " AND date_column > ?"
                  + " AND timestamp_column > ?",
              true));

  // setBytes requires supportManyParameters=1 — skipped for default config
  // ARRAY, MAP, STRUCT, VARIANT — no JDBC setter for WHERE clause filtering

  @Override
  public List<TestCase> getTestCases() {
    return TYPE_TESTS.stream()
        .map(t -> new TestCase(t.description, t.description, t.expectCloudFetch))
        .collect(Collectors.toList());
  }

  @Override
  public ComparisonResult execute(
      Connection conn1, Connection conn2, TestCase testCase, String label) throws Exception {
    TypeTest test =
        TYPE_TESTS.stream()
            .filter(t -> t.description.equals(testCase.getIdentifier()))
            .findFirst()
            .orElseThrow(
                () -> new IllegalArgumentException("Unknown test: " + testCase.getIdentifier()));

    String sql =
        test.sqlOverride != null
            ? test.sqlOverride
            : "SELECT " + test.column + " FROM " + TABLE + " WHERE " + test.column + " = ?";

    try (PreparedStatement ps1 = conn1.prepareStatement(sql);
        PreparedStatement ps2 = conn2.prepareStatement(sql)) {
      test.setter.set(ps1);
      test.setter.set(ps2);

      try (ResultSet rs1 = ps1.executeQuery();
          ResultSet rs2 = ps2.executeQuery()) {
        assertCloudFetchExpectation(testCase, rs1, rs2);
        return ResultSetComparator.compare(label, sql, testCase.getArgs(), rs1, rs2);
      }
    }
  }
}
