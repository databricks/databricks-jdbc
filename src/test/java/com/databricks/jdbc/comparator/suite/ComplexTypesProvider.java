package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.comparator.ComparisonResult;
import com.databricks.jdbc.comparator.ResultSetComparator;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * Compares complex type handling (ARRAY, MAP, STRUCT, VARIANT, nested complex) between Thrift and
 * SEA under different EnableComplexDatatypeSupport settings.
 *
 * <p>Runs under COMPLEX_TYPES_ENABLED and COMPLEX_TYPES_DISABLED configs. With complex types
 * disabled, these columns are returned as strings. With complex types enabled, they are returned as
 * java.sql.Array, java.util.Map, java.sql.Struct objects.
 */
public class ComplexTypesProvider implements SuiteProvider {

  private static final String TABLE = "comparator_tests.oss_jdbc_tests.test_result_set_types";

  // All complex columns: flat + nested 3x3 + variant
  private static final String ALL_COMPLEX_COLS =
      "array_column, map_column, struct_column, variant_column, "
          + "array_of_arrays_column, array_of_maps_column, array_of_structs_column, "
          + "map_of_arrays_column, map_of_maps_column, map_of_structs_column, "
          + "struct_with_array_column, struct_with_map_column, struct_with_struct_column";

  @Override
  public List<TestCase> getTestCases() {
    return Arrays.asList(
        new TestCase(
            "SELECT " + ALL_COMPLEX_COLS + " FROM " + TABLE + " WHERE id <= 7 ORDER BY id",
            "All complex types — inline (7 edge case rows)",
            false),
        new TestCase(
            "SELECT " + ALL_COMPLEX_COLS + " FROM " + TABLE + " LIMIT 20000",
            "All complex types — CloudFetch (20K rows)",
            true));
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
