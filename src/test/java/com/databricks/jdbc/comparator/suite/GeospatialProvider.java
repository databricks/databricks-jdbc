package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.comparator.ComparisonResult;
import com.databricks.jdbc.comparator.ResultSetComparator;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * Compares geospatial type handling (GEOMETRY, GEOGRAPHY) between Thrift and SEA under different
 * EnableGeoSpatialSupport settings.
 *
 * <p>Runs under GEOSPATIAL_ENABLED and GEOSPATIAL_DISABLED configs. With geospatial disabled, geo
 * columns are returned as STRING. With geospatial enabled, they are returned as
 * IGeometry/IGeography objects.
 */
public class GeospatialProvider implements SuiteProvider {

  private static final String TABLE = "comparator_tests.oss_jdbc_tests.test_result_set_types";

  @Override
  public List<TestCase> getTestCases() {
    return Arrays.asList(
        new TestCase(
            "SELECT geometry_column, geography_column FROM " + TABLE + " WHERE id <= 7 ORDER BY id",
            "Geo columns — inline (7 rows)",
            false),
        new TestCase(
            "SELECT geometry_column, geography_column FROM " + TABLE,
            "Geo columns — CloudFetch (150K+ rows)",
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
      assertCloudFetchExpectation(testCase, rs1, rs2);
      return ResultSetComparator.compare(label, sql, testCase.getArgs(), rs1, rs2);
    }
  }
}
