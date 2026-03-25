package com.databricks.jdbc.comparator.suite;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Argument variant data for DatabaseMetaData comparator tests.
 *
 * <p>Each static method returns the list of argument combinations for one DatabaseMetaData method.
 * Variant lists match the test plan's 9 argument variant types: null, %, empty, exact (hyphen),
 * uppercase, underscore wildcard, prefix pattern, escaped underscore, nonexistent.
 *
 * <p>Workspace: comparator_tests / comparator-tests with oss_jdbc_tests / oss-jdbc-tests schemas.
 */
public class DatabaseMetaDataParams {

  // ---------------------------------------------------------------------------
  // Argument variant lists
  // ---------------------------------------------------------------------------

  static final List<Object> CATALOG_VARIANTS =
      Arrays.asList(
          null,
          "%",
          "",
          "comparator-tests",
          "COMPARATOR-TESTS",
          "comparator_tests",
          "comp%",
          "comparator\\_tests",
          "nonexistent");

  static final List<Object> SCHEMA_PATTERN_VARIANTS =
      Arrays.asList(
          null,
          "%",
          "",
          "oss-jdbc-tests",
          "OSS-JDBC-TESTS",
          "oss_jdbc_tests",
          "oss%",
          "oss\\_jdbc\\_tests",
          "nonexistent");

  static final List<Object> TABLE_VARIANTS =
      Arrays.asList(
          null,
          "%",
          "",
          "test-result-set-types",
          "TEST-RESULT-SET-TYPES",
          "test_result_set_types",
          "test%",
          "test\\_result\\_set\\_types",
          "nonexistent");

  static final List<Object> COLUMN_VARIANTS =
      Arrays.asList(
          null,
          "%",
          "",
          "varchar-column",
          "VARCHAR-COLUMN",
          "varchar_column",
          "varchar%",
          "varchar\\_column",
          "nonexistent");

  static final List<Object> FUNCTION_VARIANTS =
      Arrays.asList(
          null,
          "%",
          "",
          "area-calc",
          "AREA-CALC",
          "area_calc",
          "area%",
          "area\\_calc",
          "nonexistent");

  static final List<Object> TABLE_TYPE_VARIANTS =
      Arrays.asList(
          null,
          new String[] {"TABLE"},
          new String[] {"VIEW"},
          new String[] {"TABLE", "VIEW"},
          new String[] {},
          new String[] {"NONEXISTENT_TYPE"},
          new String[] {"table"});

  // Keys methods: exact-match only (patterns not supported)
  static final List<Object> KEYS_CATALOG_VARIANTS = Arrays.asList(null, "comparator_tests");
  static final List<Object> KEYS_SCHEMA_VARIANTS = Arrays.asList(null, "oss_jdbc_tests");

  static final List<Object> PK_TABLE_VARIANTS =
      Arrays.asList(null, "test_result_set_types", "no_constraints", "fk_child", "fk_parent");

  static final List<Object> FK_TABLE_VARIANTS =
      Arrays.asList(null, "fk_child", "no_constraints", "test_result_set_types");

  static final List<Object> PARENT_TABLE_VARIANTS = Arrays.asList(null, "fk_parent");

  // ---------------------------------------------------------------------------
  // Methods that should be skipped entirely (known to differ by design)
  // ---------------------------------------------------------------------------

  static Set<String> getSkippedMethods() {
    return Set.of("getConnection", "getDriverVersion", "getURL", "unwrap", "isWrapperFor");
  }

  // ---------------------------------------------------------------------------
  // Per-method argument combo builders
  // Methods are added incrementally as we develop each one.
  // ---------------------------------------------------------------------------

  /** getCatalogs() — 0-arg, single call. */
  static List<Object[]> getCatalogs() {
    return Collections.singletonList(new Object[0]);
  }

  // ---------------------------------------------------------------------------
  // Cartesian product helper
  // ---------------------------------------------------------------------------

  @SafeVarargs
  static List<Object[]> cartesianProduct(List<Object>... argVariants) {
    List<Object[]> result = new ArrayList<>();
    result.add(new Object[0]);
    for (List<Object> variants : argVariants) {
      List<Object[]> next = new ArrayList<>();
      for (Object[] prefix : result) {
        for (Object val : variants) {
          Object[] combo = Arrays.copyOf(prefix, prefix.length + 1);
          combo[prefix.length] = val;
          next.add(combo);
        }
      }
      result = next;
    }
    return result;
  }
}
