package com.databricks.jdbc.comparator.suite;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Test plan for DatabaseMetaData comparator tests.
 *
 * <p>{@link #buildRegistry()} is the single entry point — read it top to bottom to see every method
 * being tested and with what arguments. Variant lists define the 9 argument variant types from the
 * test plan: null, %, empty, exact (hyphen), uppercase, underscore wildcard, prefix pattern,
 * escaped underscore, nonexistent.
 *
 * <p>Workspace: comparator_tests / comparator-tests with oss_jdbc_tests / oss-jdbc-tests schemas.
 */
public class DatabaseMetaDataParams {

  // ---------------------------------------------------------------------------
  // Test plan — read top to bottom to see all methods and their arg patterns
  // ---------------------------------------------------------------------------

  /** Builds the complete method → arg combos registry for DatabaseMetaData comparison. */
  static Map<String, List<Object[]>> buildRegistry() {
    DatabaseMetaDataRegistryBuilder r =
        new DatabaseMetaDataRegistryBuilder()
            .skipMethods("getConnection", "getDriverVersion", "getURL", "unwrap", "isWrapperFor");

    // Optional: -DMETADATA_RUN_ONLY_METHODS=getCatalogs,getSchemas
    String runOnlyFilter = System.getProperty("METADATA_RUN_ONLY_METHODS");
    if (runOnlyFilter != null && !runOnlyFilter.isEmpty()) {
      r.runOnly(runOnlyFilter.split(","));
    }

    // === ResultSet methods (cartesian product of variant lists) ===

    // getCatalogs()
    r.method("getCatalogs");

    // getSchemas(String catalog, String schemaPattern) — 9×9 = 82 combos (includes 0-arg overload)
    r.method("getSchemas").allCombinationsOf(CATALOG_VARIANTS, SCHEMA_PATTERN_VARIANTS);

    // getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
    r.method("getTables")
        .allCombinationsOf(
            CATALOG_VARIANTS, SCHEMA_PATTERN_VARIANTS, TABLE_VARIANTS, TABLE_TYPE_VARIANTS);

    // getColumns(String catalog, String schemaPattern, String tableNamePattern, String
    // columnNamePattern)
    r.method("getColumns")
        .allCombinationsOf(
            CATALOG_VARIANTS, SCHEMA_PATTERN_VARIANTS, TABLE_VARIANTS, COLUMN_VARIANTS);

    // getPrimaryKeys(String catalog, String schema, String table) — exact-match only
    r.method("getPrimaryKeys")
        .allCombinationsOf(KEYS_CATALOG_VARIANTS, KEYS_SCHEMA_VARIANTS, PK_TABLE_VARIANTS);

    // getImportedKeys(String catalog, String schema, String table) — exact-match only
    r.method("getImportedKeys")
        .allCombinationsOf(KEYS_CATALOG_VARIANTS, KEYS_SCHEMA_VARIANTS, FK_TABLE_VARIANTS);

    // getExportedKeys(String catalog, String schema, String table) — exact-match only
    r.method("getExportedKeys")
        .allCombinationsOf(KEYS_CATALOG_VARIANTS, KEYS_SCHEMA_VARIANTS, PARENT_TABLE_VARIANTS);

    // getCrossReference(6 args) — 9 curated cases, patterns not supported
    r.method("getCrossReference").explicit(crossReferenceCases());

    // getFunctions(String catalog, String schemaPattern, String functionNamePattern)
    r.method("getFunctions")
        .allCombinationsOf(CATALOG_VARIANTS, SCHEMA_PATTERN_VARIANTS, FUNCTION_VARIANTS);

    // === Stub methods (single arg set each, all return empty ResultSet) ===

    // getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String
    // columnNamePattern)
    r.method("getFunctionColumns").stub("comparator_tests", "oss_jdbc_tests", "area_calc", "%");

    // getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
    r.method("getProcedures").stub("comparator_tests", "oss_jdbc_tests", "%");

    // getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String
    // columnNamePattern)
    r.method("getProcedureColumns").stub("comparator_tests", "oss_jdbc_tests", "%", "%");

    // getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
    r.method("getColumnPrivileges")
        .stub("comparator_tests", "oss_jdbc_tests", "test_result_set_types", "%");

    // getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
    r.method("getTablePrivileges").stub("comparator_tests", "oss_jdbc_tests", "%");

    // getIndexInfo(String catalog, String schema, String table, boolean unique, boolean
    // approximate)
    r.method("getIndexInfo")
        .stub("comparator_tests", "oss_jdbc_tests", "test_result_set_types", true, false);

    // getVersionColumns(String catalog, String schema, String table)
    r.method("getVersionColumns")
        .stub("comparator_tests", "oss_jdbc_tests", "test_result_set_types");

    // getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean
    // nullable)
    r.method("getBestRowIdentifier").explicit(bestRowIdentifierCases());

    // getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
    r.method("getUDTs").stub("comparator_tests", "oss_jdbc_tests", "%", null);

    // getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
    r.method("getSuperTypes").stub("comparator_tests", "oss_jdbc_tests", "%");

    // getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
    r.method("getSuperTables").stub("comparator_tests", "oss_jdbc_tests", "test_result_set_types");

    // getAttributes(String catalog, String schemaPattern, String typeNamePattern, String
    // attributeNamePattern)
    r.method("getAttributes").stub("comparator_tests", "oss_jdbc_tests", "%", "%");

    // getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String
    // columnNamePattern)
    r.method("getPseudoColumns")
        .stub("comparator_tests", "oss_jdbc_tests", "test_result_set_types", "%");

    // === Scalar methods with arguments ===

    // supportsResultSetType(int type) — 3 types
    r.method("supportsResultSetType").allCombinationsOf(RESULT_SET_TYPES);

    // supportsResultSetConcurrency(int type, int concurrency) — 3×2 = 6 combos
    r.method("supportsResultSetConcurrency").allCombinationsOf(RESULT_SET_TYPES, CONCURRENCY_TYPES);

    // supportsResultSetHoldability(int holdability) — 2 values
    r.method("supportsResultSetHoldability").allCombinationsOf(HOLDABILITY_TYPES);

    // supportsTransactionIsolationLevel(int level) — 5 levels
    r.method("supportsTransactionIsolationLevel").allCombinationsOf(TRANSACTION_ISOLATION_LEVELS);

    // Visibility/detectability methods — 3 ResultSet types each
    r.method("ownUpdatesAreVisible").allCombinationsOf(RESULT_SET_TYPES);
    r.method("ownDeletesAreVisible").allCombinationsOf(RESULT_SET_TYPES);
    r.method("ownInsertsAreVisible").allCombinationsOf(RESULT_SET_TYPES);
    r.method("othersUpdatesAreVisible").allCombinationsOf(RESULT_SET_TYPES);
    r.method("othersDeletesAreVisible").allCombinationsOf(RESULT_SET_TYPES);
    r.method("othersInsertsAreVisible").allCombinationsOf(RESULT_SET_TYPES);
    r.method("updatesAreDetected").allCombinationsOf(RESULT_SET_TYPES);
    r.method("deletesAreDetected").allCombinationsOf(RESULT_SET_TYPES);
    r.method("insertsAreDetected").allCombinationsOf(RESULT_SET_TYPES);

    // supportsConvert(int fromType, int toType) — ~900 type pairs
    r.method("supportsConvert").explicit(supportsConvertCases());

    // === Auto-discover remaining 0-arg methods (getTableTypes, getTypeInfo, ~100 scalars) ===
    r.discoverRemainingZeroArgMethods();

    return r.build();
  }

  // ---------------------------------------------------------------------------
  // Argument variant lists
  // ---------------------------------------------------------------------------

  static final List<Object> CATALOG_VARIANTS =
      Arrays.asList(
          null, // no filter
          "%", // match all
          "", // empty string
          "comparator-tests", // exact (hyphen)
          "COMPARATOR-TESTS", // uppercase
          "comparator_tests", // unescaped _ wildcard
          "compar%", // prefix pattern
          "comparator\\_tests", // escaped _ literal
          "nonexistent");

  static final List<Object> SCHEMA_PATTERN_VARIANTS =
      Arrays.asList(
          null, // no filter
          "%", // match all
          "", // empty string
          "oss-jdbc-tests", // exact (hyphen)
          "OSS-JDBC-TESTS", // uppercase
          "oss_jdbc_tests", // unescaped _ wildcard
          "oss%", // prefix pattern
          "oss\\_jdbc\\_tests", // escaped _ literal
          "nonexistent");

  static final List<Object> TABLE_VARIANTS =
      Arrays.asList(
          null, // no filter
          "%", // match all
          "", // empty string
          "test-result-set-types", // exact (hyphen)
          "TEST-RESULT-SET-TYPES", // uppercase
          "test_result_set_types", // unescaped _ wildcard
          "test%", // prefix pattern
          "test\\_result\\_set\\_types", // escaped _ literal
          "nonexistent");

  static final List<Object> COLUMN_VARIANTS =
      Arrays.asList(
          null, // no filter
          "%", // match all
          "", // empty string
          "varchar-column", // exact (hyphen)
          "VARCHAR-COLUMN", // uppercase
          "varchar_column", // unescaped _ wildcard
          "varchar%", // prefix pattern
          "varchar\\_column", // escaped _ literal
          "nonexistent");

  static final List<Object> FUNCTION_VARIANTS =
      Arrays.asList(
          null, // no filter
          "%", // match all
          "", // empty string
          "area-calc", // exact (hyphen)
          "AREA-CALC", // uppercase
          "area_calc", // unescaped _ wildcard
          "area%", // prefix pattern
          "area\\_calc", // escaped _ literal
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
  static final List<Object> KEYS_CATALOG_VARIANTS =
      Arrays.asList(
          null, "", "comparator_tests", "comparator-tests", "COMPARATOR_TESTS", "nonexistent");
  static final List<Object> KEYS_SCHEMA_VARIANTS =
      Arrays.asList(null, "", "oss_jdbc_tests", "oss-jdbc-tests", "OSS_JDBC_TESTS", "nonexistent");

  static final List<Object> PK_TABLE_VARIANTS =
      Arrays.asList(
          null,
          "",
          "test_result_set_types",
          "TEST_RESULT_SET_TYPES",
          "test-result-set-types",
          "no_constraints",
          "fk_child",
          "FK_CHILD",
          "fk-child",
          "fk_parent",
          "FK_PARENT",
          "fk-parent",
          "nonexistent");

  static final List<Object> FK_TABLE_VARIANTS =
      Arrays.asList(
          null,
          "",
          "fk_child",
          "FK_CHILD",
          "fk-child",
          "fk_child_cross_schema",
          "fk_child_cross_catalog",
          "no_constraints",
          "test_result_set_types",
          "nonexistent");

  static final List<Object> PARENT_TABLE_VARIANTS =
      Arrays.asList(null, "", "fk_parent", "FK_PARENT", "fk-parent", "nonexistent");

  // Scalar argument lists
  static final List<Object> RESULT_SET_TYPES =
      Arrays.asList(
          ResultSet.TYPE_FORWARD_ONLY,
          ResultSet.TYPE_SCROLL_INSENSITIVE,
          ResultSet.TYPE_SCROLL_SENSITIVE);

  static final List<Object> CONCURRENCY_TYPES =
      Arrays.asList(ResultSet.CONCUR_READ_ONLY, ResultSet.CONCUR_UPDATABLE);

  static final List<Object> HOLDABILITY_TYPES =
      Arrays.asList(ResultSet.HOLD_CURSORS_OVER_COMMIT, ResultSet.CLOSE_CURSORS_AT_COMMIT);

  static final List<Object> TRANSACTION_ISOLATION_LEVELS =
      Arrays.asList(
          Connection.TRANSACTION_NONE,
          Connection.TRANSACTION_READ_UNCOMMITTED,
          Connection.TRANSACTION_READ_COMMITTED,
          Connection.TRANSACTION_REPEATABLE_READ,
          Connection.TRANSACTION_SERIALIZABLE);

  // ---------------------------------------------------------------------------
  // Curated case builders (for methods that don't fit cartesian pattern)
  // ---------------------------------------------------------------------------

  /** getCrossReference — 9 curated cases. Patterns not supported, 6 args not independent. */
  private static List<Object[]> crossReferenceCases() {
    List<Object[]> cases = new ArrayList<>();
    cases.add(new Object[] {null, null, null, null, null, null});
    cases.add(new Object[] {null, null, null, "comparator_tests", "oss_jdbc_tests", "fk_child"});
    cases.add(new Object[] {"comparator_tests", "oss_jdbc_tests", "fk_parent", null, null, null});
    cases.add(
        new Object[] {
          "comparator_tests", "oss_jdbc_tests", "fk_parent",
          "comparator_tests", "oss_jdbc_tests", "fk_child"
        });
    cases.add(
        new Object[] {
          "comparator_tests", "oss_jdbc_tests", "fk_parent",
          "comparator_tests", "oss-jdbc-tests", "fk_child_cross_schema"
        });
    cases.add(
        new Object[] {
          "comparator_tests", "oss_jdbc_tests", "fk_parent",
          "comparator-tests", "oss_jdbc_tests", "fk_child_cross_catalog"
        });
    cases.add(
        new Object[] {
          "comparator_tests", "oss_jdbc_tests", "no_constraints",
          "comparator_tests", "oss_jdbc_tests", "no_constraints"
        });
    cases.add(
        new Object[] {
          "comparator_tests", "oss_jdbc_tests", "fk_child",
          "comparator_tests", "oss_jdbc_tests", "fk_parent"
        });
    cases.add(
        new Object[] {
          "comparator_tests", "oss_jdbc_tests", "nonexistent",
          "comparator_tests", "oss_jdbc_tests", "nonexistent"
        });
    return cases;
  }

  /** getBestRowIdentifier — 3 scope values. */
  private static List<Object[]> bestRowIdentifierCases() {
    List<Object[]> cases = new ArrayList<>();
    for (int scope :
        new int[] {
          DatabaseMetaData.bestRowTemporary,
          DatabaseMetaData.bestRowTransaction,
          DatabaseMetaData.bestRowSession
        }) {
      cases.add(
          new Object[] {
            "comparator_tests", "oss_jdbc_tests", "test_result_set_types", scope, true
          });
    }
    return cases;
  }

  /** supportsConvert(fromType, toType) — all SQL type pairs except ROWID, ARRAY, STRUCT, OTHER. */
  private static List<Object[]> supportsConvertCases() {
    List<Object[]> combos = new ArrayList<>();
    List<Integer> sqlTypes = getAllSqlTypes();
    for (int from : sqlTypes) {
      for (int to : sqlTypes) {
        if (from == Types.ROWID || to == Types.ROWID) continue;
        if (from == Types.ARRAY || from == Types.STRUCT || from == Types.OTHER) continue;
        if (from == Types.BOOLEAN && to == Types.BOOLEAN) continue;
        combos.add(new Object[] {from, to});
      }
    }
    return combos;
  }

  private static List<Integer> getAllSqlTypes() {
    List<Integer> types = new ArrayList<>();
    for (Field f : Types.class.getFields()) {
      if (f.getType().equals(int.class)) {
        try {
          types.add((Integer) f.get(null));
        } catch (IllegalAccessException e) {
          // skip
        }
      }
    }
    return types;
  }
}
