package com.databricks.jdbc.comparator.setup;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Declarative spec of all workspace objects required for comparator tests.
 *
 * <p>Read this file to see everything the comparator needs. Adding a new table or function = add
 * one entry to the appropriate map/list. The setup loops through these automatically.
 *
 * <p>See my-test-folder/workspace-setup.txt for workspace details.
 */
public class WorkspaceSpec {

  // ---------------------------------------------------------------------------
  // Catalogs
  // ---------------------------------------------------------------------------

  static final List<String> CATALOGS =
      List.of("comparator_tests", "`comparator-tests`", "comparator_ddl_tests");

  // ---------------------------------------------------------------------------
  // Schemas — all 4 catalog×schema combinations
  // ---------------------------------------------------------------------------

  static final List<String> ALL_SCHEMAS =
      List.of(
          "comparator_tests.oss_jdbc_tests",
          "comparator_tests.`oss-jdbc-tests`",
          "`comparator-tests`.oss_jdbc_tests",
          "`comparator-tests`.`oss-jdbc-tests`");

  static final String PRIMARY_SCHEMA = "comparator_tests.oss_jdbc_tests";

  // ---------------------------------------------------------------------------
  // Column definitions
  // ---------------------------------------------------------------------------

  static final String TEST_RESULT_SET_COLUMNS =
      "id INT NOT NULL, "
          + "varchar_column VARCHAR(255) NOT NULL, "
          + "`varchar-column` VARCHAR(255) NOT NULL, "
          + "boolean_column BOOLEAN, "
          + "integer_column INT, "
          + "bigint_column BIGINT, "
          + "smallint_column SMALLINT, "
          + "tinyint_column TINYINT, "
          + "float_column FLOAT, "
          + "double_column DOUBLE, "
          + "decimal_column DECIMAL(10,2), "
          + "date_column DATE, "
          + "timestamp_column TIMESTAMP, "
          + "timestamp_ntz_column TIMESTAMP_NTZ, "
          + "binary_column BINARY, "
          + "array_column ARRAY<STRING>, "
          + "map_column MAP<STRING, STRING>, "
          + "struct_column STRUCT<field1: STRING, field2: INT>, "
          + "variant_column VARIANT, "
          + "ym_interval_column INTERVAL YEAR TO MONTH, "
          + "dt_interval_column INTERVAL DAY TO SECOND, "
          + "array_of_arrays_column ARRAY<ARRAY<INT>>, "
          + "array_of_maps_column ARRAY<MAP<STRING, INT>>, "
          + "array_of_structs_column ARRAY<STRUCT<x: INT, y: INT>>, "
          + "map_of_arrays_column MAP<STRING, ARRAY<INT>>, "
          + "map_of_maps_column MAP<STRING, MAP<STRING, INT>>, "
          + "map_of_structs_column MAP<STRING, STRUCT<name: STRING, age: INT>>, "
          + "struct_with_array_column STRUCT<label: STRING, items: ARRAY<INT>>, "
          + "struct_with_map_column STRUCT<label: STRING, tags: MAP<STRING, INT>>, "
          + "struct_with_struct_column STRUCT<outer_name: STRING, inner: STRUCT<a: INT, b: INT>>, "
          + "geometry_column GEOMETRY(0), "
          + "geography_column GEOGRAPHY(4326)";

  static final String NO_CONSTRAINTS_COLUMNS = "id INT, name VARCHAR(255), value DOUBLE";
  static final String FK_PARENT_COLUMNS = "parent_id INT NOT NULL, name VARCHAR(255)";
  static final String FK_CHILD_COLUMNS = "child_id INT NOT NULL, parent_id INT, name VARCHAR(255)";

  // ---------------------------------------------------------------------------
  // Tables — created in ALL 4 schemas (name → columns + inline constraints)
  // ---------------------------------------------------------------------------

  static final Map<String, String> TABLES_ALL_SCHEMAS =
      orderedMap(
          "test_result_set_types",
          TEST_RESULT_SET_COLUMNS + ", CONSTRAINT pk_trst PRIMARY KEY (id, varchar_column)",
          "`test-result-set-types`",
          TEST_RESULT_SET_COLUMNS + ", CONSTRAINT pk_trst_h PRIMARY KEY (id, `varchar-column`)",
          "no_constraints",
          NO_CONSTRAINTS_COLUMNS,
          "`no-constraints`",
          NO_CONSTRAINTS_COLUMNS);

  // ---------------------------------------------------------------------------
  // Tables — created only in PRIMARY schema (name → columns + inline constraints)
  // FK parent tables must be created before FK child tables.
  // ---------------------------------------------------------------------------

  static final Map<String, String> TABLES_PRIMARY_ONLY =
      orderedMap(
          "fk_parent", FK_PARENT_COLUMNS + ", CONSTRAINT pk_fk_parent PRIMARY KEY (parent_id)",
          "`fk-parent`", FK_PARENT_COLUMNS + ", CONSTRAINT pk_fk_parent_h PRIMARY KEY (parent_id)",
          "fk_child",
              FK_CHILD_COLUMNS
                  + ", CONSTRAINT pk_fk_child PRIMARY KEY (child_id)"
                  + ", CONSTRAINT fk_child_parent FOREIGN KEY (parent_id) REFERENCES "
                  + PRIMARY_SCHEMA
                  + ".fk_parent(parent_id)",
          "`fk-child`",
              FK_CHILD_COLUMNS
                  + ", CONSTRAINT pk_fk_child_h PRIMARY KEY (child_id)"
                  + ", CONSTRAINT fk_child_parent_h FOREIGN KEY (parent_id) REFERENCES "
                  + PRIMARY_SCHEMA
                  + ".`fk-parent`(parent_id)");

  // ---------------------------------------------------------------------------
  // Tables — specific schema (cross-schema/cross-catalog FK)
  // FK references primary schema fk_parent.
  // ---------------------------------------------------------------------------

  static final Map<String, String> TABLES_SPECIFIC =
      orderedMap(
          "comparator_tests.`oss-jdbc-tests`.fk_child_cross_schema",
              FK_CHILD_COLUMNS
                  + ", CONSTRAINT fk_cross_schema FOREIGN KEY (parent_id) REFERENCES "
                  + PRIMARY_SCHEMA
                  + ".fk_parent(parent_id)",
          "`comparator-tests`.oss_jdbc_tests.fk_child_cross_catalog",
              FK_CHILD_COLUMNS
                  + ", CONSTRAINT fk_cross_catalog FOREIGN KEY (parent_id) REFERENCES "
                  + PRIMARY_SCHEMA
                  + ".fk_parent(parent_id)");

  // ---------------------------------------------------------------------------
  // Views — created in ALL schemas
  // ---------------------------------------------------------------------------

  static final String VIEW_TEMPLATE =
      "CREATE VIEW IF NOT EXISTS {schema}.test_view AS SELECT * FROM {schema}.test_result_set_types";

  // ---------------------------------------------------------------------------
  // Functions — all 5 created in ALL schemas
  // ---------------------------------------------------------------------------

  static final Map<String, String> FUNCTIONS =
      orderedMap(
          "area(radius DOUBLE)", "RETURNS DOUBLE RETURN radius * radius * 3.14159",
          "area_calc(radius DOUBLE)", "RETURNS DOUBLE RETURN radius * radius * 3.14159",
          "`area-calc`(radius DOUBLE)", "RETURNS DOUBLE RETURN radius * radius * 3.14159",
          "compute_volume(radius DOUBLE, height DOUBLE)",
              "RETURNS DOUBLE RETURN radius * radius * 3.14159 * height",
          "`compute-volume`(radius DOUBLE, height DOUBLE)",
              "RETURNS DOUBLE RETURN radius * radius * 3.14159 * height");

  // ---------------------------------------------------------------------------
  // Data — edge case rows
  // ---------------------------------------------------------------------------

  static final int BULK_ROW_COUNT = 150000;

  /** 7 edge case rows for test_result_set_types tables. */
  static final List<String> TEST_RESULT_SET_EDGE_ROWS =
      List.of(
          // Row 1: Normal values
          "(1, 'hello', 'hello', true, 42, 123456789012345, 100, 10, 3.14, 2.718281828, 99.99,"
              + " '2024-01-15', '2024-01-15T10:30:00', '2024-01-15T10:30:00',"
              + " X'48454C4C4F', ARRAY('a','b','c'), MAP('key1','val1','key2','val2'),"
              + " STRUCT('test', 1), PARSE_JSON('{\"name\":\"alice\",\"age\":30}'),"
              + " INTERVAL '2-6' YEAR TO MONTH, INTERVAL '3 12:30:15' DAY TO SECOND,"
              + " ARRAY(ARRAY(1,2),ARRAY(3,4)), ARRAY(MAP('a',1),MAP('b',2)),"
              + " ARRAY(STRUCT(1,2),STRUCT(3,4)),"
              + " MAP('scores',ARRAY(90,85,95)), MAP('outer',MAP('inner',42)),"
              + " MAP('user',STRUCT('alice',30)),"
              + " STRUCT('scores',ARRAY(90,85,95)), STRUCT('config',MAP('a',1,'b',2)),"
              + " STRUCT('parent',STRUCT(1,2)),"
              + " ST_GeomFromText('POINT(1.5 2.5)'), ST_GeogFromText('POINT(1.5 2.5)'))",

          // Row 2: Different normals (negatives, single-element)
          "(2, 'world', 'world', false, -1, -987654321098765, -32000, -128, 0.0, 0.0, 0.00,"
              + " '2000-01-01', '2000-01-01T00:00:00', '2000-01-01T00:00:00',"
              + " X'00', ARRAY('x'), MAP('k','v'), STRUCT('foo', 2), '\"simple string\"',"
              + " INTERVAL '0-1' YEAR TO MONTH, INTERVAL '0 01:00:00' DAY TO SECOND,"
              + " ARRAY(ARRAY(10)), ARRAY(MAP('x',10)), ARRAY(STRUCT(10,20)),"
              + " MAP('single',ARRAY(1)), MAP('single',MAP('k',1)), MAP('admin',STRUCT('bob',25)),"
              + " STRUCT('single',ARRAY(1)), STRUCT('single',MAP('k',1)), STRUCT('solo',STRUCT(10,20)),"
              + " ST_GeomFromText('LINESTRING(0 0,1 1,2 2)'), ST_GeogFromText('LINESTRING(0 0,1 1,2 2)'))",

          // Row 3: Edge max
          "(3, 'edge_max', 'edge_max', true, 2147483647, 9223372036854775807, 32767, 127,"
              + " 3.4028235E38, 1.7976931348623157E308, 99999999.99,"
              + " '9999-12-31', '9999-12-31T23:59:59', '9999-12-31T23:59:59',"
              + " X'FFFFFFFF', ARRAY('a','b','c','d','e'), MAP('a','1','b','2','c','3'),"
              + " STRUCT('max', 2147483647), CAST(99999999 AS VARIANT),"
              + " INTERVAL '999-11' YEAR TO MONTH, INTERVAL '999 23:59:59.999999' DAY TO SECOND,"
              + " ARRAY(ARRAY(2147483647),ARRAY(-2147483648)), ARRAY(MAP('max',2147483647)),"
              + " ARRAY(STRUCT(2147483647,2147483647)),"
              + " MAP('max',ARRAY(2147483647)), MAP('max',MAP('v',2147483647)),"
              + " MAP('max',STRUCT('max',2147483647)),"
              + " STRUCT('max',ARRAY(2147483647)), STRUCT('max',MAP('v',2147483647)),"
              + " STRUCT('max',STRUCT(2147483647,2147483647)),"
              + " ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))'), ST_GeogFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))'))",

          // Row 4: All NULLs (except PK)
          "(4, 'all_nulls', 'all_nulls', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,"
              + " NULL, NULL, NULL, NULL, NULL, NULL,"
              + " STRUCT(CAST(NULL AS STRING), CAST(NULL AS INT)),"
              + " NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",

          // Row 5: Edge min
          "(5, 'edge_min', 'edge_min', false, -2147483648, -9223372036854775808, -32768, -128,"
              + " -3.4028235E38, -1.7976931348623157E308, -99999999.99,"
              + " '0001-01-01', '0001-01-01T00:00:00', '0001-01-01T00:00:00',"
              + " X'01', ARRAY('z'), MAP('min','val'), STRUCT('min', -2147483648),"
              + " CAST(-99999999 AS VARIANT),"
              + " INTERVAL '-999-11' YEAR TO MONTH, INTERVAL '-999 23:59:59.999999' DAY TO SECOND,"
              + " ARRAY(ARRAY(-1)), ARRAY(MAP('min',-2147483648)), ARRAY(STRUCT(-2147483648,-2147483648)),"
              + " MAP('min',ARRAY(-2147483648)), MAP('min',MAP('v',-2147483648)),"
              + " MAP('min',STRUCT('min',-2147483648)),"
              + " STRUCT('min',ARRAY(-2147483648)), STRUCT('min',MAP('v',-2147483648)),"
              + " STRUCT('min',STRUCT(-2147483648,-2147483648)),"
              + " ST_GeomFromText('POINT(0 0)'), ST_GeogFromText('POINT(0 0)'))",

          // Row 6: Empty/zero
          "(6, '', '', true, 0, 0, 0, 0, 0.0, 0.0, 0.00,"
              + " '1970-01-01', '1970-01-01T00:00:00', '1970-01-01T00:00:00',"
              + " X'', ARRAY(), MAP(), STRUCT('', 0), CAST(NULL AS VARIANT),"
              + " INTERVAL '0-0' YEAR TO MONTH, INTERVAL '0 00:00:00' DAY TO SECOND,"
              + " ARRAY(ARRAY()), ARRAY(MAP()), ARRAY(STRUCT(0,0)),"
              + " MAP('empty',ARRAY()), MAP('empty',MAP()), MAP('empty',STRUCT('',0)),"
              + " STRUCT('',ARRAY()), STRUCT('',MAP()), STRUCT('',STRUCT(0,0)),"
              + " ST_GeomFromText('POINT EMPTY'), ST_GeogFromText('POINT EMPTY'))",

          // Row 7: Special characters
          "(7, 'special !@#$%', 'special !@#$%', true, 1, 1, 1, 1, 1.1, 1.1, 1.10,"
              + " '2025-06-15', '2025-06-15T12:00:00', '2025-06-15T12:00:00',"
              + " X'DEADBEEF', ARRAY('has space','has,comma'),"
              + " MAP('key with space','val with \"quote\"'),"
              + " STRUCT('special chars: <>', 999), PARSE_JSON('[1,2,3]'),"
              + " INTERVAL '99-11' YEAR TO MONTH, INTERVAL '365 23:59:59.999999' DAY TO SECOND,"
              + " ARRAY(ARRAY(0,0,0),ARRAY(1,1,1),ARRAY(2,2,2)),"
              + " ARRAY(MAP('k1',1,'k2',2),MAP('k3',3)),"
              + " ARRAY(STRUCT(1,1),STRUCT(2,2),STRUCT(3,3)),"
              + " MAP('a',ARRAY(1,2),'b',ARRAY(3,4,5)),"
              + " MAP('a',MAP('x',1,'y',2),'b',MAP('z',3)),"
              + " MAP('u1',STRUCT('!@#',1),'u2',STRUCT('$%^',2)),"
              + " STRUCT('special',ARRAY(1,2,3,4,5)),"
              + " STRUCT('special',MAP('!@#',1,'$%^',2)),"
              + " STRUCT('!@#',STRUCT(99,-99)),"
              + " ST_GeomFromText('MULTIPOINT((0 0),(1 1),(2 2))'), ST_GeogFromText('MULTIPOINT((0 0),(1 1),(2 2))'))");

  static final List<String> NO_CONSTRAINTS_ROWS =
      List.of(
          "(1, 'row_one', 10.5)",
          "(2, 'row_two', 20.0)",
          "(3, NULL, NULL)",
          "(NULL, 'null_id', 0.0)");

  static final List<String> FK_PARENT_ROWS =
      List.of("(1, 'Parent Alpha')", "(2, 'Parent Beta')", "(3, 'Parent Gamma')");

  static final List<String> FK_CHILD_ROWS =
      List.of(
          "(101, 1, 'Child 1A')",
          "(102, 1, 'Child 1B')",
          "(103, 2, 'Child 2A')",
          "(104, 3, 'Child 3A')");

  // ---------------------------------------------------------------------------
  // Bulk row generation SQL (for CloudFetch testing)
  // ---------------------------------------------------------------------------

  static final String BULK_INSERT_SQL =
      "INSERT INTO {table} SELECT "
          + "1000 + seq AS id, "
          + "CONCAT('bulk_row_', seq) AS varchar_column, "
          + "'bulk' AS `varchar-column`, "
          + "(seq % 2 = 0) AS boolean_column, "
          + "seq AS integer_column, "
          + "CAST(seq AS BIGINT) * 100000 AS bigint_column, "
          + "CAST(seq % 32000 AS SMALLINT) AS smallint_column, "
          + "CAST(seq % 127 AS TINYINT) AS tinyint_column, "
          + "CAST(seq * 1.23 AS FLOAT) AS float_column, "
          + "seq * 3.456789 AS double_column, "
          + "CAST(seq * 1.11 AS DECIMAL(10,2)) AS decimal_column, "
          + "DATE_ADD('2020-01-01', seq % 1000) AS date_column, "
          + "TIMESTAMPADD(SECOND, seq, TIMESTAMP '2020-01-01 00:00:00') AS timestamp_column, "
          + "TIMESTAMPADD(SECOND, seq, TIMESTAMP_NTZ '2020-01-01 00:00:00') AS timestamp_ntz_column, "
          + "CAST(CONCAT('data_', seq) AS BINARY) AS binary_column, "
          + "ARRAY(CONCAT('item_', seq), 'extra') AS array_column, "
          + "MAP(CONCAT('k_', seq), CONCAT('v_', seq)) AS map_column, "
          + "NAMED_STRUCT('field1', CONCAT('val_', seq), 'field2', seq) AS struct_column, "
          + "PARSE_JSON(CONCAT('{\"id\":', seq, ',\"name\":\"bulk\"}')) AS variant_column, "
          + "CAST(CONCAT(CAST(seq / 12 AS INT), '-', seq % 12) AS INTERVAL YEAR TO MONTH) AS ym_interval_column, "
          + "CAST(CONCAT(seq % 365, ' ', seq % 24, ':', seq % 60, ':00') AS INTERVAL DAY TO SECOND) AS dt_interval_column, "
          + "ARRAY(ARRAY(seq, seq + 1)) AS array_of_arrays_column, "
          + "ARRAY(MAP(CONCAT('k', seq), seq)) AS array_of_maps_column, "
          + "ARRAY(NAMED_STRUCT('x', seq, 'y', seq + 1)) AS array_of_structs_column, "
          + "MAP(CONCAT('k', seq), ARRAY(seq)) AS map_of_arrays_column, "
          + "MAP(CONCAT('k', seq), MAP('inner', seq)) AS map_of_maps_column, "
          + "MAP('user', NAMED_STRUCT('name', CONCAT('u_', seq), 'age', seq % 100)) AS map_of_structs_column, "
          + "NAMED_STRUCT('label', CONCAT('l_', seq), 'items', ARRAY(seq)) AS struct_with_array_column, "
          + "NAMED_STRUCT('label', CONCAT('l_', seq), 'tags', MAP('t', seq)) AS struct_with_map_column, "
          + "NAMED_STRUCT('outer_name', CONCAT('o_', seq), 'inner', NAMED_STRUCT('a', seq, 'b', seq + 1)) AS struct_with_struct_column, "
          + "ST_GeomFromText(CONCAT('POINT (', seq % 180, ' ', seq % 90, ')')) AS geometry_column, "
          + "ST_GeogFromText(CONCAT('POINT (', seq % 180, ' ', seq % 90, ')')) AS geography_column "
          + "FROM (SELECT EXPLODE(SEQUENCE(1, "
          + BULK_ROW_COUNT
          + ")) AS seq)";

  // ---------------------------------------------------------------------------
  // Helper
  // ---------------------------------------------------------------------------

  /** Creates a LinkedHashMap preserving insertion order. */
  @SuppressWarnings("unchecked")
  private static <K, V> Map<K, V> orderedMap(Object... keyValues) {
    Map<K, V> map = new LinkedHashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      map.put((K) keyValues[i], (V) keyValues[i + 1]);
    }
    return map;
  }
}
