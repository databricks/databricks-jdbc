package com.databricks.jdbc.comparator.setup;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

/**
 * Workspace data setup for comparator tests.
 *
 * <p>Controlled by system property {@code -DWORKSPACE_SETUP=recreate}. When set, drops all
 * comparator catalogs (CASCADE) and recreates everything from scratch: catalogs, schemas, tables,
 * constraints, views, functions, and data.
 *
 * <p>When not set (default), skips entirely — no overhead on normal test runs.
 *
 * <p>Called from {@code JDBCDriverComparisonTest.@BeforeAll}.
 */
public class WorkspaceSetup {

  private static final String SETUP_PROPERTY = "WORKSPACE_SETUP";

  public static void run(Connection conn) throws SQLException {
    String mode = System.getProperty(SETUP_PROPERTY);
    if (mode == null || mode.isEmpty()) return;

    if ("recreate".equalsIgnoreCase(mode)) {
      recreate(conn);
    } else {
      log("Unknown mode: " + mode + ". Expected: recreate");
    }
  }

  // ---------------------------------------------------------------------------
  // Recreate — drop everything, create from scratch
  // ---------------------------------------------------------------------------

  private static void recreate(Connection conn) throws SQLException {
    log("Recreating workspace data from scratch...");

    dropCatalogs(conn);
    createCatalogs(conn);
    createSchemas(conn);
    createTables(conn);
    createViews(conn);
    createFunctions(conn);
    insertData(conn);

    log("Workspace setup complete.");
  }

  // ---------------------------------------------------------------------------
  // Layer 0: Drop
  // ---------------------------------------------------------------------------

  private static void dropCatalogs(Connection conn) throws SQLException {
    for (String catalog : WorkspaceSpec.CATALOGS) {
      tryExecute(conn, "DROP CATALOG IF EXISTS " + catalog + " CASCADE", "Dropped: " + catalog);
    }
  }

  // ---------------------------------------------------------------------------
  // Layer 1: Catalogs
  // ---------------------------------------------------------------------------

  private static void createCatalogs(Connection conn) throws SQLException {
    for (String catalog : WorkspaceSpec.CATALOGS) {
      execute(conn, "CREATE CATALOG " + catalog);
      log("Created catalog: " + catalog);
    }
  }

  // ---------------------------------------------------------------------------
  // Layer 2: Schemas
  // ---------------------------------------------------------------------------

  private static void createSchemas(Connection conn) throws SQLException {
    for (String schema : WorkspaceSpec.ALL_SCHEMAS) {
      execute(conn, "CREATE SCHEMA " + schema);
      log("Created schema: " + schema);
    }
  }

  // ---------------------------------------------------------------------------
  // Layer 3: Tables
  // ---------------------------------------------------------------------------

  private static void createTables(Connection conn) throws SQLException {
    // Tables in all schemas
    for (String schema : WorkspaceSpec.ALL_SCHEMAS) {
      for (Map.Entry<String, String> e : WorkspaceSpec.TABLES_ALL_SCHEMAS.entrySet()) {
        String fqn = schema + "." + e.getKey();
        execute(conn, "CREATE TABLE " + fqn + " (" + e.getValue() + ")");
        log("Created table: " + fqn);
      }
    }

    // Tables in primary schema only
    for (Map.Entry<String, String> e : WorkspaceSpec.TABLES_PRIMARY_ONLY.entrySet()) {
      String fqn = WorkspaceSpec.PRIMARY_SCHEMA + "." + e.getKey();
      execute(conn, "CREATE TABLE " + fqn + " (" + e.getValue() + ")");
      log("Created table: " + fqn);
    }

    // Specific tables (cross-schema/cross-catalog FK)
    for (Map.Entry<String, String> e : WorkspaceSpec.TABLES_SPECIFIC.entrySet()) {
      execute(conn, "CREATE TABLE " + e.getKey() + " (" + e.getValue() + ")");
      log("Created table: " + e.getKey());
    }
  }

  // ---------------------------------------------------------------------------
  // Layer 6: Views
  // ---------------------------------------------------------------------------

  private static void createViews(Connection conn) throws SQLException {
    for (String schema : WorkspaceSpec.ALL_SCHEMAS) {
      execute(conn, WorkspaceSpec.VIEW_TEMPLATE.replace("{schema}", schema));
      log("Created view: " + schema + ".test_view");
    }
  }

  // ---------------------------------------------------------------------------
  // Layer 7: Functions
  // ---------------------------------------------------------------------------

  private static void createFunctions(Connection conn) throws SQLException {
    for (String schema : WorkspaceSpec.ALL_SCHEMAS) {
      for (Map.Entry<String, String> e : WorkspaceSpec.FUNCTIONS.entrySet()) {
        String fqn = schema + "." + e.getKey();
        execute(conn, "CREATE FUNCTION " + fqn + " " + e.getValue());
        log("Created function: " + fqn);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Layer 8: Data
  // ---------------------------------------------------------------------------

  private static void insertData(Connection conn) throws SQLException {
    // Edge case rows for test_result_set_types in all schemas
    for (String schema : WorkspaceSpec.ALL_SCHEMAS) {
      insertRows(conn, schema + ".test_result_set_types", WorkspaceSpec.TEST_RESULT_SET_EDGE_ROWS);
      insertRows(
          conn, schema + ".`test-result-set-types`", WorkspaceSpec.TEST_RESULT_SET_EDGE_ROWS);
    }

    // no_constraints in all schemas
    for (String schema : WorkspaceSpec.ALL_SCHEMAS) {
      insertRows(conn, schema + ".no_constraints", WorkspaceSpec.NO_CONSTRAINTS_ROWS);
      insertRows(conn, schema + ".`no-constraints`", WorkspaceSpec.NO_CONSTRAINTS_ROWS);
    }

    // FK tables — primary schema only (insert parent before child)
    insertRows(conn, WorkspaceSpec.PRIMARY_SCHEMA + ".fk_parent", WorkspaceSpec.FK_PARENT_ROWS);
    insertRows(conn, WorkspaceSpec.PRIMARY_SCHEMA + ".`fk-parent`", WorkspaceSpec.FK_PARENT_ROWS);
    insertRows(conn, WorkspaceSpec.PRIMARY_SCHEMA + ".fk_child", WorkspaceSpec.FK_CHILD_ROWS);
    insertRows(conn, WorkspaceSpec.PRIMARY_SCHEMA + ".`fk-child`", WorkspaceSpec.FK_CHILD_ROWS);

    // Bulk rows — only in primary test_result_set_types
    String primaryFqn = WorkspaceSpec.PRIMARY_SCHEMA + ".test_result_set_types";
    log(
        "Inserting "
            + WorkspaceSpec.BULK_ROW_COUNT
            + " bulk rows (this may take a few minutes)...");
    execute(conn, WorkspaceSpec.BULK_INSERT_SQL.replace("{table}", primaryFqn));
    log("Bulk rows inserted.");
  }

  private static void insertRows(Connection conn, String tableFqn, List<String> rows)
      throws SQLException {
    // Use SELECT ... UNION ALL to insert all rows in one statement.
    // Avoids type inference conflicts (e.g., VARIANT column) that VALUES(...),(...) causes,
    // because each SELECT has explicit types.
    StringBuilder sql = new StringBuilder("INSERT INTO " + tableFqn + " ");
    for (int i = 0; i < rows.size(); i++) {
      if (i > 0) sql.append(" UNION ALL ");
      sql.append("SELECT ").append(rows.get(i).substring(1, rows.get(i).length() - 1)); // strip ( )
    }
    execute(conn, sql.toString());
    log("Inserted " + rows.size() + " rows into " + tableFqn);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static void execute(Connection conn, String sql) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute(sql);
    }
  }

  private static void tryExecute(Connection conn, String sql, String successMsg) {
    try {
      execute(conn, sql);
      log(successMsg);
    } catch (SQLException e) {
      log("WARNING: " + sql + " — " + e.getMessage());
    }
  }

  private static void log(String message) {
    System.out.printf("[%s] [WorkspaceSetup] %s%n", java.time.Instant.now(), message);
  }
}
