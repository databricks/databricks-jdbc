package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.comparator.ComparisonResult;
import com.databricks.jdbc.comparator.error.CapturedOutcome;
import com.databricks.jdbc.comparator.error.Captures;
import com.databricks.jdbc.comparator.error.ErrorDiffs;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Negative DDL cases — CREATE / ALTER / DROP that should fail (missing or duplicate objects,
 * missing namespaces, malformed syntax). Compares each endpoint's error behavior (class, SQLState,
 * code, message) via {@link ErrorDiffs}, honoring the {@code ERROR_COMPARISON_MODE} gate.
 *
 * <p>Isolation: operates in its own namespace under {@code comparator_ddl_tests} (the same pattern
 * as the positive DDL/DML suites), created fresh and dropped at the end, so it does not poison
 * other suites even though it runs on the shared connections. Cases that must operate on an
 * existing object (e.g. CREATE-already-exists, ALTER-add-duplicate-column) share a small seed
 * table.
 */
public class NegativeStatementDdlProvider implements SuiteProvider {

  private static final String CATALOG = "comparator_ddl_tests";
  private static final String SCHEMA1 = CATALOG + ".neg_ddl_thrift";
  private static final String SCHEMA2 = CATALOG + ".neg_ddl_sea";

  /** Produces the SQL for one side given that side's schema. */
  @FunctionalInterface
  private interface SqlFor {
    String sql(String schema);
  }

  private static final class Case {
    final String description;
    final SqlFor sql;

    Case(String description, SqlFor sql) {
      this.description = description;
      this.sql = sql;
    }
  }

  private static final List<Case> CASES =
      Arrays.asList(
          new Case("DROP TABLE a non-existent table", s -> "DROP TABLE " + s + ".__no_such__"),
          new Case(
              "DROP TABLE IF EXISTS a non-existent table",
              s -> "DROP TABLE IF EXISTS " + s + ".__no_such__"),
          new Case("CREATE TABLE that already exists", s -> "CREATE TABLE " + s + ".seed (id INT)"),
          new Case(
              "CREATE TABLE IF NOT EXISTS that already exists",
              s -> "CREATE TABLE IF NOT EXISTS " + s + ".seed (id INT)"),
          new Case(
              "ALTER TABLE a non-existent table",
              s -> "ALTER TABLE " + s + ".__no_such__ ADD COLUMNS (x INT)"),
          new Case(
              "ALTER TABLE add a duplicate column",
              s -> "ALTER TABLE " + s + ".seed ADD COLUMNS (id INT)"),
          new Case(
              "CREATE TABLE in a non-existent schema",
              s -> "CREATE TABLE " + CATALOG + ".__no_such_schema__.t (id INT)"),
          new Case(
              "CREATE SCHEMA in a non-existent catalog",
              s -> "CREATE SCHEMA __no_such_catalog__.s"),
          new Case("Malformed DDL syntax", s -> "CREATE TABEL " + s + ".bad (id INT)"));

  @Override
  public List<TestCase> getTestCases() {
    return CASES.stream()
        .map(c -> new TestCase(c.description, c.description))
        .collect(Collectors.toList());
  }

  @Override
  public ComparisonResult execute(
      Connection conn1, Connection conn2, TestCase testCase, String label) throws Exception {
    Case c =
        CASES.stream()
            .filter(x -> x.description.equals(testCase.getIdentifier()))
            .findFirst()
            .orElseThrow(
                () -> new IllegalArgumentException("Unknown case: " + testCase.getIdentifier()));

    // Setup a fresh seed table per side (bookkeeping — outside the captured call).
    setup(conn1, SCHEMA1);
    setup(conn2, SCHEMA2);
    try {
      CapturedOutcome left = Captures.capture(() -> execUpdate(conn1, c.sql.sql(SCHEMA1)));
      CapturedOutcome right = Captures.capture(() -> execUpdate(conn2, c.sql.sql(SCHEMA2)));
      ComparisonResult result = new ComparisonResult(label, c.description, testCase.getArgs());
      for (String d : ErrorDiffs.compare(left, right, "update count ")) {
        result.dataDifferences.add(d);
      }
      return result;
    } finally {
      exec(conn1, "DROP SCHEMA IF EXISTS " + SCHEMA1 + " CASCADE");
      exec(conn2, "DROP SCHEMA IF EXISTS " + SCHEMA2 + " CASCADE");
    }
  }

  private void setup(Connection conn, String schema) {
    exec(conn, "DROP SCHEMA IF EXISTS " + schema + " CASCADE");
    exec(conn, "CREATE SCHEMA " + schema);
    exec(conn, "CREATE TABLE " + schema + ".seed (id INT)");
  }

  private int execUpdate(Connection conn, String sql) throws Exception {
    try (Statement stmt = conn.createStatement()) {
      return stmt.executeUpdate(sql);
    }
  }

  /** Best-effort setup/teardown DDL; failures are ignored (this is bookkeeping, not the test). */
  private void exec(Connection conn, String sql) {
    try (Statement stmt = conn.createStatement()) {
      stmt.executeUpdate(sql);
    } catch (Exception ignored) {
      // best-effort
    }
  }
}
