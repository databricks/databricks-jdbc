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
 * Negative DML cases — INSERT / UPDATE / DELETE that should fail (type mismatch, constraint
 * violation, missing table, wrong method, non-updatable target, overflow). Compares each endpoint's
 * error behavior via {@link ErrorDiffs}, honoring the {@code ERROR_COMPARISON_MODE} gate.
 *
 * <p>Isolation: operates in its own namespace under {@code comparator_ddl_tests}, seeded fresh per
 * run and dropped at the end, so it is safe on the shared connections.
 */
public class NegativeStatementDmlProvider implements SuiteProvider {

  private static final String CATALOG = "comparator_ddl_tests";
  private static final String SCHEMA1 = CATALOG + ".neg_dml_thrift";
  private static final String SCHEMA2 = CATALOG + ".neg_dml_sea";

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
          new Case(
              "INSERT a non-numeric string into an INT column",
              s -> "INSERT INTO " + s + ".seed (id, name) VALUES ('not_an_int', 'x')"),
          new Case(
              "INSERT violating NOT NULL on id",
              s -> "INSERT INTO " + s + ".seed (id, name) VALUES (NULL, 'x')"),
          new Case(
              "INSERT into a non-existent table",
              s -> "INSERT INTO " + s + ".__no_such__ VALUES (1)"),
          new Case(
              "UPDATE a non-existent table",
              s -> "UPDATE " + s + ".__no_such__ SET name = 'x' WHERE id = 1"),
          new Case(
              "DELETE from a non-existent table",
              s -> "DELETE FROM " + s + ".__no_such__ WHERE id = 1"),
          new Case("executeUpdate on a SELECT (wrong method)", s -> "SELECT * FROM " + s + ".seed"),
          new Case(
              "Write overflow: CAST('123456' AS DECIMAL(2,0))",
              s ->
                  "INSERT INTO "
                      + s
                      + ".seed (id, name) VALUES (CAST('123456' AS DECIMAL(2,0)), 'x')"));

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

    setup(conn1, SCHEMA1);
    setup(conn2, SCHEMA2);
    try {
      CapturedOutcome left = Captures.capture(() -> execUpdate(conn1, c.sql.sql(SCHEMA1)));
      CapturedOutcome right = Captures.capture(() -> execUpdate(conn2, c.sql.sql(SCHEMA2)));
      ComparisonResult result = new ComparisonResult(label, c.description, testCase.getArgs());
      ErrorDiffs.foldInto(result, left, right, "update count ", "");
      return result;
    } finally {
      exec(conn1, "DROP SCHEMA IF EXISTS " + SCHEMA1 + " CASCADE");
      exec(conn2, "DROP SCHEMA IF EXISTS " + SCHEMA2 + " CASCADE");
    }
  }

  private void setup(Connection conn, String schema) {
    exec(conn, "DROP SCHEMA IF EXISTS " + schema + " CASCADE");
    exec(conn, "CREATE SCHEMA " + schema);
    exec(conn, "CREATE TABLE " + schema + ".seed (id INT NOT NULL, name STRING)");
    exec(conn, "INSERT INTO " + schema + ".seed VALUES (1, 'alice')");
  }

  private int execUpdate(Connection conn, String sql) throws Exception {
    try (Statement stmt = conn.createStatement()) {
      return stmt.executeUpdate(sql);
    }
  }

  private void exec(Connection conn, String sql) {
    try (Statement stmt = conn.createStatement()) {
      stmt.executeUpdate(sql);
    } catch (Exception ignored) {
      // best-effort setup/teardown
    }
  }
}
