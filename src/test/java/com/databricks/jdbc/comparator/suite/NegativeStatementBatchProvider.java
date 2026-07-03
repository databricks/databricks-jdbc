package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.comparator.ComparisonResult;
import com.databricks.jdbc.comparator.error.CapturedOutcome;
import com.databricks.jdbc.comparator.error.Captures;
import com.databricks.jdbc.comparator.error.ErrorDiffs;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Negative batch cases — {@code executeBatch()} with a failing command (partial failure, full
 * failure, or a command that produces a ResultSet). Compares each endpoint's error behavior via
 * {@link ErrorDiffs}, and — when both sides throw {@link BatchUpdateException} — also compares the
 * per-element update counts, which are the JDBC-defined signal for partial batch failure.
 *
 * <p>Isolation: own namespace under {@code comparator_ddl_tests}, seeded fresh and dropped, so it
 * is safe on the shared connections.
 */
public class NegativeStatementBatchProvider implements SuiteProvider {

  private static final String CATALOG = "comparator_ddl_tests";
  private static final String SCHEMA1 = CATALOG + ".neg_batch_thrift";
  private static final String SCHEMA2 = CATALOG + ".neg_batch_sea";

  /** Adds the batch commands for one side, given that side's schema. */
  @FunctionalInterface
  private interface BatchFor {
    void add(Statement stmt, String schema) throws Exception;
  }

  private static final class Case {
    final String description;
    final BatchFor batch;

    Case(String description, BatchFor batch) {
      this.description = description;
      this.batch = batch;
    }
  }

  private static final List<Case> CASES =
      Arrays.asList(
          new Case(
              "executeBatch with one failing row (partial failure)",
              (stmt, s) -> {
                stmt.addBatch("INSERT INTO " + s + ".seed VALUES (10, 'ok')");
                stmt.addBatch("INSERT INTO " + s + ".seed VALUES ('bad', 'x')"); // type error
                stmt.addBatch("INSERT INTO " + s + ".seed VALUES (11, 'ok2')");
              }),
          new Case(
              "executeBatch with all rows failing",
              (stmt, s) -> {
                stmt.addBatch("INSERT INTO " + s + ".__no_such__ VALUES (1)");
                stmt.addBatch("INSERT INTO " + s + ".__no_such__ VALUES (2)");
              }),
          new Case(
              "executeBatch where one command returns a ResultSet",
              (stmt, s) -> {
                stmt.addBatch("INSERT INTO " + s + ".seed VALUES (20, 'ok')");
                stmt.addBatch("SELECT * FROM " + s + ".seed"); // illegal in a batch
              }));

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
    try (Statement s1 = conn1.createStatement();
        Statement s2 = conn2.createStatement()) {
      // Building the batch is bookkeeping; only executeBatch() is the captured driver call.
      c.batch.add(s1, SCHEMA1);
      c.batch.add(s2, SCHEMA2);
      CapturedOutcome left = Captures.capture(() -> s1.executeBatch());
      CapturedOutcome right = Captures.capture(() -> s2.executeBatch());

      ComparisonResult result = new ComparisonResult(label, c.description, testCase.getArgs());
      for (String d : ErrorDiffs.compare(left, right, "batch counts ")) {
        result.dataDifferences.add(d);
      }
      // When both threw BatchUpdateException, also compare the per-element update counts.
      String counts = compareUpdateCounts(left, right);
      if (counts != null) {
        result.dataDifferences.add(counts);
      }
      return result;
    } finally {
      exec(conn1, "DROP SCHEMA IF EXISTS " + SCHEMA1 + " CASCADE");
      exec(conn2, "DROP SCHEMA IF EXISTS " + SCHEMA2 + " CASCADE");
    }
  }

  /**
   * Returns a diff string if both sides threw BatchUpdateException with differing counts, else
   * null.
   */
  private static String compareUpdateCounts(CapturedOutcome left, CapturedOutcome right) {
    if (!left.threw() || !right.threw()) {
      return null;
    }
    if (left.throwable() instanceof BatchUpdateException
        && right.throwable() instanceof BatchUpdateException) {
      int[] c1 = ((BatchUpdateException) left.throwable()).getUpdateCounts();
      int[] c2 = ((BatchUpdateException) right.throwable()).getUpdateCounts();
      if (!Arrays.equals(c1, c2)) {
        return "Error batch updateCounts mismatch: "
            + Arrays.toString(c1)
            + " vs "
            + Arrays.toString(c2);
      }
    }
    return null;
  }

  private void setup(Connection conn, String schema) {
    exec(conn, "DROP SCHEMA IF EXISTS " + schema + " CASCADE");
    exec(conn, "CREATE SCHEMA " + schema);
    exec(conn, "CREATE TABLE " + schema + ".seed (id INT, name STRING)");
  }

  private void exec(Connection conn, String sql) {
    try (Statement stmt = conn.createStatement()) {
      stmt.executeUpdate(sql);
    } catch (Exception ignored) {
      // best-effort setup/teardown
    }
  }
}
