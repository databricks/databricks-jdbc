package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.comparator.ComparisonResult;
import com.databricks.jdbc.comparator.ResultSetComparator;
import com.databricks.jdbc.comparator.error.CapturedOutcome;
import com.databricks.jdbc.comparator.error.Captures;
import com.databricks.jdbc.comparator.error.ErrorDiffs;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Compares DML operations (INSERT, UPDATE, DELETE) between Thrift and SEA connections.
 *
 * <p>Runs as a single ordered flow: setup → INSERT → UPDATE → DELETE. Each connection operates on
 * its own namespace inside the comparator_ddl_tests catalog. Compares executeUpdate return values
 * (update counts), exception behavior, and side effects (data verification via SELECT).
 */
public class StatementDmlProvider implements SuiteProvider {

  private static final String CATALOG = "comparator_ddl_tests";

  @Override
  public List<TestCase> getTestCases() {
    return Collections.singletonList(
        new TestCase("DML_FLOW", "DML flow: INSERT → UPDATE → DELETE with data verification"));
  }

  @Override
  public ComparisonResult execute(
      Connection conn1, Connection conn2, TestCase testCase, String label) throws Exception {
    String schema1 = CATALOG + ".dml_thrift";
    String schema2 = CATALOG + ".dml_sea";
    String table1 = schema1 + ".test_dml";
    String table2 = schema2 + ".test_dml";

    List<String> differences = new ArrayList<>();

    // Clean slate
    exec(conn1, "DROP SCHEMA IF EXISTS " + schema1 + " CASCADE");
    exec(conn2, "DROP SCHEMA IF EXISTS " + schema2 + " CASCADE");

    // Setup: create schema + table with seed data
    exec(conn1, "CREATE SCHEMA " + schema1);
    exec(conn2, "CREATE SCHEMA " + schema2);
    exec(conn1, "CREATE TABLE " + table1 + " (id INT, name STRING, value DOUBLE)");
    exec(conn2, "CREATE TABLE " + table2 + " (id INT, name STRING, value DOUBLE)");
    exec(conn1, "INSERT INTO " + table1 + " VALUES (1, 'alice', 10.0), (2, 'bob', 20.0)");
    exec(conn2, "INSERT INTO " + table2 + " VALUES (1, 'alice', 10.0), (2, 'bob', 20.0)");

    // Verify seed data
    verifySideEffect(
        conn1,
        conn2,
        "SELECT count(*) FROM " + table1,
        "SELECT count(*) FROM " + table2,
        differences,
        "seed data row count");

    // 1. INSERT
    compareDml(
        conn1,
        conn2,
        "INSERT INTO " + table1 + " VALUES (3, 'charlie', 30.0)",
        "INSERT INTO " + table2 + " VALUES (3, 'charlie', 30.0)",
        differences,
        "INSERT");
    verifySideEffect(
        conn1,
        conn2,
        "SELECT * FROM " + table1 + " WHERE id = 3",
        "SELECT * FROM " + table2 + " WHERE id = 3",
        differences,
        "row exists after INSERT");

    // 2. UPDATE
    compareDml(
        conn1,
        conn2,
        "UPDATE " + table1 + " SET value = 99.9 WHERE id = 1",
        "UPDATE " + table2 + " SET value = 99.9 WHERE id = 1",
        differences,
        "UPDATE");
    verifySideEffect(
        conn1,
        conn2,
        "SELECT value FROM " + table1 + " WHERE id = 1",
        "SELECT value FROM " + table2 + " WHERE id = 1",
        differences,
        "value updated after UPDATE");

    // 3. DELETE
    compareDml(
        conn1,
        conn2,
        "DELETE FROM " + table1 + " WHERE id = 2",
        "DELETE FROM " + table2 + " WHERE id = 2",
        differences,
        "DELETE");
    verifySideEffect(
        conn1,
        conn2,
        "SELECT count(*) FROM " + table1,
        "SELECT count(*) FROM " + table2,
        differences,
        "row count after DELETE");

    // Cleanup
    exec(conn1, "DROP SCHEMA IF EXISTS " + schema1 + " CASCADE");
    exec(conn2, "DROP SCHEMA IF EXISTS " + schema2 + " CASCADE");

    ComparisonResult result = new ComparisonResult(label, "DML_FLOW", testCase.getArgs());
    result.dataDifferences = differences;
    return result;
  }

  /** Executes DML on both connections and compares update counts and errors. */
  private void compareDml(
      Connection conn1,
      Connection conn2,
      String sql1,
      String sql2,
      List<String> differences,
      String operation) {
    CapturedOutcome left = Captures.capture(() -> executeDml(conn1, sql1));
    CapturedOutcome right = Captures.capture(() -> executeDml(conn2, sql2));

    if (left.threw() || right.threw()) {
      // An error on either side. Honor the ERROR_COMPARISON_MODE gate so `off` disables deep
      // comparison here just as it does on the ResultSetComparator path (the rollout kill switch).
      for (String d : ErrorDiffs.compare(left, right, "update count ")) {
        differences.add(operation + ": " + d);
      }
    } else if (!left.value().equals(right.value())) {
      differences.add(
          operation + ": update count mismatch: " + left.value() + " vs " + right.value());
    }
  }

  private int executeDml(Connection conn, String sql) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      return stmt.executeUpdate(sql);
    }
  }

  /**
   * Verifies side effects by comparing SELECT results from both connections. Uses
   * ResultSetComparator for full cell-by-cell comparison.
   */
  private void verifySideEffect(
      Connection conn1,
      Connection conn2,
      String sql1,
      String sql2,
      List<String> differences,
      String context) {
    try (Statement s1 = conn1.createStatement();
        Statement s2 = conn2.createStatement();
        ResultSet rs1 = s1.executeQuery(sql1);
        ResultSet rs2 = s2.executeQuery(sql2)) {
      ComparisonResult verifyResult =
          ResultSetComparator.compare("verify:" + context, sql1, new Object[] {}, rs1, rs2);
      if (verifyResult.hasDifferences()) {
        differences.add(context + " verification found differences:");
        differences.addAll(verifyResult.dataDifferences);
      }
    } catch (Exception e) {
      differences.add(context + " verification failed: " + e.getMessage());
    }
  }

  private void exec(Connection conn, String sql) {
    try (Statement stmt = conn.createStatement()) {
      stmt.executeUpdate(sql);
    } catch (Exception ignored) {
    }
  }
}
