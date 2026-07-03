package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.comparator.ComparisonResult;
import com.databricks.jdbc.comparator.config.ConnectionFactory;
import com.databricks.jdbc.comparator.error.CapturedOutcome;
import com.databricks.jdbc.comparator.error.Captures;
import com.databricks.jdbc.comparator.error.ErrorDiffs;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * Negative cancel/timeout cases — {@code cancel()} mid-query, {@code cancel()} after completion,
 * double {@code cancel()}, and {@code setQueryTimeout(1)} on a slow query. Compares how the two
 * endpoints surface each outcome via {@link ErrorDiffs}.
 *
 * <p>Isolation: cancel/timeout affect statement/connection state, so the suite opens its OWN fresh
 * connections per side (closed in a {@code finally}). The "slow query" is a bounded range
 * aggregation (~a few seconds) — no server-side sleep is available, so it is intentionally modest
 * to keep load and flakiness low; {@code setQueryTimeout(1)} and mid-flight {@code cancel()} still
 * act well within it.
 */
public class NegativeCancelTimeoutProvider implements SuiteProvider {

  // Bounded slow query: a cross-join aggregation that runs a few seconds on a small warehouse
  // without any explicit sleep. Kept modest to limit shared-warehouse load.
  private static final String SLOW_QUERY =
      "SELECT COUNT(*) FROM range(0, 100000000) a CROSS JOIN range(0, 20) b";

  private static final String CANCEL_MID = "CANCEL_MID_QUERY";
  private static final String CANCEL_AFTER = "CANCEL_AFTER_COMPLETE";
  private static final String CANCEL_TWICE = "CANCEL_ALREADY_CANCELLED";
  private static final String QUERY_TIMEOUT = "SET_QUERY_TIMEOUT_1";

  @Override
  public List<TestCase> getTestCases() {
    return Arrays.asList(
        new TestCase(CANCEL_MID, "cancel() mid slow query"),
        new TestCase(CANCEL_AFTER, "cancel() after the query completed"),
        new TestCase(CANCEL_TWICE, "cancel() on an already-cancelled statement"),
        new TestCase(QUERY_TIMEOUT, "setQueryTimeout(1) on a slow query"));
  }

  /** Runs one case's action against a single connection; returns a value or throws. */
  private Object runCase(String id, Connection conn) throws Exception {
    switch (id) {
      case CANCEL_MID:
        return cancelMid(conn);
      case CANCEL_AFTER:
        return cancelAfterComplete(conn);
      case CANCEL_TWICE:
        return cancelTwice(conn);
      case QUERY_TIMEOUT:
        return queryTimeout(conn);
      default:
        throw new IllegalArgumentException("Unknown case: " + id);
    }
  }

  /** Starts the slow query on a background thread, cancels it after a short delay, joins. */
  private Object cancelMid(Connection conn) throws Exception {
    try (Statement stmt = conn.createStatement()) {
      final Object[] box = new Object[1];
      Thread runner =
          new Thread(
              () -> {
                try {
                  stmt.executeQuery(SLOW_QUERY);
                  box[0] = "completed";
                } catch (Throwable t) {
                  box[0] = t;
                }
              });
      runner.start();
      Thread.sleep(1500); // let the query start, then cancel it mid-flight
      stmt.cancel();
      runner.join(60000);
      if (box[0] instanceof Throwable) {
        throw (Exception) box[0]; // surface the cancellation error for comparison
      }
      return box[0];
    }
  }

  /** Runs the query to completion, then cancels the (finished) statement. */
  private Object cancelAfterComplete(Connection conn) throws Exception {
    try (Statement stmt = conn.createStatement()) {
      stmt.executeQuery("SELECT 1").close();
      stmt.cancel();
      return "cancelled after complete";
    }
  }

  /** Cancels a statement twice in a row. */
  private Object cancelTwice(Connection conn) throws Exception {
    try (Statement stmt = conn.createStatement()) {
      stmt.executeQuery("SELECT 1").close();
      stmt.cancel();
      stmt.cancel();
      return "double cancel ok";
    }
  }

  /** setQueryTimeout(1) on the slow query — expect a timeout error. */
  private Object queryTimeout(Connection conn) throws Exception {
    try (Statement stmt = conn.createStatement()) {
      stmt.setQueryTimeout(1);
      stmt.executeQuery(SLOW_QUERY).close();
      return "completed within timeout";
    }
  }

  /** Not used — this suite requires the ConnectionFactory overload below. */
  @Override
  public ComparisonResult execute(
      Connection conn1, Connection conn2, TestCase testCase, String label) throws Exception {
    throw new UnsupportedOperationException(
        "NEGATIVE_CANCEL_TIMEOUT requires the ConnectionFactory overload");
  }

  @Override
  public ComparisonResult execute(
      Connection conn1,
      Connection conn2,
      ConnectionFactory factory,
      TestCase testCase,
      String label)
      throws Exception {
    String id = testCase.getIdentifier();
    Connection left = factory.openFresh("LEFT");
    Connection right = factory.openFresh("RIGHT");
    try {
      CapturedOutcome lo = Captures.capture(() -> runCase(id, left));
      CapturedOutcome ro = Captures.capture(() -> runCase(id, right));
      ComparisonResult result =
          new ComparisonResult(label, testCase.getDescription(), testCase.getArgs());
      ErrorDiffs.foldInto(result, lo, ro, "result ", "");
      return result;
    } finally {
      closeQuietly(left);
      closeQuietly(right);
    }
  }

  private static void closeQuietly(Connection conn) {
    try {
      if (conn != null) conn.close();
    } catch (SQLException ignored) {
      // best-effort
    }
  }
}
