package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.comparator.ComparisonResult;
import com.databricks.jdbc.comparator.config.ConnectionFactory;
import com.databricks.jdbc.comparator.error.CapturedOutcome;
import com.databricks.jdbc.comparator.error.Captures;
import com.databricks.jdbc.comparator.error.ErrorDiffs;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Negative transaction cases — {@code commit()} / {@code rollback()} with autocommit on, and a
 * server-rejected statement inside a manual transaction ({@code setAutoCommit(false)} then a DDL).
 * Compares each endpoint's error behavior via {@link ErrorDiffs}.
 *
 * <p>Isolation: these cases mutate the connection's autocommit/transaction state, so the suite
 * overrides the {@link ConnectionFactory} overload and opens its OWN fresh connections per side
 * (closed in a {@code finally}), never touching the shared connections. A fresh pair per case keeps
 * state changes from leaking between cases.
 */
public class NegativeTransactionProvider implements SuiteProvider {

  // Throwaway table for the DDL-in-transaction case, in a schema known to exist so the case fails
  // (if it does) for the transaction reason, not a missing namespace. Dropped in a finally so no
  // stray state remains whether the DDL is rejected or (on a backend where DDL isn't
  // transactional) succeeds.
  private static final String TXN_TMP_TABLE = "comparator_tests.oss_jdbc_tests.neg_txn_tmp";

  @FunctionalInterface
  private interface ConnAction {
    Object run(Connection conn) throws Exception;
  }

  private static final class Case {
    final String description;
    final ConnAction action;

    Case(String description, ConnAction action) {
      this.description = description;
      this.action = action;
    }
  }

  private static final List<Case> CASES =
      Arrays.asList(
          new Case(
              "commit() with autocommit on",
              conn -> {
                conn.setAutoCommit(true);
                conn.commit();
                return "ok";
              }),
          new Case(
              "rollback() with autocommit on",
              conn -> {
                conn.setAutoCommit(true);
                conn.rollback();
                return "ok";
              }),
          new Case(
              "DDL inside a manual transaction (setAutoCommit(false))",
              conn -> {
                conn.setAutoCommit(false);
                try (Statement s = conn.createStatement()) {
                  return s.execute("CREATE TABLE " + TXN_TMP_TABLE + " (id INT)");
                }
              }));

  @Override
  public List<TestCase> getTestCases() {
    return CASES.stream()
        .map(c -> new TestCase(c.description, c.description))
        .collect(Collectors.toList());
  }

  /** Not used — this suite requires the ConnectionFactory overload below. */
  @Override
  public ComparisonResult execute(
      Connection conn1, Connection conn2, TestCase testCase, String label) throws Exception {
    throw new UnsupportedOperationException(
        "NEGATIVE_TRANSACTION requires dedicated connections; use the ConnectionFactory overload");
  }

  @Override
  public ComparisonResult execute(
      Connection conn1,
      Connection conn2,
      ConnectionFactory factory,
      TestCase testCase,
      String label)
      throws Exception {
    Case c =
        CASES.stream()
            .filter(x -> x.description.equals(testCase.getIdentifier()))
            .findFirst()
            .orElseThrow(
                () -> new IllegalArgumentException("Unknown case: " + testCase.getIdentifier()));

    Connection left = factory.openFresh("LEFT");
    Connection right = factory.openFresh("RIGHT");
    try {
      CapturedOutcome lo = Captures.capture(() -> c.action.run(left));
      CapturedOutcome ro = Captures.capture(() -> c.action.run(right));
      ComparisonResult result = new ComparisonResult(label, c.description, testCase.getArgs());
      ErrorDiffs.foldInto(result, lo, ro, "result ", "");
      return result;
    } finally {
      // Drop the throwaway table on both sides in case a backend committed the DDL, then close.
      dropTxnTmp(left);
      dropTxnTmp(right);
      closeQuietly(left);
      closeQuietly(right);
    }
  }

  private static void dropTxnTmp(Connection conn) {
    try (Statement s = conn.createStatement()) {
      s.execute("DROP TABLE IF EXISTS " + TXN_TMP_TABLE);
    } catch (Exception ignored) {
      // best-effort cleanup
    }
  }

  private static void closeQuietly(Connection conn) {
    try {
      if (conn != null) conn.close();
    } catch (Exception ignored) {
      // best-effort
    }
  }
}
