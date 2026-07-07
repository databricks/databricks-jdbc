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
  // (if it does) for the transaction reason, not a missing namespace. Each side gets its OWN table
  // name (per the DDL/DML suites' per-side isolation) so LEFT creating it doesn't make RIGHT fail
  // with "table already exists". Dropped before and after use so no stray state remains whether the
  // DDL is rejected or (on a backend where DDL isn't transactional) succeeds.
  private static final String TXN_TMP_TABLE_BASE = "comparator_tests.oss_jdbc_tests.neg_txn_tmp";

  @FunctionalInterface
  private interface ConnAction {
    Object run(Connection conn, String tmpTable) throws Exception;
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
              (conn, tmpTable) -> {
                conn.setAutoCommit(true);
                conn.commit();
                return "ok";
              }),
          new Case(
              "rollback() with autocommit on",
              (conn, tmpTable) -> {
                conn.setAutoCommit(true);
                conn.rollback();
                return "ok";
              }),
          new Case(
              "DDL inside a manual transaction (setAutoCommit(false))",
              (conn, tmpTable) -> {
                conn.setAutoCommit(false);
                try (Statement s = conn.createStatement()) {
                  return s.execute("CREATE TABLE " + tmpTable + " (id INT)");
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

    // Distinct table name per side so LEFT's CREATE doesn't collide with RIGHT's.
    String leftTable = TXN_TMP_TABLE_BASE + "_left";
    String rightTable = TXN_TMP_TABLE_BASE + "_right";

    Connection left = factory.openFresh("LEFT");
    Connection right = factory.openFresh("RIGHT");
    try {
      // Clean slate before the captured call (bookkeeping) so a leftover table from a prior crashed
      // run can't make the CREATE case fail for the wrong reason.
      dropTxnTmp(left, leftTable);
      dropTxnTmp(right, rightTable);
      CapturedOutcome lo = Captures.capture(() -> c.action.run(left, leftTable));
      CapturedOutcome ro = Captures.capture(() -> c.action.run(right, rightTable));
      ComparisonResult result = new ComparisonResult(label, c.description, testCase.getArgs());
      ErrorDiffs.foldInto(result, lo, ro, "result ", "");
      return result;
    } finally {
      // Drop each side's throwaway table in case a backend committed the DDL, then close.
      dropTxnTmp(left, leftTable);
      dropTxnTmp(right, rightTable);
      closeQuietly(left);
      closeQuietly(right);
    }
  }

  private static void dropTxnTmp(Connection conn, String tmpTable) {
    try {
      // Reset autocommit so the DROP commits even if a case left the connection mid-transaction.
      conn.setAutoCommit(true);
    } catch (Exception ignored) {
      // best-effort
    }
    try (Statement s = conn.createStatement()) {
      s.execute("DROP TABLE IF EXISTS " + tmpTable);
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
