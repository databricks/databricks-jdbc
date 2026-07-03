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
 * Negative connection-state cases — {@code setCatalog}/{@code setSchema} to a non-existent target,
 * {@code getCatalog}/{@code getSchema} after a failed namespace switch, {@code setClientInfo} with
 * an invalid value, and {@code USE} of a non-existent catalog/schema. Compares each endpoint's
 * error behavior via {@link ErrorDiffs}.
 *
 * <p>Isolation: these cases mutate session state, so the suite overrides the {@link
 * ConnectionFactory} overload and opens its OWN fresh connections per side (closed in a {@code
 * finally}). It never touches the shared {@code conn1}/{@code conn2}, so it cannot poison other
 * suites. A fresh pair is opened per case so state changes don't leak between cases either.
 */
public class NegativeConnectionStateProvider implements SuiteProvider {

  /** An action on a single connection that may throw; returns a value on success. */
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
              "setCatalog to a non-existent catalog",
              conn -> {
                conn.setCatalog("__no_such_catalog__");
                return conn.getCatalog();
              }),
          new Case(
              "setSchema to a non-existent schema",
              conn -> {
                conn.setSchema("__no_such_schema__");
                return conn.getSchema();
              }),
          new Case(
              "setClientInfo with an invalid session conf value",
              conn -> {
                conn.setClientInfo("statement_timeout", "not_a_number");
                return "ok";
              }),
          new Case(
              "USE a non-existent catalog",
              conn -> {
                try (Statement s = conn.createStatement()) {
                  return s.execute("USE CATALOG __no_such_catalog__");
                }
              }),
          new Case(
              "USE a non-existent schema",
              conn -> {
                try (Statement s = conn.createStatement()) {
                  return s.execute("USE SCHEMA __no_such_schema__");
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
        "NEGATIVE_CONNECTION_STATE requires dedicated connections; use the ConnectionFactory "
            + "overload");
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

    // Fresh, dedicated connections per side — opening them is bookkeeping (outside capture); a
    // failure to connect propagates and fails the test loudly (harness problem, not a driver diff).
    Connection left = factory.openFresh("LEFT");
    Connection right = factory.openFresh("RIGHT");
    try {
      CapturedOutcome lo = Captures.capture(() -> c.action.run(left));
      CapturedOutcome ro = Captures.capture(() -> c.action.run(right));
      ComparisonResult result = new ComparisonResult(label, c.description, testCase.getArgs());
      ErrorDiffs.foldInto(result, lo, ro, "value ", "");
      return result;
    } finally {
      closeQuietly(left);
      closeQuietly(right);
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
