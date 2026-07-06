package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.comparator.ComparisonResult;
import com.databricks.jdbc.comparator.config.ConnectionFactory;
import com.databricks.jdbc.comparator.error.CapturedOutcome;
import com.databricks.jdbc.comparator.error.Captures;
import com.databricks.jdbc.comparator.error.ErrorDiffs;
import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Negative connection cases — opening a connection is itself the test. Each case starts from a
 * side's healthy resolved URL/token and corrupts exactly one piece (token, host, warehouse id, or a
 * connection param), then compares how the two endpoints surface the connect failure.
 *
 * <p>These do not use {@code openFresh} (that opens a HEALTHY connection); they build the broken
 * URL/token from {@link ConnectionFactory#urlFor}/{@link ConnectionFactory#token} and open via
 * {@link ConnectionFactory#open}. Any connection that does unexpectedly open is closed in a {@code
 * finally}. Nothing touches the shared connections.
 */
public class NegativeConnectionProvider implements SuiteProvider {

  /** Corrupts a (url, token) pair for one side into a deliberately-broken one. */
  @FunctionalInterface
  private interface Corruptor {
    String[] brokenUrlAndToken(String goodUrl, String goodToken);
  }

  private static final class Case {
    final String description;
    final Corruptor corruptor;

    Case(String description, Corruptor corruptor) {
      this.description = description;
      this.corruptor = corruptor;
    }
  }

  private static final List<Case> CASES =
      Arrays.asList(
          new Case(
              "Bad token (auth failure)",
              (url, token) -> new String[] {url, "dapi0000000000000000000000000000badx"}),
          new Case("Blank token (missing credential)", (url, token) -> new String[] {url, ""}),
          new Case(
              "Invalid authMech",
              (url, token) ->
                  new String[] {url.replaceFirst("authMech=[^;]*", "authMech=99"), token}),
          new Case(
              "Non-existent cluster id",
              (url, token) ->
                  new String[] {
                    url.replaceFirst(
                        "httpPath=[^;]*", "httpPath=/sql/protocolv1/o/0/0000-000000-nosuchcl"),
                    token
                  }),
          new Case(
              "Unknown host",
              (url, token) ->
                  new String[] {
                    url.replaceFirst(
                        "jdbc:databricks://[^:/;]+", "jdbc:databricks://no-such-host.invalid"),
                    token
                  }),
          new Case(
              "Malformed URL (bad httpPath)",
              (url, token) ->
                  new String[] {
                    url.replaceFirst("httpPath=[^;]*", "httpPath=/not/a/valid/path"), token
                  }),
          new Case(
              "Non-existent warehouse id",
              (url, token) ->
                  new String[] {
                    url.replaceFirst(
                        "/sql/1.0/warehouses/[^;]*", "/sql/1.0/warehouses/0000000000000000"),
                    token
                  }),
          new Case(
              "Non-existent ConnCatalog",
              (url, token) -> new String[] {url + ";ConnCatalog=__no_such_catalog__", token}),
          new Case(
              "Non-existent ConnSchema",
              (url, token) ->
                  new String[] {
                    url + ";ConnCatalog=comparator_tests;ConnSchema=__no_such_schema__", token
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
        "NEGATIVE_CONNECTION requires the ConnectionFactory overload");
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

    String[] left = c.corruptor.brokenUrlAndToken(factory.urlFor("LEFT"), factory.token());
    String[] right = c.corruptor.brokenUrlAndToken(factory.urlFor("RIGHT"), factory.token());

    // The captured driver call is the connect itself. A connection that unexpectedly opens is a
    // real (one-sided) divergence; close whatever opened afterward.
    CapturedOutcome lo = Captures.capture(() -> factory.open(left[0], left[1]));
    CapturedOutcome ro = Captures.capture(() -> factory.open(right[0], right[1]));
    try {
      ComparisonResult result = new ComparisonResult(label, c.description, testCase.getArgs());
      ErrorDiffs.foldInto(result, lo, ro, "connection ", "");
      return result;
    } finally {
      closeIfConnection(lo);
      closeIfConnection(ro);
    }
  }

  private static void closeIfConnection(CapturedOutcome outcome) {
    if (!outcome.threw() && outcome.value() instanceof Connection) {
      try {
        ((Connection) outcome.value()).close();
      } catch (Exception ignored) {
        // best-effort
      }
    }
  }
}
