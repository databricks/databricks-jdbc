package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.comparator.ComparisonResult;
import com.databricks.jdbc.comparator.error.CapturedOutcome;
import com.databricks.jdbc.comparator.error.Captures;
import com.databricks.jdbc.comparator.error.ErrorDiffs;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Negative ResultSet-iteration cases — mis-using a ResultSet after it (or its statement) is closed,
 * and reading a column out of range. Compares how the two endpoints surface each failure via {@link
 * ErrorDiffs}.
 *
 * <p>Best-effort note: the originally-intended case — {@code next()} failing mid-iteration due to
 * CloudFetch presigned-link expiry or a chunk-download failure — cannot be provoked
 * deterministically from a client test (links are valid for the query's lifetime and expiry is
 * time/infra dependent), so it is intentionally NOT included here. The cases below are the
 * reliably-reproducible ResultSet misuse errors; CloudFetch-expiry comparison, if ever needed,
 * would require server-side fault injection out of scope for this harness.
 *
 * <p>Read-only, safe on the shared connections.
 */
public class NegativeResultSetProvider implements SuiteProvider {

  private static final String TABLE = "comparator_tests.oss_jdbc_tests.test_result_set_types";
  private static final String INLINE_QUERY = "SELECT id FROM " + TABLE + " LIMIT 1";
  // Wide + full-table scan → a CloudFetch-backed result (same pattern the positive SELECT suite
  // uses), so the misuse runs against a chunk/link-backed ResultSet rather than an inline one.
  private static final String CLOUDFETCH_QUERY = "SELECT * FROM " + TABLE + " ORDER BY id";

  private enum Kind {
    NEXT_AFTER_CLOSE,
    GET_AFTER_CLOSE,
    GET_AFTER_STMT_CLOSE,
    COLUMN_OUT_OF_RANGE
  }

  private static final class Case {
    final String description;
    final Kind kind;
    final String query;

    Case(String description, Kind kind, String query) {
      this.description = description;
      this.kind = kind;
      this.query = query;
    }
  }

  private static final List<Case> CASES =
      Arrays.asList(
          new Case("next() after the ResultSet is closed", Kind.NEXT_AFTER_CLOSE, INLINE_QUERY),
          new Case("getObject() after the ResultSet is closed", Kind.GET_AFTER_CLOSE, INLINE_QUERY),
          new Case("next() after the Statement is closed", Kind.GET_AFTER_STMT_CLOSE, INLINE_QUERY),
          new Case(
              "getObject() on an out-of-range column index",
              Kind.COLUMN_OUT_OF_RANGE,
              INLINE_QUERY),
          // CloudFetch-backed variants: identical misuse on a large (CloudFetch) result. The errors
          // are client-side, so they are expected to match inline — this exercises the CloudFetch
          // result path for parity with the positive suites.
          new Case(
              "next() after the ResultSet is closed (CloudFetch)",
              Kind.NEXT_AFTER_CLOSE,
              CLOUDFETCH_QUERY),
          new Case(
              "getObject() after the ResultSet is closed (CloudFetch)",
              Kind.GET_AFTER_CLOSE,
              CLOUDFETCH_QUERY),
          new Case(
              "next() after the Statement is closed (CloudFetch)",
              Kind.GET_AFTER_STMT_CLOSE,
              CLOUDFETCH_QUERY),
          new Case(
              "getObject() on an out-of-range column index (CloudFetch)",
              Kind.COLUMN_OUT_OF_RANGE,
              CLOUDFETCH_QUERY));

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
    CapturedOutcome left = Captures.capture(() -> runCase(conn1, c));
    CapturedOutcome right = Captures.capture(() -> runCase(conn2, c));
    ComparisonResult result = new ComparisonResult(label, c.description, testCase.getArgs());
    ErrorDiffs.foldInto(result, left, right, "result ", "");
    return result;
  }

  private Object runCase(Connection conn, Case c) throws Exception {
    switch (c.kind) {
      case NEXT_AFTER_CLOSE:
        {
          Statement s = conn.createStatement();
          ResultSet rs = s.executeQuery(c.query);
          rs.close();
          try {
            return rs.next(); // ResultSet closed
          } finally {
            s.close();
          }
        }
      case GET_AFTER_CLOSE:
        {
          Statement s = conn.createStatement();
          ResultSet rs = s.executeQuery(c.query);
          rs.next();
          rs.close();
          try {
            return rs.getObject(1); // ResultSet closed
          } finally {
            s.close();
          }
        }
      case GET_AFTER_STMT_CLOSE:
        {
          Statement s = conn.createStatement();
          ResultSet rs = s.executeQuery(c.query);
          s.close(); // closing the statement closes the ResultSet
          return rs.next();
        }
      case COLUMN_OUT_OF_RANGE:
        {
          try (Statement s = conn.createStatement();
              ResultSet rs = s.executeQuery(c.query)) {
            rs.next();
            return rs.getObject(999); // out-of-range column index
          }
        }
      default:
        throw new IllegalArgumentException("Unknown kind: " + c.kind);
    }
  }
}
