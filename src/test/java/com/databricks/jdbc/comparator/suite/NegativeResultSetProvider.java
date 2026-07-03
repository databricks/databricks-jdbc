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

  private static final String NEXT_AFTER_CLOSE = "next() after the ResultSet is closed";
  private static final String GET_AFTER_CLOSE = "getObject() after the ResultSet is closed";
  private static final String GET_AFTER_STMT_CLOSE = "next() after the Statement is closed";
  private static final String COLUMN_OUT_OF_RANGE = "getObject() on an out-of-range column index";

  @Override
  public List<TestCase> getTestCases() {
    return Arrays.asList(
        new TestCase(NEXT_AFTER_CLOSE, NEXT_AFTER_CLOSE),
        new TestCase(GET_AFTER_CLOSE, GET_AFTER_CLOSE),
        new TestCase(GET_AFTER_STMT_CLOSE, GET_AFTER_STMT_CLOSE),
        new TestCase(COLUMN_OUT_OF_RANGE, COLUMN_OUT_OF_RANGE));
  }

  @Override
  public ComparisonResult execute(
      Connection conn1, Connection conn2, TestCase testCase, String label) throws Exception {
    String id = testCase.getIdentifier();
    CapturedOutcome left = Captures.capture(() -> runCase(conn1, id));
    CapturedOutcome right = Captures.capture(() -> runCase(conn2, id));
    ComparisonResult result = new ComparisonResult(label, id, testCase.getArgs());
    ErrorDiffs.foldInto(result, left, right, "result ", "");
    return result;
  }

  private Object runCase(Connection conn, String id) throws Exception {
    switch (id) {
      case NEXT_AFTER_CLOSE:
        {
          Statement s = conn.createStatement();
          ResultSet rs = s.executeQuery("SELECT id FROM " + TABLE + " LIMIT 1");
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
          ResultSet rs = s.executeQuery("SELECT id FROM " + TABLE + " LIMIT 1");
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
          ResultSet rs = s.executeQuery("SELECT id FROM " + TABLE + " LIMIT 1");
          s.close(); // closing the statement closes the ResultSet
          return rs.next();
        }
      case COLUMN_OUT_OF_RANGE:
        {
          try (Statement s = conn.createStatement();
              ResultSet rs = s.executeQuery("SELECT id FROM " + TABLE + " LIMIT 1")) {
            rs.next();
            return rs.getObject(999); // out-of-range column index
          }
        }
      default:
        throw new IllegalArgumentException("Unknown case: " + id);
    }
  }
}
